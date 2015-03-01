
import os
import textwrap
import logging
import time
import traceback
import random
import atexit
try:
    import cPickle as pickle
except ImportError:
    import pickle
import pdb
from pprint import pformat

try:
    from lrucache import LRUCache
except ImportError as e:
    pass

THIS_DIR = os.path.normpath(os.path.dirname(os.path.realpath(__file__)))
LOGGER = logging.getLogger(__name__)
DEBUG = False


class CompositionError(Exception):
    pass



class DropItem(Exception):
    def __init__(self, message=None, doc=None):
        if message is None: message = 'Droping item.'
        self.message = message
        self.doc = doc
        msg = u"""
            {name}: {msg}
            doc: {doc}
        """.format(
            name=self.__class__.__name__,
            msg=self.message,
            doc=pformat(self.doc)
        )
        msg = textwrap.dedent(msg)
        self.error_output = msg

    def __repr__(self):
        return '{0}({1}{2})'.format(self.__class__.__name__, self.message, self.doc)

    def __str__(self):
        return self.error_output.encode('utf-8')

    def __unicode__(self):
        return self.error_output



class Composable(object):
    def __or__(self, other):
        if not callable(other):
            raise CompositionError(
                "{0} is not composable with {0}".format(self, other)
            )
        if self.rt == 'collection':
            if other.rt == 'collection':
                return CompositeBatchProcessor(self, other)
            else:
                return Mapper(self, other)
        else:
            return CompositeProcessor(self, other)



class Processor(Composable):
    name = None
    rt = 'atom'

    def __init__(self, *args, **kwargs):
        #   Raise errors by default.
        self.error = kwargs.pop('error', 'raise')
        self.name = self.name or self.__class__.__name__
        self.func = kwargs.pop('func', None)

    def __call__(self, *args, **kwargs):
        return self.process(*args, **kwargs)

    def __eq__(self, other):
        eq = (
            other and
            self.__class__ is other.__class__ and
            self.__dict__ == other.__dict__
        )
        return eq

    def __repr__(self):
        return "{0}()".format(self.__class__.__name__)

    def process(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def get_state(self):
        try:
            self.func.get_state()
        except AttributeError as e:
            raise NotImplementedError

    def to_df(self):
        try:
            self.func.to_df()
        except AttributeError as e:
            raise NotImplementedError

    def to_csv(self, _id=None):
        try:
            self.func.to_csv(_id=_id)
        except AttributeError as e:
            raise NotImplementedError

    def to_json(self):
        try:
            self.func.to_json()
        except AttributeError as e:
            raise NotImplementedError

    def finalize(self):
        try:
            res = self.func.finalize()
        except AttributeError as e:
            ########################
            LOGGER.error(
                """
                Processor doesn't have a 'finalize' method: {}
                """.format(
                    self.func.name if hasattr(self.func, 'name')
                    else self.func.__name__
                )
            )
            ########################
            raise NotImplementedError
        else:
            return res



def compose(items):
    composed = None
    for item in items:
        if composed is None:
            composed = item
        else:
            composed = composed | item
    return composed


class Mapper(Processor):
    rt = 'collection'
    def __init__(self, prs_collection, prs_atom, **kwargs):
        self.prs_collection = prs_collection
        self.prs_atom = prs_atom

    def __call__(self, docs, **kwargs):
        docs = self.prs_collection(docs)
        for doc in docs:
            yield prs_atom(doc)


class CompositeProcessor(Processor):
    path_pickle = os.path.realpath(os.path.join(THIS_DIR, '../log/processor'))
    rt = 'atom'

    def __init__(self, *composables, **kwargs):
        super(CompositeProcessor, self).__init__(*composables, **kwargs)
        self.path_pickle = kwargs.get('path_pickle') or self.path_pickle
        if not os.path.exists(self.path_pickle):
            os.makedirs(self.path_pickle)
        self.processors = []
        for comp in composables:
            if isinstance(comp, self.__class__):
                self.processors.extend(comp.processors)
            else:
                self.processors.append(comp)

    def __call__(self, doc, **kwargs):
        #   Run filters, etc., using their __call__ method.
        for prs in self.processors:
            try:
                doc = prs(doc, **kwargs)
                #   Stop processing on this document?
                meta = doc.setdefault('__meta__', {}) or {}
                if meta.get('skip'):
                    break
            except Exception as e:
                if prs.error == 'raise':
                    raise
                else:
                    msg = """
                    Error processing:
                        Task: {}
                        Exception: {}
                        Traceback: {}
                    """.format(prs.name, e, traceback.format_exc())
                    LOGGER.error(textwrap.dedent(msg))
                if DEBUG:
                    self.to_pickle(doc, prs.name)
        return doc

    def __eq__(self, other):
        eq = other and (self.__class__ is other.__class__) and (self.processors == other.processors)
        return eq

    def __getitem__(self, item):
        return self.processors.__getitem__(item)

    def __len__(self):
        return len(self.processors)

    def __repr__(self):
        return "{0}({1})".format(
            self.__class__.__name__,
            ", ".join(repr(prs) for prs in self.processors)
        )

    def to_pickle(self, doc, name):
        with open(os.path.join(self.path_pickle, name), 'ab') as f:
            pickle.dump(doc, f)

    def finalize(self, **kwargs):
        for prs in self.processors:
            try:
                if hasattr(prs, 'finalize'):
                    prs.finalize(**kwargs)
            except Exception as e:
                if prs.error == 'raise':   raise
                else:                       pass

    def clean(self, *args, **kwargs):
        for prs in self.processors:
            if hasattr(prs, "clean"):
                prs.clean(*args, **kwargs)

    def get_state(self, *args, **kwargs):
        for prs in self.processors:
            if hasattr(prs, "get_state"):
                prs.get_state(*args, **kwargs)

    def to_df(self, *args, **kwargs):
        for prs in self.processors:
            if hasattr(prs, "to_df"):
                prs.to_df(*args, **kwargs)

    def to_csv(self, *args, **kwargs):
        for prs in self.processors:
            if hasattr(prs, "to_csv"):
                prs.to_csv(*args, **kwargs)



class MultiProcessor(Processor):
    """
    Composes a list of processors into a single processor, and runs it on every document in a collection.
    """
    rt = 'collection'

    def __init__(self, processors):
        # for name, prs in processors:
        #     prs.name = name
        self.processor = compose(processors)

    def process(self, docs, **params):
        for idx, doc in enumerate(docs):
            if idx % 1000 == 0:
                logging.info(idx)
            doc_prs = self.processor(doc, **params)
            yield doc_prs

    def finalize(self):
        return self.processor.finalize()

    def __call__(self, docs, **params):
        docs_prs = self.process(docs, **params)
        self.finalize(**params)
        return docs_prs



class CompositeBatchProcessor(CompositeProcessor):
    rt = 'collection'

    def __init__(self, *composables, **kwargs):
        super(CompositeBatchProcessor, self).__init__(*composables, **kwargs)

    def __call__(self, docs, **kwargs):
        #   Run filters, etc., using their __call__ method.
        for prs in self.processors:
            try:
                docs = prs(docs, **kwargs)
            except Exception as e:
                if prs.error == 'raise':   raise
                else:                       pass
        return docs



class BatchProcessor(Composable):
    """
    A processor that works on an iterable collection of documents.
    """
    rt = 'collection'

    def __init__(self, processors):
        #   Raise errors by default.
        self.error = kwargs.pop('error', 'raise')
        self.name = self.name or self.__class__.__name__
        for name, prs in processors:
            prs.name = name
        self.processor = compose(processors)

    def process(self, docs, **params):
        doc_prs = self.processor(docs, **params)
        return doc_prs

    def finalize(self):
        return self.processor.finalize()

    def __call__(self, docs, **params):
        docs_prs = self.process(docs, **params)
        self.finalize(**params)
        return docs_prs

    def __eq__(self, other):
        eq = (
            other and
            self.__class__ is other.__class__ and
            self.__dict__ == other.__dict__
        )
        return eq

    def __repr__(self):
        return "{0}()".format(self.__class__.__name__)

    def get_state(self):
        raise NotImplementedError

    def to_df(self):
        raise NotImplementedError

    def to_csv(self, _id=None):
        raise NotImplementedError

    def to_json(self):
        raise NotImplementedError

    def finalize(self):
        raise NotImplementedError



def coroutine(func):
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        cr.next()
        return cr
    return start


class CoroutineProcessor(object):
    name = None
    log_rate = 1000
    log_rate_skip = 50

    def __init__(self, func, targets=None, **kwargs):
        if targets is None:
            targets = []
        elif not hasattr(targets, '__iter__'):
            targets = [targets]
        self.targets = targets
        self.func = func
        #   Raise errors by default.
        self.error = kwargs.pop('error', None)#'raise')
        self.name = (
            kwargs.get('name') or self.name or
            (
                self.func.__class__.__name__
                if hasattr(func, '__class__') else self.func.__name__
            )
        )
        # self.ids = set()
        self.ids = LRUCache(max_size=10000)
        self.idx = 0
        self.idx_skip = 0
        #   Make sure we clean up if the program exits before the finalize
        #   functions are called.
        if hasattr(self.func, 'finalize'):
            atexit.register(self.finalize)

    @coroutine
    def run(self, *args, **kwargs):
        self.targets = [ t.run(*args, **kwargs) for t in self.targets ]
        #   =============================================================
        def send(res):
            if isinstance(res, dict) or not hasattr(res, '__iter__'):
                res = [res]
            for item in res:
                ###############
                LOGGER.debug(pformat(item))
                ###############
                _id = item.get('id')
                #   ----------------------------------------
                if self.idx % self.log_rate == 0:
                    LOGGER.debug(
                        "[{0}] ({1}) {2}".format(self.idx, self.name, _id)
                    )
                #   ----------------------------------------
                self.idx += 1
                if _id:
                    exists = self.ids.get(_id)
                    # if _id in self.ids:
                    if exists:
                        LOGGER.warning(
                            "[{0}] ({1}) Document already processed!".format(
                            _id, self.name
                        ))
                    else:
                        # self.ids.add(_id)
                        self.ids[_id] = 1
                #   Stop processing this document?
                meta = item.get('__meta__', {}) or {}
                procs = set(meta.get('processors', []) or [])
                procs.add(self.name)
                meta['processors'] = list(procs)
                item['__meta__'] = meta
                if meta.get('skip'):
                    #   -----------------------------------
                    if self.idx_skip % self.log_rate_skip == 0:
                        LOGGER.info("[{0}] ({1}) Skipped ({2}). Current: {3} ".format(
                            self.idx_skip, self.name, _id, pformat(item['__meta__'])
                        ))
                    #   -----------------------------------
                    self.idx_skip += 1
                    continue
                #   Pass the item to the next stage
                #   in the pipeline.
                for target in self.targets:
                    target.send(item)
        #   =============================================================
        try:
            while True:
                doc = (yield)
                ###############
                LOGGER.debug(pformat(doc))
                ###############
                if doc is StopIteration:
                    raise StopIteration
                try:
                    res = self.func(doc, *args, **kwargs)
                except Exception as e:
                    if self.error == 'raise':
                        raise
                    else:
                        msg = """
                        Error processing document:
                            Task: {}
                            Exception: {}
                            Traceback: {}
                        """.format(self.name, e, traceback.format_exc())
                        LOGGER.error(textwrap.dedent(msg))
                    if DEBUG:
                        self.to_pickle(doc)
                else:
                    if res:
                        if res is StopIteration:
                            raise StopIteration
                        else:
                            send(res)
        except StopIteration as e:
            res = self.finalize()
            if res:
                #   Process final batch, if any.
                send(res)
            for target in self.targets:
                try:
                    target.send(StopIteration)
                except StopIteration:
                    pass
        except GeneratorExit:
            LOGGER.warning("[{0}] Quitting...".format(self.name))

    def to_pickle(self, doc):
        with open(os.path.join(self.path_pickle, self.name or 'UNK'), 'ab') as f:
            pickle.dump(doc, f)

    def get_state(self):
        try:
            self.func.get_state()
        except AttributeError as e:
            raise NotImplementedError

    def to_df(self):
        try:
            self.func.to_df()
        except AttributeError as e:
            raise NotImplementedError

    def to_csv(self, _id=None):
        try:
            self.func.to_csv(_id=_id)
        except AttributeError as e:
            raise NotImplementedError

    def to_json(self):
        try:
            self.func.to_json()
        except AttributeError as e:
            raise NotImplementedError

    def finalize(self):
        try:
            res = self.func.finalize()
        except AttributeError as e:
            ########################
            LOGGER.error(
                """
                Processor doesn't have a 'finalize' method: {}
                """.format(
                    self.func.name if hasattr(self.func, 'name')
                    else self.func.__name__
                )
            )
            ########################
            raise NotImplementedError
        except Exception as e:
            msg = """
            Error finalizing task:
                Processor: {}
                Exception: {}
                Traceback: {}
            """.format(self.name, e, traceback.format_exc())
            LOGGER.error(textwrap.dedent(msg))
        else:
            return res

    def __eq__(self, other):
        eq = (
            other and
            self.__class__ is other.__class__ and
            self.__dict__ == other.__dict__
        )
        return eq

    def __or__(self, other):
        if isinstance(other, CoroutineProcessor):
            return CoroutineProcessor(
                self.func, self.targets + [other], error=self.error
            )
        elif callable(other):
            return CoroutineProcessor(
                self.func,
                self.targets + [CoroutineProcessor(other, error=self.error)]
            )
        else:
            raise Exception(
                "{0} is not composable with {0}".format(self, other)
            )

    def __getitem__(self, item):
        return self.targets.__getitem__(item)

    def __len__(self):
        return len(self.targets)

    def __repr__(self):
        return "{0}({1})".format(
            self.__class__.__name__,
            ", ".join(repr(prs) for prs in self.targets)
        )

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)



class CoroutineSource(CoroutineProcessor):
    log_rate = 100

    def run(self, *args, **kwargs):
        try:
            self.targets = [ t.run(*args, **kwargs) for t in self.targets ]
            docs = self.func(*args, **kwargs)
            for idx, doc in enumerate(docs):
                if idx % self.log_rate == 0:
                    LOGGER.debug("\n{}\n[{}] Processing...".format("-" * 80, idx))
                if not doc:
                    time.sleep(0.1)
                    continue
                for target in self.targets:
                    target.send(doc)
            self.finalize()
            raise StopIteration
        except StopIteration:
            #   Pass along the stop iteration.
            for target in self.targets:
                try:
                    target.send(StopIteration)
                except StopIteration:
                    pass
        except GeneratorExit:
            LOGGER.warning("[{0}] Quitting...".format(self.name))

    def __or__(self, other):
        if isinstance(other, CoroutineProcessor):
            return CoroutineSource(
                self.func, self.targets + [other], error=self.error
            )
        elif callable(other):
            return CoroutineSource(
                self.func,
                self.targets + [CoroutineProcessor(other, error=self.error)]
            )
        else:
            raise Exception(
                "{0} is not composable with {0}".format(self, other)
            )

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)



def test_coroutine_processor():
    def func_read(fname):
        with open(fname) as f:
            for line in f:
                yield line.strip()

    class func_grep(object):
        def __init__(self, pattern, **params):
            self.pattern = pattern
        def __call__(self, doc, *args, **params):
            pattern = self.pattern
            if pattern in doc:
                # print "\tFound {0}!".format(pattern)
                return doc

    def func_print(doc, *args, **params):
        print doc

    reader = CoroutineSource(func_read)
    grepper = CoroutineProcessor(func_grep("python"))
    printer = CoroutineProcessor(func_print)
    procs = reader | (grepper | printer)
    try:
        procs("test-log")
    except StopIteration:
        pass

    print "\nChaining...\n"
    procs = chain([reader, grepper, printer])
    try:
        procs("test-log")
    except StopIteration:
        pass



#   `Fold-right`, preserving non-associativity.
def chain(items):
    composed = None
    items = reversed(items)
    for item in items:
        if composed is None:
            composed = item
        else:
            composed = (item | composed)
    return composed

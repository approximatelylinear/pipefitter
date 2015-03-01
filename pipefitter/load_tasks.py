
import os
import pdb
from pprint import pformat
import traceback
import logging
import textwrap
import importlib

LOGGER = logging.getLogger(__name__)


def get_task_proc(task, module_root):
    task_parts = task.split('.')
    mod = module_root
    for t in task_parts:
        mod = getattr(mod, t)
    #   Last part
    return mod


def load_task(task, module_base='.', **params):
    if hasattr(task, '__iter'):
        module_name, name = task[0].rsplit('.', 1)
    else:
        module_name, name = task.rsplit('.', 1)
    LOGGER.info("Loading: {}, {}, {}".format(task, module_base, module_name))
    module_name = '.'.join([module_base, module_name])
    module = importlib.import_module(
        module_name, package=globals()['__package__']
    )
    task_processor = getattr(module, name)
    if hasattr(task_processor, '__iter__'):
        task_func = (
            task_processor[0].__name__,
            task_processor[0](**task_processor[1])
        )
    else:
        try:
            task_func = task_processor.__name__, task_processor(**params)
        except Exception as e:
            #############################
            msg = """
            Error loading task.
                Exception: {}
                Traceback: {}
            """.format(e, traceback.format_exc())
            LOGGER.error(textwrap.dedent(msg))
            #############################
    return task_func



def load_tasks(tasks, module_base='.', **params):
    """Create task processors from a set of tasks by importing their associated
    modules.
    """
    #   ===============================
    def get_task_proc(task, module_root):
        task_parts = task.split('.')
        mod = module_root
        for t in task_parts:
            mod = getattr(mod, t)
        #   Last part
        return mod
    #   ===============================
    ############
    LOGGER.debug(pformat(tasks))
    ############
    names = [
        task[0].rsplit('.', 1)
            if hasattr(task, '__iter__') else task.rsplit('.', 1)
                for task in tasks
    ]
    module_names, task_names = zip(*names)
    module_names = [ '.'.join([module_base, m]) for m in set(module_names) ]
    ############
    LOGGER.debug(pformat(module_names))
    ############
    #   Import child modules
    for m in module_names: __import__(m, globals(), locals(),)
    #   Import parent module
    module_root = __import__(module_base, globals(), locals(),)
    ###########
    LOGGER.debug(pformat(module_root))
    # pdb.set_trace()
    ###########
    task_processors = [
        get_task_proc(t[0], module_root)
            if hasattr(t, '__iter__') else get_task_proc(t, module_root)
                for t in tasks
    ]
    task_funcs = [
        (task_p[0].__name__, task_p[0](**task_p[1]))
            if hasattr(task_p, '__iter__')
            else (task_p.__name__, task_p(**params))
                for task_p in task_processors
    ]
    return task_funcs

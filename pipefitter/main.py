
import os
import logging
import copy
import pdb
from pprint import pformat

#   3rd party
import yaml

#   Custom
import processor, load_tasks

THIS_DIR = os.path.normpath(os.path.dirname(os.path.realpath(__file__)))


LOGGER = logging.getLogger(__name__)

def load_config(name):
    with open(name, 'rbU') as f:
        return yaml.load(f)


def main(config_name):
    load_config(config_name)
    module_base = globals()['__package__']
    tasks = []
    task_source = get_config()['action']['loaddata']
    #   Load the tasks that provide the source data
    tasks_source = [
        load_task(task, module_base) for task in task_source if task
    ]
    #   Combine the tasks
    prs_source = make_processor(tasks_source, name='source')
    cr_source = processor.CoroutineSource(prs_source, name='source')
    params_action = {}
    actions = get_config()['action']['main']
    actions = concat(actions)
    tasks_action = [
        load_task(action, module_base, params_action)
            for action in actions if action
    ]
    cr_action = cr_source | make_coroutine_processor(tasks_action)
    result = cr_action()
    return result


def make_processor(tasks, name=None):
    tasks = [processor.Processor(name=task.name, func=task) for task in tasks]
    proc_task = processor.compose(tasks)
    if name:
        proc_task.name = name
    return proc_task


def make_coroutine_processor(tasks, name=None):
    tasks = [
        processor.CoroutineProcessor(task, name=task.name) for task in tasks
    ]
    proc_task = processor.chain(tasks)
    if name:
        proc_task.name = name
    return proc_task


def load_task(action, module_base, params_global=None):
    if params_global is None:
        params_global = {}
    #   ------------------------------------
    LOGGER.info("Importing {0} ...".format(action))
    #   ------------------------------------
    name, params = action.items()[0]
    params = params or {}
    if isinstance(params, list):
        #   List of dictionaries
        params_ = {}
        for d in params:
            for k, v in d.iteritems():
                params_[k] = v
        params = params_
    params.setdefault('path_base', get_config()['general']['path_data'])
    params.update(params_global)
    _, task = load_tasks.load_task(
        name, module_base=module_base, **params
    )
    task.name = name
    return task



def concat(data, lvl=0):
    if isinstance(data, dict) or not hasattr(data, '__iter__'):
        return [data]
    elif len(data) == 1:
        if hasattr(data[0], '__iter__'):
            return concat(data[0], lvl+1)
        else:
            return data
    else:
        return concat(data[0], lvl+1) + concat(data[1:], lvl+1)



if __name__ == '__main__':
    import sys
    logging.basicConfig()
    config_name = sys.argv[1]
    config_name = os.path.join(THIS_DIR, 'config', config_name)
    main(config_name)


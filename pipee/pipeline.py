import json
from collections import OrderedDict
from datetime import datetime
from typing import Tuple, Dict, List

from pydantic import BaseModel
from sqlalchemy.ext.declarative import declarative_base

from pipee.common import StatusCode
from pipee.dao import PipelineDAO
from pipee.models import PipelineEntry as Ppl
from pipee.utils import is_kwargs, stringify

import traceback

Base = declarative_base()


class ContextRegistry(BaseModel):
    """
    {
        "task_name": (args, kwargs)
    }
    All the params in args and kwargs must be either 
    json serializable built-in types or instances of 
    pydantic BaseModel 

    """
    ctx_registry: Dict[str, Tuple[List, Dict]] = dict()

    def register_context(self, task_name, *args, **kwargs):
        """
        
        """
        self.ctx_registry[task_name] = (args, kwargs)

    def get_context(self, task_name, as_dict=False):
        if not as_dict:
            return self.ctx_registry.get(task_name)
        else:
            return self.dict().get("ctx_registry").get(task_name)

    def deregister_context(self, task_name):
        self.ctx_registry.pop(task_name)

    def dump(self):
        """
        Dump the context as json string
        This is for persistence
        """
        return json.dumps(self.dict())


class ContextProcessor:

    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    def process_context(self, args, kwargs):
        raise NotImplementedError


class DefaultContextProcessor(ContextProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process_context(self, args, kwargs):
        return args, kwargs


class Task:
    """
    Abstract task instances that are chained into a pipeline
    The load() and proceed() methods of a task should be carefully designed 
    such that one task's load() can always process the return value
    of previous task.

    We do not suggest using load() with **kwargs (if you don't know about *args and
     **kwargs, just don't use them), because it makes program unpredictable and 
     unmaintainable. If you are using **kwargs anyway, just use kw_retval() for keyword 
     part of return value and add @enable_kwargs to the load method of successor task
    """

    def __init__(self, *args, **kwargs):
        pass

    def load(self, *args, **kwargs):
        """
        load task from existing context
        """
        raise NotImplementedError

    def proceed(self):
        raise NotImplementedError

    def rollback(self):
        raise NotImplementedError


class TaskRegistry:

    def __init__(self, ):
        self.task_registry = OrderedDict()
        pass

    def register_task(self, task_name, task_func: Task):
        """
        Register a task with task_name
        """
        self.task_registry[task_name] = task_func

    def deregister_task(self, task_name):
        """
        Remove a task from registry, which is deprecated
        """
        self.task_registry.pop(task_name)

    def get_task(self, task_name):
        return self.task_registry.get(task_name)

    def get_next(self, task_name):
        """
        Get next stage
        :param task_name:
        :return stage_name, task_instance 
        """
        index = list(self.task_registry.keys()).index(task_name)
        if index + 1 < self.__len__():
            return self.__getitem__(index + 1)
        return None

    def __getitem__(self, i):
        return list(self.task_registry.items())[i]

    def __iter__(self):
        for k, v in self.task_registry.items():
            yield k, v

    def __len__(self):
        return len(self.task_registry.items())


class Pipeline:
    """
    Logical pipeline unit. It's made up of a chain of tasks and 
    corresponding contexts
    """

    def __init__(self, key, modifier, rollback_for=Exception, stringifier=stringify, result_parser=lambda x: x):
        """
        :param key: Globally unique key of pipeline. It's suggested that key is bound to 
            existing resources for further query
        :param modifier: Who does the modifier belong to
        :param rollback_for: rollback for certain type of Exception
        """
        self.context: ContextRegistry = ContextRegistry()
        self.tasks: TaskRegistry = TaskRegistry()
        self.message = ''
        self.key = key
        self.stage = StatusCode.INIT
        self.status = ''
        self.modifier = modifier
        self.initialized = False
        self.result = None
        self.stringifier = stringifier
        self.result_parser=result_parser
        self.rollback_for = rollback_for

    def init(self):
        """
        A pipee is initialized after all tasks
        are registered
        """
        raise NotImplementedError

    def update(self):
        """
        Flush current pipeline state to database
        """
        
        PipelineDAO.update_pipeline(self.key, {
           "status": self.status,
           "message": str(self.message),
           "stage": self.stage,
           "context": self.context.dump(),
           "result": self.stringifier(self.result),
           "modified_time": datetime.now()
        })

    def load(self):
        """
        Load a pipeline from database
        """
        pipeline_entry = PipelineDAO.get_pipeline_by_key(self.key)
        self.status = pipeline_entry.status
        self.stage = pipeline_entry.stage
        self.context = ContextRegistry(**json.loads(pipeline_entry.context))

    

    def register_context(self, task_name, *args, **kwargs):
        """
        Register a task to the pipee
        :param task_name: name of the stage
        """
        self.context.register_context(task_name, *args ** kwargs)

    def register_task(self, task_name, task: Task):
        self.tasks.register_task(task_name, task)

    def start(self, *args, **kwargs):
        """
        Start the pipee from the beginning, all existing contexts
        are dropped
        """
        stage_name, _ = self.tasks[0]
        res = None

        self.stage = stage_name
        self.context.register_context(stage_name, *args, **kwargs)
        print("Inserting")
        PipelineDAO.insert_pipeline(Ppl(
            id=None, key=self.key, stage=self.stage,
            status=self.status,
            message=self.message,
            context=self.context.dump(),
            created_time=datetime.now(),
            modified_time=datetime.now(),
            modifier=self.modifier
        ))
        self.proceed()

    def proceed(self):
        """
        execute current task and move to next stage
        """
        self.status = StatusCode.IN_PROGRESS
        if self.stage is None:
            return

        task = self.tasks.get_task(self.stage)
        args, kwargs = self.context.get_context(self.stage)
        try:
            task.load(*args, **kwargs)
            res = task.proceed()
            if self.tasks.get_next(self.stage) is not None:
                next_stage, _ = self.tasks.get_next(self.stage)
                self.stage = next_stage
                if isinstance(res, tuple):
                    if is_kwargs(res[-1]):
                        print(res)
                        self.context.register_context(next_stage, *res[:-1], **res[-1])
                    self.context.register_context(next_stage, *res)
                    
                else:
                    self.context.register_context(next_stage, res)
                
            else:
                # Pipeline finished
                self.stage = None
                self.status = StatusCode.FINISHED
                self.result = res
            
            # TODO update pipeline in database
            self.update()
            
            return res
        except Exception as e:
            # TODO implement rollback_for
            
            if isinstance(e, self.rollback_for):
                task.rollback()
            else:
                self.message = str(traceback.format_exception(type(e), e, e.__traceback__))
                self.status = StatusCode.ERROR
            self.update()

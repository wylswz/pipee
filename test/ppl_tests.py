
from pipee.pipeline import Task, Pipeline
from pipee.utils import kw_retval, enable_kwargs
from pydantic import BaseModel
from typing import List, Tuple
import unittest
import json


class ParamA(BaseModel):
    var_a1: int
    var_a2: str = None
    var_a3: List[int] = None


class ParamB(BaseModel):
    var_b1: ParamA


class ParamC(BaseModel):
    var_c1: List[ParamB]

class MyTaskA(Task):
    def load(self, a: ParamA):
        self.a = a
    
    def proceed(self):
        print("proceeding A")
        return ParamB(**{"var_b1": self.a})

    def rollback(self):
        print("rolling back A")


class MyTaskB(Task):
    def load(self, b: ParamB):
        self.b = b

    def proceed(self):
        print("proceeding B")
        print(kw_retval(g='g', h='h'))
        return 'c', 'd', 'e', 'f',  kw_retval(g='g', h='h')
    
    def rollback(self):
        print("rolling back B")

class MyTaskC(Task):

    @enable_kwargs
    def load(self, c, d, e, *args, **kwargs):
        print(args, kwargs)
        print(c,d,e,args,kwargs)
        self.c = c
        self.d = d
        self.e = e
        self.f = args[0]
        self.g = kwargs.get('g')

    def proceed(self):
        print("proceeding C")
        print(self.g)
        return self.g, {"carry":1}
    
    def rollback(self):
        print("rolling back C")

class MyTaskD(Task):

    def load(self,*args, **kwargs):
        self.carry = args[1].get("carry")

    def proceed(self):
        return self.carry

class PipelineTestCase(unittest.TestCase):

    def setUp(self):

        self.param_a = ParamA(**{"var_a1":3})
        self.param_b = ParamB(**{"var_b1": self.param_a})
        self.param_c = ParamC(**{"var_c1": [self.param_b]})

class TestPipelineExecution(PipelineTestCase):

    def test_task_chaining(self):
        pipeline = Pipeline("test_ppl", "test_user")
        pipeline.register_task('task_a', MyTaskA())
        pipeline.register_task('task_b', MyTaskB())
        pipeline.register_task('task_c', MyTaskC())
        pipeline.register_task('task_d', MyTaskD())

        pipeline.start(self.param_a)
        pipeline.proceed()
        pipeline.proceed()
        pipeline.proceed()
        self.assertEqual(1,pipeline.result)

        


if __name__ == '__main__':
    unittest.main()
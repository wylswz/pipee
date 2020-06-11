from dotenv import load_dotenv
load_dotenv("demo/demo.env")

from pydantic import BaseModel
from pipee.pipeline import Pipeline, Task
from pipee.utils import enable_kwargs, kw_retval
from pipee.common import StatusCode
from pipee.dao import PipelineDAO
from typing import List, Dict



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
        print()
        input("proceeding A")
        return ParamB(**{"var_b1": self.a})

    def rollback(self):
        print("rolling back A")


class MyTaskB(Task):
    def load(self, b: ParamB):
        self.b = b

    def proceed(self):
        input("proceeding B")
        print(kw_retval(g='g', h='h'))
        return 'c', 'd', 'e', 'f',  kw_retval(g='g', h='h')
    
    def rollback(self):
        print("rolling back B")

class MyTaskC(Task):

    @enable_kwargs
    def load(self, c, d, e, *args, **kwargs):
        self.c = c
        self.d = d
        self.e = e
        self.f = args[0]
        self.g = kwargs.get('g')

    def proceed(self):
        input("proceeding C")
        return self.g, {"carry":0}
    
    def rollback(self):
        print("rolling back C")

class MyTaskD(Task):

    def __init__(self):
        self.num = 0


    def load(self,*args, **kwargs):
        self.carry = int(args[1].get("carry"))

    def proceed(self):
        input("proceeding D")
        1/self.num
        return self.num

    def rollback(self):
        input("rollingback D")
        print(self.num)
        self.num = 1



PipelineDAO.delete_pipeline_by_key("demo:key:1")
ppl = Pipeline(key="demo:key:1", modifier="demo", rollback_for=ZeroDivisionError)
ppl.register_task("A", MyTaskA())
ppl.register_task("B", MyTaskB())
ppl.register_task("C", MyTaskC())
ppl.register_task("D", MyTaskD())
ppl.start(ParamA(**{"var_a1":3}))
while ppl.status != StatusCode.FINISHED:
    print(ppl)
    
    ppl.proceed()



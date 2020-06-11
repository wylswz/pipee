from pipee.pipeline import Task, Pipeline
from pydantic import BaseModel
from typing import List, Tuple

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
        print(self.a)
        return ParamB(**{"var_b1": self.a})

    def rollback(self):
        print("rolling back A")
        print(self.a)


class MyTaskB(Task):
    def load(self, b: ParamB):
        self.b = b

    def proceed(self):
        print("proceeding B")
        print(self.b)
        return 'c', 'd', 'e', 'f',  {'g': ParamC(**{"var_c1":[self.b]})}
    
    def rollback(self):
        print("rolling back B")
        print(self.b)

class MyTaskC(Task):
    def load(self, c, d, e, *args, **kwargs):
        self.c = c
        self.d = d
        self.e = e
        self.f = args[0]
        self.g = kwargs.get('g')

    def proceed(self):
        print("proceeding C")
    
    def rollback(self):
        print("rolling back C")


param_a = ParamA(**{"var_a1":3})
param_b = ParamB(**{"var_b1": param_a})
param_c = ParamC(**{"var_c1": [param_b]})


# pipee = Pipeline("test_pipeline", "yunluw")
# pipee.register_task("taskA", MyTaskA())
# pipee.register_task("taskB", MyTaskB())
# pipee.register_task("taskC", MyTaskC())

# pipee.start(param_a)
# pipee.proceed()
# pipee.proceed()
# pipee.proceed()

# print(pipee.context.get_context('taskB'))

from pipee.system.connections import Connection
engine = Connection.get_engine()
Connection.init_db()
print(Connection._get_address())

# class TypeCast(BaseModel):
#     t: Tuple[int, str]

# print(TypeCast(**{"t":[1,"asd"]}))


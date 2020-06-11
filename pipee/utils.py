from pydantic import BaseModel
import json


_rnd_token = '836e22ae-abaf-11ea-90bd-106530269587'


def kw_retval(**kwargs):
    """
    Indicated that the return value should be processed 
    by next task as **kwargs in load() method 
    """
    res = kwargs
    res['_ret_val_type' + _rnd_token] = 'kwargs'
    return res


def is_kwargs(kwargs):
    """
    Check whether a dictionary is supposed to be unpacked, or
    it's just a normal dict
    """
    if not isinstance(kwargs, dict):
        return False

    return kwargs.get('_ret_val_type' + _rnd_token) is not None


def clean_args(kwargs):
    """
    Remove extra fields in kwargs
    """
    if kwargs.get('_ret_val_type' + _rnd_token) is not None:
        kwargs.pop('_ret_val_type' + _rnd_token)


def enable_kwargs(func):
    """
    Applied to load() method to indicate that **kwargs should be processed 
    explicitly. Otherwise will be a normal dict in *args
    """

    def wrapper(*args, **kwargs):

        if len(args) >= 1:

            if is_kwargs(args[-1]):
                clean_args(args[-1])
                for k, v in args[-1].items():
                    kwargs[k] = v
                args = args[:-1]
        print(args, kwargs)
        return func(*args, **kwargs)

    return wrapper

def stringify(result):
    """
    Convert a result to string
    Built-in implementation only support BaseModel. 
    Other types are converted using str() directly
    """
    if isinstance(result, BaseModel):
        return json.dumps(result.dict())
    else:
        return str(result)

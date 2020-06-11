# Pipeline

## Overview
Aimed to design a pipelined task execution pattern with following features

- Support chained task execution
- Persisted task context
- Fine grained retry/rollback
- Load from context and continue execution
- Log status/operators/messages/operations
- Decoupled from any existing project
- Easy to config and integrate


still under development


## Configuration
See configuration/ folder for details

## Pylint 
pylint <module_or_package> -r n --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" > report.txt

## Test

Create a virtual env `venv`

```bash
$ python3 -m virtualenv venv
$ source venv/bin/activate
```

Run tests

```bash
$ python3 -m unittest discover -s test -p "*tests.py"
```


## Run test with code coverage

This requires `coverage` library
```bash
$ pip3 install coverage
```

```bash
$ coverage run --source=pypee -m unittest discover -s test -p "*tests.py"
```

View coverages

```bash
$ coverage report
```

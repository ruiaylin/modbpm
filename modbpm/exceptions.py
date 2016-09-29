# -*- coding: utf-8 -*-
"""
modbpm.exceptions
=================

Built-in exception of modbpm.
"""


class TaskTerminate(Exception):

    def __init__(self, data, ex_data, status_code):
        self.data = data
        self.ex_data = ex_data
        self.status_code = status_code
        super(TaskTerminate, self).__init__()


class Finished(TaskTerminate):
    status_code = 0

    def __init__(self, status_code=0, *args, **kwargs):
        assert status_code == 0, "failed status_code must be 0"
        kwargs["status_code"] = status_code
        super(Finished, self).__init__(*args, **kwargs)


class Failed(TaskTerminate):
    status_code = 1

    def __init__(self, status_code=1, *args, **kwargs):
        assert status_code > 0, "failed status_code must gt 0"
        kwargs["status_code"] = status_code
        super(Failed, self).__init__(*args, **kwargs)


class Revoked(Exception):
    status_code = 2


class Timeout(Exception):
    status_code = 11


class ImportException(Exception):
    status_code = 1


class InstantiactionException(Exception):
    status_code = 2


class RuntimeException(Exception):
    status_code = 3

# -*- coding: utf-8 -*-
"""
modbpm.exceptions
=================

Built-in exception of modbpm.
"""


class Finished(Exception):
    status_code = 0


class Failed(Exception):
    status_code = 1


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

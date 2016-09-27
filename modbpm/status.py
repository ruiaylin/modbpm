"""
modbpm.status
=============

Task status codes.
"""

from modbpm import exceptions
from modbpm.utils.collections import ConstantDict

SUCCESS = 0
FAILURE = exceptions.ImportException.status_code
EXCEPTION = 255
TIMEOUT = 100

STATUS_MAP = ConstantDict({
    SUCCESS: 'SUCCESS',
})

"""
modbpm.utils.unique
===================
"""

from __future__ import absolute_import

import uuid


def uniqid():
    return uuid.uuid3(
        uuid.uuid1(),
        uuid.uuid4().hex
    ).hex

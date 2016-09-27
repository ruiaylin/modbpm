"""
modbpm.utils.random
===================

An extension of built-in random module.
"""
from __future__ import absolute_import

from random import *


def randstr(length=6):
    """
    >>> randstr() == randstr()
    False

    >>> len(randstr(8))
    8
    """
    ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return ''.join([choice(ALPHABET) for _ in range(length)])

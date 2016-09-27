# -*- coding: utf-8 -*-
"""
modbpm.states
=============

Activity states of modbpm.
"""
from modbpm.utils.collections import ConstantDict

CREATED = 'CREATED'
READY = 'READY'
RUNNING = 'RUNNING'
BLOCKED = 'BLOCKED'
SUSPENDED = 'SUSPENDED'
FINISHED = 'FINISHED'
FAILED = 'FAILED'
REVOKED = 'REVOKED'

ARCHIVED_STATES = frozenset([FINISHED, FAILED, REVOKED])

APPOINTABLE_STATES = frozenset([SUSPENDED, REVOKED])
TRANSITABLE_STATES = frozenset([READY, RUNNING, BLOCKED, FINISHED, FAILED])

SUSPENDABLE_STATES = frozenset([BLOCKED])
REVOCABLE_STATES = frozenset([BLOCKED])
RESUMABLE_STATES = frozenset([BLOCKED, SUSPENDED])

ALL_STATES = frozenset([CREATED, READY, RUNNING, BLOCKED,
                        SUSPENDED, FINISHED, FAILED, REVOKED])

_PRIORITY = ConstantDict({
    CREATED: 0,
    READY: 1,
    RUNNING: 1,
    BLOCKED: 1,
    SUSPENDED: 7,
    FINISHED: 9,
    FAILED: 9,
    REVOKED: 8,
})


def priority(state):
    """Get the priority for state::

        >>> priority(CREATED)
        0

        >>> priority('UNKNOWN')
        -1
    """
    if state in _PRIORITY:
        return _PRIORITY[state]
    else:
        return -1


class State(str):
    """State is a subclass of :class:`str`, implementing comparison
    methods adhering to state priority rules::

        >>> State(CREATED) < State(FINISHED)
        True

        >>> State(FAILED) < State(REVOKED)
        False

        >>> State(FINISHED) <= State(FAILED)
        True
    """

    def __gt__(self, other):
        return priority(self) > priority(other)

    def __ge__(self, other):
        return priority(self) >= priority(other)

    def __lt__(self, other):
        return priority(self) < priority(other)

    def __le__(self, other):
        return priority(self) <= priority(other)


_TRANSITION = ConstantDict({
    CREATED: frozenset([READY, FAILED, REVOKED]),
    READY: frozenset([RUNNING, REVOKED, SUSPENDED]),
    RUNNING: frozenset([BLOCKED, FINISHED, FAILED]),
    BLOCKED: frozenset([READY, REVOKED, FAILED]),
    SUSPENDED: frozenset([READY, REVOKED]),
    FINISHED: frozenset([]),
    FAILED: frozenset([]),
    REVOKED: frozenset([]),
})


def can_transit(from_state, to_state):
    """Test if :param:`from_state` can transit to :param:`to_state`::

        >>> can_transit(CREATED, READY)
        True

        >>> can_transit(CREATED, RUNNING)
        False
    """
    if from_state in _TRANSITION:
        if to_state in _TRANSITION[from_state]:
            return True
    return False

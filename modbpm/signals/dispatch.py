"""
modbpm.signals.dispatch
=======================
"""

from __future__ import absolute_import

from modbpm import signals, tasks
from modbpm.models import ActivityModel
from modbpm.signals import handlers

DISPATCH_UID = __name__.replace('.', '_')


def dispatch_activity_lazy_transit():
    signals.lazy_transit.connect(
        handlers.activity_lazy_transit_handler,
        sender=ActivityModel,
        dispatch_uid=DISPATCH_UID
    )


def dispatch_activity_created():
    signals.activity_created.connect(
        handlers.activity_created_handler,
        sender=ActivityModel,
        dispatch_uid=DISPATCH_UID
    )


def dispatch_activity_ready():
    signals.activity_ready.connect(
        handlers.activity_ready_handler,
        sender=ActivityModel,
        dispatch_uid=DISPATCH_UID
    )


def dispatch_activity_running():
    signals.activity_running.connect(
        handlers.activity_running_handler,
        sender=ActivityModel,
        dispatch_uid=DISPATCH_UID
    )


def dispatch_activity_blocked():
    signals.activity_blocked.connect(
        handlers.activity_blocked_handler,
        sender=ActivityModel,
        dispatch_uid=DISPATCH_UID
    )


def dispatch_activity_suspended():
    signals.activity_suspended.connect(
        handlers.activity_suspended_handler,
        sender=ActivityModel,
        dispatch_uid=DISPATCH_UID
    )


def dispatch_activity_finished():
    signals.activity_finished.connect(
        handlers.activity_finished_handler,
        sender=ActivityModel,
        dispatch_uid=DISPATCH_UID
    )
    signals.activity_finished.connect(
        handlers.activity_finished_handler,
        sender=tasks.acknowledge,
        dispatch_uid=DISPATCH_UID
    )


def dispatch_activity_failed():
    signals.activity_failed.connect(
        handlers.activity_failed_handler,
        sender=ActivityModel,
        dispatch_uid=DISPATCH_UID
    )


def dispatch_activity_revoked():
    signals.activity_revoked.connect(
        handlers.activity_revoked_handler,
        sender=ActivityModel,
        dispatch_uid=DISPATCH_UID
    )


def dispatch_all():
    dispatch_activity_lazy_transit()
    dispatch_activity_created()
    dispatch_activity_ready()
    dispatch_activity_running()
    dispatch_activity_blocked()
    dispatch_activity_suspended()
    dispatch_activity_finished()
    dispatch_activity_failed()
    dispatch_activity_revoked()


def dispatch():
    dispatch_activity_lazy_transit()
    dispatch_activity_created()
    dispatch_activity_ready()
    dispatch_activity_finished()


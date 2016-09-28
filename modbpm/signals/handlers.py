# -*- coding: utf-8 -*-
"""
modbpm.signals.handlers
=======================

Built-in signal handlers.
"""
from __future__ import absolute_import

import logging

from modbpm import states, tasks
from modbpm.conf import settings
from modbpm.models import ActivityModel

logger = logging.getLogger(__name__)


def activity_lazy_transit_handler(sender, activity_id, to_state, countdown,
                                  **kwargs):
    logger.info("activity_lazy_transit_handler #%s" % activity_id)
    tasks.transit.apply_async(args=(activity_id, to_state),
                              countdown=countdown)


def activity_created_handler(sender, instance, **kwargs):
    """
    activity created handler.
    """
    r = tasks.initiate.apply_async(args=(instance.pk,))
    logger.info("activity_created_handler #%s %r" % (instance.pk, r))


def activity_ready_handler(sender, instance, **kwargs):
    """
    activity ready handler.
    """
    logger.info("activity_ready_handler #%s" % instance.pk)
    tasks.schedule.apply_async(args=(instance.pk,))


def activity_running_handler(sender, instance, **kwargs):
    """
    activity running handler.
    """
    pass


def activity_blocked_handler(sender, instance, **kwargs):
    """
    activity blocked handler.
    """
    pass


def activity_suspended_handler(sender, instance, **kwargs):
    """
    activity suspended handler.
    """
    pass


def activity_finished_handler(sender, instance, **kwargs):
    """
    activity finished handler.
    """
    logger.info("activity_finished_handler #%s" % instance.pk)
    wake_up_parent_activity(instance)


def activity_failed_handler(sender, instance, **kwargs):
    """
    activity failed handler.
    """
    pass


def wake_up_parent_activity(instance):
    parent = instance.parent
    if isinstance(parent, ActivityModel)\
            and parent.state not in states.ARCHIVED_STATES \
            and not parent.state == states.SUSPENDED:
        if parent._transit(states.READY):
            instance._ack()
            logger.info("activity #%s waked up by #%s"
                        % (parent.pk, instance.pk))
        else:
            countdown = settings.MODBPM_ACKNOWLEDGE_COUNTDOWN
            tasks.acknowledge.apply_async(args=(instance.id,),
                                          countdown=countdown)

# -*- coding: utf-8 -*-
"""
modbpm.signals
==============

Built-in activity signals.
"""
from __future__ import absolute_import

from django.dispatch import Signal

lazy_transit = Signal(providing_args=['activity_id', 'to_state', 'countdown'])

activity_created = Signal(providing_args=['instance'])
activity_ready = Signal(providing_args=['instance'])
activity_running = Signal(providing_args=['instance'])
activity_blocked = Signal(providing_args=['instance'])
activity_suspended = Signal(providing_args=['instance'])
activity_finished = Signal(providing_args=['instance'])
activity_failed = Signal(providing_args=['instance'])
activity_revoked = Signal(providing_args=['instance'])

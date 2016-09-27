# -*- coding: utf-8 -*-
"""
modbpm.core.activity
====================

A basic implementation of activity in BPMN 2.0 patterns.
"""
from __future__ import absolute_import

import stackless

from abc import ABCMeta, abstractmethod

from django.db import transaction

from modbpm import status, exceptions, messages
from modbpm.models import ActivityModel


class AbstractActivity(object):

    __metaclass__ = ABCMeta

    def __init__(self, act_id, act_name):
        self._act_id = act_id
        self._act_name = act_name

        self._registry = {}

    def _destroy(self):
        """
        Destroy activity.
        """
        map(lambda tasklet: tasklet.kill(), self._registry)

    def _initiate(self, *args, **kwargs):
        """
        Initiate activity.
        """
        obj = self._get_model()
        self._register(stackless.tasklet(self.on_start)(*args, **kwargs),
                       obj.name)

    @transaction.atomic  # important!
    def _get_model(self):
        """
        Get model object of this activity.
        """
        query_kwargs = {
            'pk': self._act_id,
        }
        try:
            return ActivityModel.objects.get(**query_kwargs)
        except ActivityModel.DoesNotExist:
            raise RuntimeError(
                messages.build_message(
                    messages.ACT_MODEL_NOT_EXIST,
                    query_kwargs
                )
            )

    def _register(self, tasklet, act_name):
        """
        Register tasklets of this activity.
        """
        self._registry[tasklet] = act_name

    def _resume(self):
        """
        Resume tasklets of this activity.
        """
        for tasklet in self._registry:
            if tasklet.alive:
                tasklet.insert()

    @abstractmethod
    def _schedule(self):
        raise NotImplementedError

    @abstractmethod
    def on_start(self):
        raise NotImplementedError

    def finish(self, data=None, ex_data=None, status_code=status.SUCCESS):
        """
        Finish this activity.
        """
        if status_code:
            raise exceptions.Failed(data, ex_data, status_code)
        else:
            raise exceptions.Finished(data, ex_data)

# -*- coding: utf-8 -*-
"""
modbpm.activity.task
====================

Implementation of task of BPMN activity.
"""
import logging
import stackless
import types

from abc import ABCMeta

from modbpm import states, status
from modbpm.conf import settings
from modbpm.core.activity import AbstractActivity

logger = logging.getLogger(__name__)


class AbstractTask(AbstractActivity):
    """
    组件类需要继承此类，并实现start方法。

    * 如果是短任务：
        在start方法内调用finish结束任务
    * 如果是长任务，而且是否结束是基于轮询问的：
        在start方法内调用set_scheduler设置轮询的间隔。实现on_schedule方法，实现轮询的逻辑并在结束的时候调用finish结束轮询。
    * 如果是长任务，而且结束与否是基于外部回调的：
        在start方法内调用set_scheduler，设置轮询间隔为-1。如果外部回调会提供status_code等返回值，则无需实现on_schedule方法。
        如果外部回调无法提供status_code等返回值，则需要实现on_schedule方法，去访问外部资源获取status_code，然后调用finish结束。
    """

    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        super(AbstractTask, self).__init__(*args, **kwargs)

        self.schedule_count = 1

    def set_default_scheduler(self, on_schedule):
        self.set_scheduler(on_schedule, DefaultIntervalGenerator())

    def set_static_scheduler(self, on_schedule, interval):
        self.set_scheduler(on_schedule, StaticIntervalGenerator(interval))

    def set_null_scheduler(self, on_schedule):
        self.set_scheduler(on_schedule, NullIntervalGenerator())

    def set_scheduler(self, on_schedule, interval):
        assert isinstance(on_schedule, types.MethodType)
        assert self is getattr(on_schedule, 'im_self')

        self._interval = interval
        self.on_schedule = on_schedule

    def _schedule(self):
        logger.info("_schedule task #%s" % self._act_id)
        model = self._get_model()
        if model.state in states.ARCHIVED_STATES:
            return False

        self._register(stackless.tasklet(self.on_schedule)(),
                       model.name)

        if hasattr(self, '_interval'):
            countdown = self._interval.next()
            if countdown is not None:
                MIN_INTERVAL = settings.MODBPM_MIN_SCHEDULE_INTERVAL
                MAX_INTERVAL = settings.MODBPM_MAX_SCHEDULE_INTERVAL

                if countdown < MIN_INTERVAL:
                    countdown = MIN_INTERVAL
                elif countdown > MAX_INTERVAL:
                    countdown = MAX_INTERVAL

                model._lazy_transit(states.READY, countdown=countdown)
                self.schedule_count += 1

    def finish(self, data=None, ex_data=None, status_code=status.SUCCESS):
        """
        把组件的状态置为已完成，并设置返回值。虽然本调用后面的语句仍然会被执行，但是不推荐这么做。
        调用的时机可以是 :py:meth:start 也可以是 :py:meth:on_schedule 。
        组件必须显式地调用finish，与过程不同

        :param data: 组件的返回值。调用者如果用read()方法等待，则会获得该值
        :param ex_data: 组件的其他返回。用于调试
        :param status_code: 0表示正常，非0表示异常。异常情况由引擎接管，决定是否调用后面的任务
        :type status_code: int
        :return: 无返回值
        """
        return super(AbstractTask, self).finish(data, ex_data, status_code)


class DefaultIntervalGenerator(object):

    def __init__(self):
        self.count = 0

    def next(self):
        self.count += 1
        return self.count ** 2


class NullIntervalGenerator(object):

    def __init__(self):
        self.count = 0

    def next(self):
        self.count += 1


class StaticIntervalGenerator(object):

    def __init__(self, interval):
        self.count = 0
        self.interval = interval

    def next(self):
        self.count += 1
        return self.interval

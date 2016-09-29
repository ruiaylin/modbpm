# -*- coding: utf-8 -*-
"""
modbpm.activity.process
=======================

Implementation of process of BPMN activity.
"""
import contextlib
import logging
import stackless

from abc import ABCMeta

from django.db import transaction

from modbpm import states, messages
from modbpm.models import ActivityModel
from modbpm.core.activity import AbstractActivity

logger = logging.getLogger(__name__)


class ActivityHandler(object):

    def __init__(self, process, name, predecessors=None):
        self.process = process
        self.name = name
        self.predecessors = predecessors

        self.process._register(self, self.name, obj_type='handler')

    def __call__(self, *args, **kwargs):
        self.process._register(stackless.tasklet(self._start)(*args, **kwargs),
                               self.name)

        if not getattr(self.process, '_is_parallel', False):
            self.join()

        return self

    def _start(self, *args, **kwargs):
        cleaned_args, cleaned_kwargs = clean(*args, **kwargs)

        if isinstance(self.predecessors, (list, tuple)):
            join(*self.predecessors)

        query_kwargs = {
            'pk': self.process._act_id,
        }
        try:
            parent = ActivityModel.objects.get(**query_kwargs)
        except ActivityModel.DoesNotExist:
            raise RuntimeError(messages.build_message(
                messages.ACT_MODEL_NOT_EXIST,
                query_kwargs
            ))
        else:
            act = ActivityModel.objects.create_model(
                self.name,
                parent,
                *cleaned_args,
                **cleaned_kwargs
            )

            self.identifier_code = act.identifier_code
            self.token_code = act.token_code

    @transaction.atomic  # prevent phantom reads
    def _get_model(self):
        identifier_code = getattr(self, 'identifier_code', None)

        if identifier_code:
            query_kwargs = {
                'identifier_code': identifier_code,
                'token_code__isnull': False,
            }
            try:
                return ActivityModel.objects.get(**query_kwargs)
            except ActivityModel.DoesNotExist:
                logger.info(
                    messages.build_message(
                        messages.ACT_MODEL_NOT_EXIST,
                        query_kwargs
                    )
                )

    def join(self):
        while True:
            model = self._get_model()
            if isinstance(model, ActivityModel) \
                    and model.state == states.FINISHED:
                return model
            else:
                stackless.schedule()

    def read(self):
        model = self.join()
        return model.data


class DefaultScheduleMixin(object):

    def _schedule(self):
        model = self._get_model()
        if model.state in states.ARCHIVED_STATES:
            return False

        finished_handler_num = 0
        archived_handler_num = 0
        blocked_handler_num = 0
        for handler, name in self._handler_registry.iteritems():
            sub_act_model = handler._get_model()
            if sub_act_model is not None:
                if sub_act_model.state in states.ARCHIVED_STATES:
                    archived_handler_num += 1
                if sub_act_model.state == states.FINISHED:
                    finished_handler_num += 1
            else:
                blocked_handler_num += 1

        logger.info('activity #%d finished: %d, archived: %d blocked: %d',
                    self._act_id,
                    finished_handler_num,
                    archived_handler_num,
                    blocked_handler_num)

        if hasattr(self, 'archived_handler_num'):
            if archived_handler_num != self.archived_handler_num:
                self.archived_handler_num = archived_handler_num
                return True
        else:
            setattr(self, 'archived_handler_num', 0)
            return True

        # test if this activity could be finished implicitly
        #   1) all of the registered handlers are finished
        #   2) non of the registered handlers are blocked
        if finished_handler_num == len(self._handler_registry) \
                and not blocked_handler_num:
            # count the amount of alive tasklets
            alive_tasklet_num = 0
            for tasklet, name in self._registry.iteritems():
                if tasklet.alive:
                    alive_tasklet_num += 1

            # finish this activity implicitly only if
            # non of the registered tasklets are alive
            if not alive_tasklet_num:
                self.finish()


class StrictScheduleMixin(object):

    def _schedule(self):
        # TODO: to be implemented
        pass


class LooseScheduleMixin(object):

    def _schedule(self):
        # TODO: to be implemented
        pass


class AbstractProcess(AbstractActivity):

    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        super(AbstractProcess, self).__init__(*args, **kwargs)

        self._handler_registry = {}

    def _register(self, obj, name, obj_type=None):
        if obj_type:
            getattr(self, '_%s_registry' % obj_type)[obj] = name
        else:
            super(AbstractProcess, self)._register(obj, name)

    def is_parallel(self):
        return getattr(self, '_parallel', False)

    def start(self, activity, predecessors=None):
        assert issubclass(activity, AbstractActivity)
        if predecessors is None:
            predecessors = []
        return ActivityHandler(
            process=self,
            name='%s.%s' % (activity.__module__, activity.__name__),
            predecessors=predecessors,
        )

    def finish(self, data=None, ex_data=None, return_code=0):
        """
        把过程的状态设置为已结束，并提供返回值。虽然本调用后面的语句仍然会被执行，但是不推荐这么做。
        如果不显式调用finish，引擎会在所有子任务执行完成后自动调用finish，返回值为空。

        :param data: 过程的返回值。调用者如果用read()方法等待，则会获得该值
        :type data: str
        :param ex_data: 过程的其他返回。用于调试
        :type ex_data: str
        :param return_code: 0表示正常，非0表示异常。异常情况由引擎接管，决定是否调用后面的任务
        :type return_code: int
        :return: 无返回值
        """
        (data, ex_data, return_code), _ = clean(data, ex_data, return_code)
        return super(AbstractProcess, self).finish(data, ex_data, return_code)

    def set_serial(self):
        self._is_parallel = False

    def set_parallel(self):
        self._is_parallel = True

    @contextlib.contextmanager
    def run_in_serial(self):
        original_value = getattr(self, '_is_parallel', False)

        try:
            self._is_parallel = False
            yield
        finally:
            self._is_parallel = original_value

    @contextlib.contextmanager
    def run_in_parallel(self):
        original_value = getattr(self, '_is_parallel', False)

        try:
            self._is_parallel = True
            yield
        finally:
            self._is_parallel = original_value


class AbstractParallelProcess(AbstractProcess):

    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        super(AbstractParallelProcess, self).__init__(*args, **kwargs)
        self._is_parallel = True


class AbstractBaseProcess(DefaultScheduleMixin, AbstractProcess):

    __metaclass__ = ABCMeta


def clean(*args, **kwargs):
    cleaned_args = []
    for arg in args:
        if isinstance(arg, ActivityHandler):
            arg = arg.read()
        cleaned_args.append(arg)

    cleaned_kwargs = {}
    for k, v in kwargs.iteritems():
        if isinstance(v, ActivityHandler):
            v = v.read()
        cleaned_kwargs[k] = v

    return cleaned_args, cleaned_kwargs


def join(*handlers):
    for handler in handlers:
        handler.join()

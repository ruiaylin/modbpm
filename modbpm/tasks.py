# -*- coding: utf-8 -*-

try:
    import cPickle as pickle
except ImportError:
    import pickle

import contextlib
import logging
import stackless
import traceback

from celery import task
from celery.exceptions import SoftTimeLimitExceeded

from modbpm import signals, states, exceptions, messages
from modbpm.models import ActivityModel


logger = logging.getLogger(__name__)


@contextlib.contextmanager
def global_exception_handler(act):

    try:
        yield
    except SoftTimeLimitExceeded, e:
        # TODO: to be implemented
        pass
    except exceptions.Finished, e:
        act.finish(*e.args)
    except exceptions.ImportException, e:
        act.finish(ex_data=e.message, status_code=1)
    except exceptions.InstantiactionException, e:
        act.finish(ex_data=e.message, status_code=2)
    except exceptions.RuntimeException, e:
        act.finish(ex_data=e.message, status_code=3)
    except:
        act.finish(ex_data=traceback.format_exc(), status_code=1)


@contextlib.contextmanager
def import_exception_handler():
    try:
        yield
    except:
        raise exceptions.ImportException(traceback.format_exc())


@contextlib.contextmanager
def instantiation_exception_handler():
    try:
        yield
    except:
        raise exceptions.InstantiactionException(traceback.format_exc())


@contextlib.contextmanager
def runtime_exception_handler(backend):
    try:
        yield
    except (exceptions.Finished, exceptions.Failed), e:
        raise e
    except:
        raise exceptions.RuntimeException(traceback.format_exc())


@task(ignore_result=True)
def initiate(act_id):
    query_kwargs = {
        'pk': act_id,
        'state': states.CREATED,
    }
    try:
        act = ActivityModel.objects.get(**query_kwargs)
    except ActivityModel.DoesNotExist:
        logger.info(
            messages.build_message(
                messages.ACT_MODEL_NOT_EXIST,
                query_kwargs
            )
        )
    else:
        logger.info("initiate activity #%s" % act.pk)

        with global_exception_handler(act):
            module_name, _, cls_name = act.name.rpartition('.')

            with import_exception_handler():
                module = __import__(module_name, globals(), locals(), ['*'])
                cls = getattr(module, cls_name)

            with instantiation_exception_handler():
                backend = cls(act.pk, act.name)

            with runtime_exception_handler(backend):
                backend._initiate(*act.args, **act.kwargs)

            # if parent activity has an appointment state, inherit it.
            if isinstance(act.parent, ActivityModel) \
                    and act.parent.appointment in states.APPOINTABLE_STATES:
                act._appoint(act.parent.appointment)

            act._transit(states.READY, snapshot=pickle.dumps(backend))

            with runtime_exception_handler(backend):
                backend._destroy()


@task(ignore_result=True)
def schedule(act_id):
    query_kwargs = {
        'pk': act_id,
        'state': states.READY,
    }
    try:
        act = ActivityModel.objects.get(**query_kwargs)
    except ActivityModel.DoesNotExist:
        logger.info(
            messages.build_message(
                messages.ACT_MODEL_NOT_EXIST,
                query_kwargs
            )
        )
    else:
        logger.info("schedule activity #%s" % act_id)

        with global_exception_handler(act):
            if act._transit(states.RUNNING):
                backend = pickle.loads(act.snapshot.data)

                with runtime_exception_handler(backend):
                    backend._resume()

                    stackless.schedule()
                    while backend._schedule():
                        stackless.schedule()

                act._transit(states.BLOCKED, snapshot=pickle.dumps(backend))

                with runtime_exception_handler(backend):
                    backend._destroy()


@task(ignore_result=True)
def transit(act_id, to_state):
    query_kwargs = {
        'pk': act_id,
    }
    try:
        act = ActivityModel.objects.get(**query_kwargs)
    except ActivityModel.DoesNotExist:
        logger.info(
            messages.build_message(
                messages.ACT_MODEL_NOT_EXIST,
                query_kwargs
            )
        )
    else:
        logger.info("transit activity #%s" % act_id)

        with global_exception_handler(act):
            act._transit(to_state)


@task(ignore_result=True)
def acknowledge(act_id):
    query_kwargs = {
        'pk': act_id,
        'acknowledgment': 0,
    }
    try:
        act = ActivityModel.objects.get(**query_kwargs)
    except ActivityModel.DoesNotExist:
        logger.info(
            messages.build_message(
                messages.ACT_MODEL_NOT_EXIST,
                query_kwargs
            )
        )
    else:
        logger.info("acknowledge activity #%s" % act_id)

        signals.activity_finished.send(sender=acknowledge,
                                       instance=act)

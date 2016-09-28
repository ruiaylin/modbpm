# -*- coding: utf-8 -*-
"""
modbpm.models
=============
"""

try:
    import cPickle as pickle
except ImportError:
    import pickle

import logging
import zlib

from django.db import models, transaction
from django.db.models import F
from django.utils.timezone import now

from modbpm import signals, states, status
from modbpm.utils import random, unique

logger = logging.getLogger(__name__)


class CompressedIOField(models.BinaryField):

    def __init__(self, compress_level=6, *args, **kwargs):
        super(CompressedIOField, self).__init__(*args, **kwargs)
        self.compress_level = compress_level

    def get_prep_value(self, value):
        value = super(CompressedIOField, self).get_prep_value(value)
        return zlib.compress(pickle.dumps(value), self.compress_level)

    def to_python(self, value):
        value = super(CompressedIOField, self).to_python(value)
        return pickle.loads(zlib.decompress(value))

    def from_db_value(self, value, expression, connection, context):
        return self.to_python(value)


class CompressedBinaryField(models.BinaryField):

    def __init__(self, compress_level=6, *args, **kwargs):
        super(CompressedBinaryField, self).__init__(*args, **kwargs)
        self.compress_level = compress_level

    def get_prep_value(self, value):
        value = super(CompressedBinaryField, self).get_prep_value(value)
        return zlib.compress(value, self.compress_level)

    def to_python(self, value):
        value = super(CompressedBinaryField, self).to_python(value)
        return zlib.decompress(value)

    def from_db_value(self, value, expression, connection, context):
        return self.to_python(value)


class ActivityInputs(models.Model):

    args = CompressedIOField(
        blank=True,
    )
    kwargs = CompressedIOField(
        blank=True,
    )

    checksum = models.CharField(
        max_length=64,
        unique=True,
        null=True,
        blank=True,
    )

    def __unicode__(self):
        return unicode(u"#%s" % self.pk)


class ActivityOutputs(models.Model):

    data = CompressedIOField(
        blank=True,
    )
    ex_data = CompressedIOField(
        blank=True,
    )

    checksum = models.CharField(
        max_length=64,
        unique=True,
        null=True,
        blank=True,
    )

    def __unicode__(self):
        return unicode(u"#%s" % self.pk)


class ActivitySnapshot(models.Model):

    data = CompressedBinaryField(
        blank=True,
    )

    def __unicode__(self):
        return unicode(u"#%s" % self.pk)


class ActivityModelManager(models.Manager):

    @transaction.atomic
    def _supersede(self, instance, *args, **kwargs):
        assert isinstance(instance, self.model)

        if instance.state == states.FAILED:
            identifier_code = instance.identifier_code
            token_code = instance.token_code

            args = args if args else instance.args
            kwargs = kwargs if kwargs else instance.kwargs

            self.model.objects.filter(pk=instance.pk) \
                              .update(token_code=None)
            activity = self.model(name=instance.name,
                                  parent=instance.parent,
                                  args=args,
                                  kwargs=kwargs,
                                  identifier_code=identifier_code,
                                  token_code=token_code)
            activity.save()
            return activity
        return False

    def create_model(self, _name, _parent, *args, **kwargs):
        params = {
            'name': _name,
        }

        # create inputs object if necessary.
        if args or kwargs:
            params['inputs'] = ActivityInputs.objects.create(
                args=args,
                kwargs=kwargs,
            )

        # create model object of the target activity.
        activity = self.model.objects.create(**params)

        # build relationships with ancestors.
        if isinstance(_parent, self.model):
            rels = [ActivityRelationship(
                ancestor=_parent,
                descendant=activity,
                distance=1,
            )]
            for rel in _parent.ancestor_set.all():
                rels.append(ActivityRelationship(
                    ancestor=rel.ancestor,
                    descendant=activity,
                    distance=(rel.distance + 1),
                ))
            with transaction.atomic():
                ActivityRelationship.objects.bulk_create(rels)

        # signals must be sent outside transactions to prevent
        # phantom reads and transaction deadlocks
        signals.activity_created.send(sender=self.model,
                                      instance=activity)

        return activity

    def retry_activity(self, instance, *args, **kwargs):
        if instance.state == states.FAILED:
            return self._supersede(instance, *args, **kwargs)
        return False


class ActivityModel(models.Model):

    name = models.CharField(
        max_length=100,
    )
    identifier_code = models.SlugField(
        max_length=32,
        default=unique.uniqid,
    )
    token_code = models.SlugField(
        max_length=6,
        null=True,
        default=random.randstr,
    )

    # inputs and outputs
    inputs = models.ForeignKey(
        ActivityInputs,
        null=True,
        blank=True,
    )
    outputs = models.ForeignKey(
        ActivityOutputs,
        null=True,
        blank=True,
    )

    # states and transitions
    state = models.CharField(
        max_length=16,
        choices=zip(states.ALL_STATES, states.ALL_STATES),
        default=states.CREATED,
    )
    appointment = models.CharField(
        max_length=16,
        choices=zip(states.APPOINTABLE_STATES, states.APPOINTABLE_STATES),
        default='',
        blank=True,
    )
    status_code = models.IntegerField(
        blank=True,
        null=True,
    )

    snapshot = models.ForeignKey(ActivitySnapshot, null=True, blank=True)
    acknowledgment = models.PositiveSmallIntegerField(
        default=0,
    )

    # important datetimes
    date_created = models.DateTimeField(auto_now_add=True, blank=True)
    date_archived = models.DateTimeField(blank=True, null=True)

    descendants = models.ManyToManyField(
        'self',
        through='ActivityRelationship',
        through_fields=('ancestor', 'descendant'),
        symmetrical=False,
        related_name='ancestors'
    )

    objects = ActivityModelManager()

    class Meta:
        unique_together = ('identifier_code', 'token_code')

    def __unicode__(self):
        return unicode(u"[#%d] %s" % (
            self.pk,
            self.name
        ))

    def _ack(self):
        """
        Shortcut for self._acknowledge()
        """
        self._acknowledge()

    def _acknowledge(self):
        self.__class__.objects.filter(pk=self.pk) \
                              .update(acknowledgment=F('acknowledgment') + 1)

    @transaction.atomic
    def _appoint(self, to_state):
        """
        Set appointment state of this activity.
        """
        if to_state not in states.APPOINTABLE_STATES:
            raise RuntimeError("cat not appoint to state: %r" % to_state)

        if self.token_code:
            rows = self.__class__.objects.filter(
                pk=self.pk,
                token_code__isnull=False
            ).exclude(
                state__in=states.ARCHIVED_STATES
            ).update(appointment=to_state)
        else:
            rows = 0

        if rows:
            self.appointment = to_state
            self.descendants.exclude(token_code__isnull=True,
                                     state__in=states.ARCHIVED_STATES) \
                            .update(appointment=to_state)
            return True

        return False

    def _transit(self, to_state, **kwargs):
        if to_state not in states.TRANSITABLE_STATES:
            raise TypeError("cat not transit to state: %r" % to_state)

        appointment_flag = 0  # 没有处理预约
        if self.appointment:
            if self.appointment not in states.APPOINTABLE_STATES \
                    or to_state in states.ARCHIVED_STATES:
                appointment_flag = 1  # 处理了预约但未设置为预约状态
            elif states.State(to_state) < states.State(self.appointment) \
                    and states.can_transit(self.state, self.appointment):
                to_state = self.appointment
                appointment_flag = 2  # 设置为预约状态

        if states.can_transit(self.state, to_state) and self.token_code:
            kwargs.update({
                'token_code': random.randstr(),
                'state': to_state,
            })

            if appointment_flag:  # 一旦处理了预约，就将其置空
                kwargs['appointment'] = ''

            original_state = self.state

            with transaction.atomic():
                sid = transaction.savepoint()

                if to_state in states.ARCHIVED_STATES:
                    # prepare outputs model if necessary
                    data = kwargs.pop('data', None)
                    ex_data = kwargs.pop('ex_data', None)
                    if not (data is None and ex_data is None):
                        kwargs['outputs'] = ActivityOutputs.objects.create(
                            data=data,
                            ex_data=ex_data,
                        )

                    # clear snapshot model foreign key
                    if isinstance(self.snapshot, ActivitySnapshot):
                        kwargs['snapshot'] = None
                        _snapshot_id = self.snapshot_id

                    # set date_archived value to now
                    kwargs['date_archived'] = now()
                elif isinstance(kwargs.get('snapshot'), basestring):
                    snapshot, created = self._update_or_create_snapshot(
                        kwargs['snapshot']
                    )
                    if created:
                        kwargs['snapshot'] = snapshot
                    else:
                        del kwargs['snapshot']

                logger.info("transit activity #%s from %r to %r"
                            % (self.pk, self.state, to_state))

                rows = self.__class__.objects.filter(
                    pk=self.pk,
                    token_code=self.token_code
                ).update(**kwargs)

                if rows:
                    _snapshot_id = locals().get('_snapshot_id')
                    if isinstance(_snapshot_id, (int, long)):
                        ActivitySnapshot.objects.filter(pk=_snapshot_id) \
                                                .delete()
                    transaction.savepoint_commit(sid)

                    for k, v in kwargs.iteritems():
                        setattr(self, k, v)
                else:
                    transaction.savepoint_rollback(sid)

        if self.state == original_state:
            logger.info("transit activity #%s failed." % self.pk)
        else:
            # state change signal
            sc_signal = getattr(signals,
                                'activity_' + to_state.lower(),
                                None)

            if sc_signal:
                logger.info("send signal %r for activity #%s"
                            % (to_state, self.pk))
                # TODO: signals must be sent outside transactions to
                # prevent phantom reads and transaction deadlocks
                sc_signal.send(sender=self.__class__,
                               instance=self)

            if appointment_flag != 2:
                logger.info("transit activity #%s success." % self.pk)
                return True

        return False

    def _lazy_transit(self, to_state, countdown=10):
        signals.lazy_transit.send(sender=self.__class__,
                                  activity_id=self.pk,
                                  to_state=to_state,
                                  countdown=countdown)

    def _update_or_create_snapshot(self, snapshot):
        if isinstance(self.snapshot, ActivitySnapshot):
            ActivitySnapshot.objects.filter(pk=self.snapshot.pk) \
                                    .update(data=snapshot)
            obj = self.snapshot
            created = False
        else:
            obj = ActivitySnapshot.objects.create(
                data=snapshot,
            )
            created = True

        return obj, created

    @property
    def _snapshot(self):
        if isinstance(self.snapshot, ActivitySnapshot):
            return self.snapshot.data

    @property
    def args(self):
        if isinstance(self.inputs, ActivityInputs):
            return self.inputs.args
        else:
            return list()

    @property
    def kwargs(self):
        if isinstance(self.inputs, ActivityInputs):
            return self.inputs.kwargs
        else:
            return dict()

    @property
    def data(self):
        if isinstance(self.outputs, ActivityOutputs):
            return self.outputs.data

    @property
    def ex_data(self):
        if isinstance(self.outputs, ActivityOutputs):
            return self.outputs.ex_data

    @property
    def parent_id(self):
        if hasattr(self, '_parent_id'):
            return getattr(self, '_parent_id')
        else:
            try:
                self._parent_id = ActivityRelationship.objects.get(
                    descendant=self.pk,
                    distance=1,
                ).ancestor_id
            except ActivityRelationship.DoesNotExist:
                self._parent_id = None

            return self._parent_id

    @property
    def parent(self):
        if self.parent_id:
            return self.__class__.objects.get(pk=self.parent_id)

    def pause(self):
        """
        Pause this activity.
        """
        return self._appoint(states.SUSPENDED)

    def revoke(self):
        """
        Revoke this activity.
        """
        return self._appoint(states.REVOKED)

    def resume(self):
        u"""
        1、清理未执行的预约状态
        2、恢复当前活动
        3、恢复子活动
        """
        result = self._transit(states.READY)
        if result:
            self.descendants.exclude(token_code__isnull=True,
                                     state__in=states.ARCHIVED_STATES)

    def finish(self, data=None, ex_data=None, status_code=status.SUCCESS):
        """
        Finish this activity.
        """
        to_state = states.FAILED if status_code else states.FINISHED

        if not states.can_transit(self.state, to_state):
            raise RuntimeError("cat not transit to state: %r" % to_state)

        kwargs = {
            'data': data,
            'ex_data': ex_data,
            'status_code': status_code,
        }

        if not self._transit(to_state, **kwargs):
            raise RuntimeError("can not finish activity #%s from state: %r"
                               % (self.pk, self.state))


class ActivityRelationship(models.Model):

    ancestor = models.ForeignKey(
        ActivityModel,
        related_name='descendant_set',
        db_index=True
    )
    descendant = models.ForeignKey(
        ActivityModel,
        related_name='ancestor_set',
        db_index=True
    )
    distance = models.PositiveSmallIntegerField(
        default=0
    )

    class Meta:
        unique_together = (('ancestor', 'descendant'),
                           ('descendant', 'distance'))

    def __unicode__(self):
        return unicode(u"#%s -(%s)-> #%s" % (
            self.ancestor_id,
            self.distance,
            self.descendant_id,
        ))

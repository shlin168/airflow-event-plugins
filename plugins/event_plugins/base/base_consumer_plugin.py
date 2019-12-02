# -*- coding: UTF-8 -*-

import os
import time

from airflow.exceptions import AirflowException, AirflowSensorTimeout, \
    AirflowSkipException, AirflowRescheduleException
from airflow.models import TaskInstance, BaseOperator, SkipMixin, TaskReschedule
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow.utils import timezone
from airflow.settings import Session
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep

from event_plugins import factory
from event_plugins.common.schedule.timeout import TaskTimeout
from event_plugins.common.schedule.time_utils import TimeUtils
from event_plugins.common.status import DBStatus
from event_plugins.common.storage.db import get_session, USE_AIRFLOW_DATABASE
from event_plugins.common.storage.event_message import EventMessageCRUD, STORAGE_CONF
from event_plugins.common.success.success_mixin import SuccessMixin


class BaseConsumerOperator(BaseOperator, SuccessMixin, SkipMixin):

    ui_color = '#16a085'
    valid_modes = ['poke', 'reschedule']

    source_type = 'base'

    @apply_defaults
    def __init__(self,
                 msgs,
                 poke_interval,
                 timeout=None,
                 mark_success=False,
                 soft_fail=False,
                 mode='poke',
                 status_file=None,
                 debug_mode=False,
                 sensor_name=None,
                 *args,
                 **kwargs):
        super(BaseConsumerOperator, self).__init__(*args, **kwargs)
        self.timeout = timeout
        self.mark_success = mark_success
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.debug_mode = debug_mode

        # check parameters
        if sensor_name is None:
            sensor_name = ".".join([self.dag.dag_id, self.task_id])
        self.set_mode(mode)
        self.set_db_handler(sensor_name)
        self.set_all_msgs_handler(msgs)

    def set_mode(self, mode):
        if mode not in self.valid_modes:
            raise AirflowException(
                "The mode must be one of {valid_modes}, {d}.{t}'; received '{m}'."
                .format(valid_modes=self.valid_modes,
                        d=self.dag.dag_id if self.dag else "",
                        t=self.task_id,
                        m=mode))
        self.mode = mode

    def set_db_handler(self, sensor_name):
        session = get_session()
        self.db_handler = EventMessageCRUD(self.source_type, sensor_name, session)

    def set_all_msgs_handler(self, msgs):
        self.all_msgs_handler = factory.plugin_factory(self.source_type).all_msgs_handler(msgs)

    def poke(self, context, consumer):
        # initialize or update messages in status db before consuming messages
        self.initialize_db_handler()
        # start conuming and matching messages
        msg_list = consumer.get_messages()
        receive_dt = TimeUtils().get_now()
        received_msgs = list()
        for msg in msg_list:
            try:
                msg_value = factory.plugin_factory(self.source_type) \
                                .msg_handler(msg=msg, mtype='receive').value()
                match_wanted, receive_msg = self.all_msgs_handler.match(msg, receive_dt)
            except Exception, e:
                if self.debug_mode:
                    self.log.warning(e)
                    self.log.warning('[SkipMessage] {}'.format(msg_value))
            else:
                if match_wanted is not None:
                    received_msgs.append(match_wanted)
                    self.db_handler.update_on_receive(match_wanted, receive_msg)
                    if self.debug_mode:
                        self.log.info("Received wanted data: {}".format(msg_value))
                        self.log.info(self.db_handler.tabulate_data())
                    if self.mark_success:
                        self._mark_success_task_by_id(context, match_wanted['task_id'])
                else:
                    if self.debug_mode:
                        self.log.info('Received message and pass: {}'.format(msg_value))

        # mark skip if last_receive_time is not None and task status is None (received before)
        if self.mark_success:
            for have_successed_msg in self.db_handler.have_successed_msgs(received_msgs):
                self._mark_skip_task_by_id(context, have_successed_msg['task_id'])
        return self.is_criteria_met()

    def execute(self, context):
        if self.mark_success:
            self.downstream_tasks_map = dict([(task_id, context['dag_run'].get_task_instance(task_id))
                                              for task_id in context['task'].get_direct_relative_ids(upstream=False)])
            if self.debug_mode:
                self.log.info('downstream task {}'.format(self.downstream_tasks_map.keys()))

        # initialize connector
        self.initialize_conn_handler()
        started_at = TimeUtils().get_now()

        # If reschedule, use first start date of current try
        if self.reschedule:
            task_reschedules = TaskReschedule.find_for_task_instance(context['ti'])
            if task_reschedules:
                started_at = task_reschedules[0].start_date

        timeout_handler = TaskTimeout(context, self.poke_interval, self.timeout, started_at)
        self.log.info('Timeout datetime: {}'.format(timeout_handler.timeout_dt))
        while True:
            # check if task is timeout
            if timeout_handler.is_timeout():
                self.handle_timeout(context)

            # check if criteria met
            if self.poke(context, self.conn_handler):
                break

            # check if next schedule is timeout, set last time poke before actually timeout
            timeout_handler.execute_last_poke_after_secs = None
            if timeout_handler.is_next_poke_timeout():
                self.log.info('next poke will exceed timeout: {}'.format(timeout_handler.timeout_dt))
                if timeout_handler.execute_last_poke_after_secs:
                    self.log.info('poke after {}s'.format(
                        timeout_handler.execute_last_poke_after_secs))
                    self.schedule_next_time(timeout_handler.execute_last_poke_after_secs)
                else:
                    self.handle_timeout(context)
            else:
                self.schedule_next_time(self.poke_interval)

        # critieria met
        self.close_connection()
        self.log.info('get all wanted messages, close consumer and exit...')

    def close_connection(self):
        # close connection before exit
        # 1. close connection to source
        self.conn_handler.close()
        # 2. close db connection if not using airflow database to store messages status
        if USE_AIRFLOW_DATABASE is False:
            self.db_handler.session.remove()

    def schedule_next_time(self, seconds):
        # handle different mode: reschedule or poke
        if self.reschedule:
            self.close_connection()
            # use airflow timezone to get now here
            reschedule_date = TimeUtils().add_seconds(timezone.utcnow(), seconds)
            raise AirflowRescheduleException(reschedule_date)
        else:
            time.sleep(seconds)

    def handle_timeout(self, context):
        self.close_connection()
        if self.soft_fail and not context['ti'].is_eligible_to_retry():
            self._skip_unexecuted_downstream_tasks(context)
            raise AirflowSkipException('Snap. Time is OUT.')
        else:
            raise AirflowSensorTimeout('Snap. Time is OUT.')

    def initialize_conn_handler(self):
        raise NotImplementedError('implement how to connect to source and return connector')

    def initialize_db_handler(self):
        # Initialize status DB, clear last_receive_time if msg timeout
        # override if you need to render the messages
        msgs = self.all_msgs_handler.get_wanted_msgs()
        self.db_handler.initialize(msg_list=msgs)
        if self.debug_mode:
            self.log.info(self.db_handler.tabulate_data())

    def is_criteria_met(self):
        # check if condition met before exist poke function
        threshold = 50
        if self.debug_mode:
            threshold = None
        if self.db_handler.status() == DBStatus.ALL_RECEIVED:
            self.log.info(self.db_handler.tabulate_data(threshold=threshold))
            return True
        elif self.db_handler.status() == DBStatus.NOT_ALL_RECEIVED:
            unreceived_rmsgs = self.db_handler.get_unreceived_msgs()
            self.log.info(self.db_handler.tabulate_data(threshold=threshold))
            self.log.info('criteria not met in this round, require msgs {}'.format(unreceived_rmsgs))
            return False

    @property
    def reschedule(self):
        return self.mode == 'reschedule'

    @property
    def deps(self):
        """
        Adds one additional dependency for all sensor operators that
        checks if a sensor task instance can be rescheduled.
        """
        return BaseOperator.deps.fget(self) | {ReadyToRescheduleDep()}

    def _mark_success_task_by_id(self, context, task_id):
        ti = self.downstream_tasks_map[task_id]
        self.log.info('mark task success: {}'.format(task_id))
        self.success(context['dag_run'], context['ti'].execution_date, [ti])

    def _mark_skip_task_by_id(self, context, task_id):
        ti = self.downstream_tasks_map[task_id]
        if ti.current_state() == State.NONE:
            self.log.info('mark task skip since status is None: {}'.format(task_id))
            self.skip(context['dag_run'], context['ti'].execution_date, [ti])

    def _skip_unexecuted_downstream_tasks(self, context):
        unreceived_msgs = self.db_handler.get_unreceived_msgs()
        unexecuted_task_ids = self.all_msgs_handler.get_task_ids(unreceived_msgs)
        self.log.info('skip task: {}'.format(unexecuted_task_ids))
        unexecuted_tasks = map(lambda v: self.downstream_tasks_map[v], unexecuted_task_ids)
        if len(unexecuted_tasks) > 0:
            self.skip(context['dag_run'], context['ti'].execution_date, unexecuted_tasks)

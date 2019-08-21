# -*- coding: UTF-8 -*-
from __future__ import print_function

import six
from datetime import datetime
from dateutil.relativedelta import relativedelta
from croniter import croniter

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowException

from event_plugins.common.schedule.time_utils import TimeUtils


class TaskTimeout(LoggingMixin):

    task_type = None
    timeout_mode = None
    timeout_dt = None

    # poke last time before timeout. If the poke_interval is quite large (suce as 1 hour),
    # the messages received in last hour before timeout whould not be received.
    # just make sure the task is triggered for the last time before exit.
    last_poke_offset = 30

    execute_last_poke_after_secs = None

    def __init__(self, context, poke_interval, timeout=None, started_at=None):
        self.set_task_type(context)
        self.set_timeout_mode(timeout)
        self.timeout = timeout
        self.started_at = started_at
        if self.schedule_task:
            # adjust the logic in airflow
            time_delta = TimeUtils().time_delta(context['next_execution_date'],
                                                context['execution_date'])
            self.execution_date = context['next_execution_date']
            self.next_execution_date = self.execution_date + time_delta
        else:
            # manual task
            self.execution_date = context['execution_date']

        self.set_timeout_dt()
        self.poke_interval = poke_interval

    def set_task_type(self, context):
        interval = (context['next_execution_date'] - context['execution_date']).total_seconds()
        if interval > 0:
            self.task_type = 'scheduled'
        else:
            self.task_type = 'manual'

    def set_timeout_mode(self, timeout):
        if not timeout:
            self.timeout_mode = None
        elif isinstance(timeout, int):
            self.timeout_mode = 'seconds'
        elif isinstance(timeout, six.string_types):
            assert croniter.is_valid(timeout), "invalid timeout string, should be crontab format"
            self.timeout_mode = 'crontab'
        else:
            raise ValueError('only support int and crontab string, get {}'.format(timeout))

    def set_timeout_dt(self):
        started_at = self.started_at or self.execution_date
        if self.schedule_task:
            # schedule
            timeout_dt = None
            if self.crontab:
                timeout_dt = self.get_crontab_timeout(started_at, self.timeout)
                self.log.info('crontab: {}'.format(timeout_dt))
            elif self.seconds:
                timeout_dt = TimeUtils().add_seconds(started_at, self.timeout)
                self.log.info('seconds: {}'.format(timeout_dt))
            self.set_schedule_timeout(timeout_dt)
        elif self.manual_task:
            # manual
            assert self.timeout, 'timeout need to be set since DAG is not scheduled'
            if self.crontab:
                now = TimeUtils().get_now()
                self.timeout_dt = self.get_crontab_timeout(now, self.timeout)
            elif self.seconds:
                self.timeout_dt = self.execution_date + relativedelta(seconds=self.timeout)

    def get_crontab_timeout(self, base, crontab_string):
        iter = croniter(crontab_string, base)
        return iter.get_next(datetime)

    def set_schedule_timeout(self, timeout_dt):
        if timeout_dt:
            if timeout_dt > self.next_execution_date:
                self.log.warning('timeout {} > next schedule date, adjust to {}'
                        .format(self.timeout_dt, self.next_execution_date))
                self.timeout_dt = self.next_execution_date
            else:
                self.timeout_dt = timeout_dt
        else:
            self.log.info('timeout not set, set next timeout to {}'.format(self.next_execution_date))
            self.timeout_dt = self.next_execution_date
        self.log.info('set timeout to {}'.format(self.timeout_dt))

    @property
    def schedule_task(self):
        return self.task_type == 'scheduled'

    @property
    def manual_task(self):
        return self.task_type == 'manual'

    @property
    def crontab(self):
        return self.timeout_mode == 'crontab'

    @property
    def seconds(self):
        return self.timeout_mode == 'seconds'

    def is_timeout(self):
        now = TimeUtils().get_now()
        if now >= self.timeout_dt:
            self.log.info('timeout')
            return True
        else:
            return False

    def is_next_poke_timeout(self, now=None):
        now = now or TimeUtils().get_now()
        next_poke_dt = now + relativedelta(seconds=self.poke_interval)
        if next_poke_dt >= self.timeout_dt:
            self.set_execute_last_poke_after_secs()
            return True
        else:
            return False

    def set_execute_last_poke_after_secs(self):
        execute_last_poke_after_secs = ((self.timeout_dt - TimeUtils().get_now()).total_seconds()
                                            - self.last_poke_offset)
        if execute_last_poke_after_secs > 0:
            self.execute_last_poke_after_secs = execute_last_poke_after_secs

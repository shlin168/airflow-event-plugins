# -*- coding: UTF-8 -*-
from __future__ import print_function

import datetime as dt
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
import pendulum
import six
import time

from airflow.utils import timezone
from airflow.settings import TIMEZONE as AIRFLOW_TIMEZONE

from event_plugins.common.storage.db import STORAGE_CONF


AIRFLOW_EVENT_PLUGINS_TIMEZONE = pendulum.timezone('UTC')
if STORAGE_CONF.get("Timezone", "timezone") is not None:
    AIRFLOW_EVENT_PLUGINS_TIMEZONE = pendulum.timezone(STORAGE_CONF.get("Timezone", "timezone"))
else:
    # use timezone in airflow if not set
    AIRFLOW_EVENT_PLUGINS_TIMEZONE = AIRFLOW_TIMEZONE


'''
All the time and timezone handling stuff are aggreated in this module
'''
class TimeUtils(object):

    def get_now(cls, tz=AIRFLOW_EVENT_PLUGINS_TIMEZONE):
        return cls.make_aware(dt.datetime.now(), tz)

    def datetime(cls, *args, **kwargs):
        # TODO: using timezone.datetime from airflow
        return dt.datetime(*args, **kwargs)

    def cvt_if_pendulumdt2dt(cls, base):
        '''Convert to datetime object if it's pendulum datetime
            Args:
                base (pendumlum datetime | time-aware/naive datetime)
            Returns:
                time-aware/naive datetime object of input
            Notice:
                If airflow trigger a scheduled task,
                    type(context['execution_date']) = <pendulum.pendulum.Pendulum>
                    type(context['next_execution_date'] = <datetime.datetime>
                If airflow trigger a manual task,
                    type(context['execution_date']) = <pendulum.pendulum.Pendulum>
                    type(context['next_execution_date']) = <pendulum.pendulum.Pendulum>
                Event plugins use datetime.datetime. which gets wrong in datetime comparison
                for computing timeout in manual task ...
                ---
                2019-12-05 20:40:20.000000+00:00 <datetime.datetime>
                2019-12-02T20:40:20.000000+00:00 <pendulum.pendulum.Pendulum>
                ---
        '''
        if isinstance(base, pendulum.pendulum.Pendulum):
            if base.tzinfo is not None:
                base_tz = base.tzinfo
                return dt.datetime.fromtimestamp(base.timestamp()).replace(tzinfo=base_tz)
            else:
                return dt.datetime.fromtimestamp(base.timestamp())
        elif isinstance(base, dt.date) or isinstance(base, dt.datetime):
            return base
        else:
            print(type(base))
            raise TypeError

    def is_naive(cls, base):
        return timezone.is_naive(base)

    def make_naive(cls, base, tz=AIRFLOW_EVENT_PLUGINS_TIMEZONE):
        if cls.is_naive(base):
            return base
        return timezone.make_naive(base, tz)

    def make_aware(cls, base, tz=AIRFLOW_EVENT_PLUGINS_TIMEZONE):
        ''' Add timezone '''
        if cls.is_naive(base):
            return timezone.make_aware(base, tz)
        else:
            return timezone.make_aware(cls.make_naive(base), tz)

    def add_seconds(cls, base, seconds, fmt="%Y-%m-%d %H:%M:%S"):
        '''Add offset to time
            Args:
                base (int | time-aware/naive datetime| string): base before offset
                seconds (int): offset seconds
            Returns:
                offset_dt (datetime): datetime after offset (keep timezone if input type is time-aware datetime)
        '''
        return cls.cvt_datetime(base, fmt) + relativedelta(seconds=seconds)

    def add_days(cls, base, days, fmt="%Y-%m-%d %H:%M:%S"):
        '''Add day offset to time
            Args:
                base (int | time-aware/naive datetime| string): base before offset
                days (int): offset days
            Returns:
                offset_dt (datetime): datetime after offset (keep timezone if input type is time-aware datetime)
        '''
        return cls.cvt_datetime(base, fmt) + relativedelta(days=days)

    def add_months(cls, base, months, fmt="%Y-%m-%d %H:%M:%S"):
        '''Add month offset to time
            Args:
                base (int | time-aware/naive datetime| string): base before offset
                days (int): offset months
            Returns:
                offset_dt (datetime): datetime after offset (keep timezone if input type is time-aware datetime)
        '''
        return cls.cvt_datetime(base, fmt) + relativedelta(months=months)

    def cvt_datetime(cls, base, fmt="%Y-%m-%d %H:%M:%S"):
        '''Convert to datetime
            Args:
                base(int | time-aware/naive datetime | date | string)
            Returns:
                datetime(datetime)
        '''
        if isinstance(base, six.string_types):
            base = dt.datetime.strptime(base, fmt)
        elif isinstance(base, int):
            base = dt.datetime.fromtimestamp(base)
        elif isinstance(base, dt.date) or isinstance(base, dt.datetime):
            pass
        else:
            print(type(base))
            raise TypeError
        return base

    def cvt_datetime2str(cls, base, fmt='%Y-%m-%d %H:%M:%S'):
        '''Convert datetime to string
            Args:
                base(int | datetime): the object to be converted to string
                fmt (str): output datetime string format
            Returns:
                string in datetime format or None if parsing error
        '''
        if isinstance(base, int):
            base = dt.datetime.fromtimestamp(base)
        return base.strftime(fmt)

    def time_delta(cls, dt1, dt2):
        ''' Time delta between two datetime '''
        return relativedelta(dt1, dt2)

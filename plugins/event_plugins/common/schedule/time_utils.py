# -*- coding: UTF-8 -*-
from __future__ import print_function
import six

import time
import datetime as dt
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from airflow.utils import timezone

'''
All the time handling stuff in event_plugin are aggreated in this module
'''

class TimeUtils(object):

    def get_now(cls, tz=None):
        return cls.make_aware(dt.datetime.now(), tz)

    def datetime(cls, *args, **kwargs):
        # TODO: using timezone.datetime from airflow
        return dt.datetime(*args, **kwargs)

    def make_aware(cls, base, tz=None):
        ''' Add timezone '''
        if timezone.is_naive(base):
            return timezone.make_aware(base, tz)
        else:
            return timezone.make_aware(timezone.make_naive(base), tz)

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

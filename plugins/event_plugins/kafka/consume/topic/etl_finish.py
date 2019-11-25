# -*- coding: UTF-8 -*-

from event_plugins.common.schedule.time_utils import TimeUtils
from event_plugins.kafka.consume.topic.basic import BasicMessage
from event_plugins.kafka.consume.utils import render_func


class ETLFinish:

    def __init__(self, name):
        self.name = name

    def msg_handler(self, wanted_msg):
        """
        :type wanted_msg: dict object
        """
        return Message(wanted_msg)


class Message(BasicMessage):

    offset_sec = 0
    offset_day = 2

    match_keys = ['db', 'table']
    render_match_keys = [('partition_values', {'yyyymm': '_get_exec_partition'})]
    time_key = 'exec_date'

    def __init__(self, wanted_msg):
        super(Message, self).__init__(wanted_msg)

    '''
        define corresponding render functions for keys in self.render_match_keys
    '''
    @render_func(key='yyyymm')
    def _get_exec_partition(self):
        offset_date = TimeUtils().add_seconds(TimeUtils().get_now(), self.offset_sec)
        exec_dt = TimeUtils().add_days(offset_date, 0-self.offset_day)
        return exec_dt

    def get_match_handler(self):
        return ETLMatch


class ETLMatch:

    @staticmethod
    def match_by_keys(msg, wanted_msg, match_keys):
        return all([msg.get(key) == wanted_msg.get(key) for key in match_keys])

    @staticmethod
    def match_by_rkeys(msg, wanted_msg, match_keys):
        return all([msg.get(key) == wanted_msg.get(key) for key in match_keys])

    @staticmethod
    def match_by_tkey(msg_dt, wanted_dt):
        msg_date = TimeUtils().cvt_datetime2str(msg_dt, fmt='%Y%m%d')
        wanted_date = TimeUtils().cvt_datetime2str(wanted_dt, fmt='%Y%m%d')
        if msg_date == wanted_date:
            return True
        return False

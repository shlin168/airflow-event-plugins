# -*- coding: UTF-8 -*-

from event_plugins.kafka.message.topic.basic import BasicMessage
from event_plugins.common.schedule.time_utils import TimeUtils


class HippoFinish:

    def __init__(self, name):
        self.name = name

    def msg_handler(self, wanted_msg):
        return Message(wanted_msg)


class Message(BasicMessage):

    offset_sec = 7200
    offset_day = 0

    match_keys = ['hippo_name', 'job_name', 'is_success']
    render_match_keys = []
    time_key = 'finish_time'

    def __init__(self, wanted_msg):
        super(Message, self).__init__(wanted_msg)

    def get_match_handler(self):
        return HippoFinishMatch


class HippoFinishMatch:

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

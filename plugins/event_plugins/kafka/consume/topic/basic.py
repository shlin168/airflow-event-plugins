# -*- coding: UTF-8 -*-
from __future__ import print_function

from event_plugins.common.schedule.time_utils import TimeUtils


class BasicMessage(object):
    ''' Preprocess and match wanted msgs and incoming msg in one topic
        Args:
            offset_sec(int): the offset seconds to the start of the day
                E.g., if the msgs come after 22:00 is regard as next-day msgs,
                then the offset is 2 hours (7200 secs)
            offset_day(int): offset to the actual date of data
            match_keys(list): the keys that map the full value between msgs defined in conf
                and the coming msgs
            render_match_keys(list): the keys that render the msgs defined in conf first,
                then map the full value between msgs defined in conf and the coming msgs
                E.g., [('partition_values', {'yyyymm': '_get_exec_partition'})]
                    the value of partition_values would be rendered, {{ yyyymm }} would be
                    the return value of self._get_exec_partition
            time_key(str): the key that show the event finish time, won't compare if not given.
                compare method could be written in JsonMatch class
    '''

    offset_sec = 0
    offset_day = 0
    valid_freq = ['D', 'M']

    match_keys = []
    render_match_keys = []
    time_key = None

    def __init__(self, wanted_msg):
        self.wanted_msg = wanted_msg

    def _get_all_keys(self):
        check_keys = self.match_keys + [k for k, _ in self.render_match_keys]
        if self.time_key:
            check_keys += [self.time_key]
        return check_keys

    def _check_valid_msg(self, msg):
        for k in self._get_all_keys():
            if k not in msg:
                return False
        return True

    def time_offset(self, base):
        ''' Add offset to time
            Args:
                base (int | time-aware datetime | naive datetime): base(timestamp or datetime) before offset
            Returns:
                offset_dt (datetime): datetime after offset (keep timezone if input type is time-aware datetime)
        '''
        return TimeUtils().add_seconds(base, self.offset_sec)

    def match(self, msg, receive_dt):
        ''' Define how to match a received message with self.wanted_msg
            Args:
                msg (json object): received message
                receive_dt(datetime): time when the message is consumed, with timezone
            Returns:
                match or not (boolean)
        '''
        if not self._check_valid_msg(msg):
            raise ValueError("[MessageFormatError] msg send to topic '{}' ".format(self.wanted_msg['topic']) +
                             "need to have keys: {}".format(self._get_all_keys()))
        match_handler = self.get_match_handler()

        if match_handler.match_by_keys(msg, self.wanted_msg, self.match_keys):
            if self.time_key is not None and self.time_key not in msg:
                print("specify time key '{}' in msg for matching".format(self.time_key))
            elif (self.time_key is None) or \
                (match_handler.match_by_tkey(self.time_offset(msg[self.time_key]),
                                             self.time_offset(receive_dt))):

                # return if there's no other keys need to be match
                if len(self.render_match_keys) == 0:
                    print('match with {}'.format(self.match_keys))
                    return True

                # match if there are render keys
                render_keys = [key for key, _ in self.render_match_keys]
                if match_handler.match_by_rkeys(msg, self.wanted_msg, render_keys):
                    print('match with {}'.format(self.match_keys + render_keys))
                    return True
        return False

    def timeout(self):
        ''' Get timeout depends on self.wanted_msg
            Returns:
                timeout(datetime): timeout datetime with timezone
        '''
        base_time = TimeUtils().add_seconds(TimeUtils().get_now(), self.offset_sec)
        if self.wanted_msg['frequency'] == 'D':
            end_of_day = base_time.replace(hour=23, minute=59, second=59)
            return TimeUtils().add_seconds(end_of_day, 0-self.offset_sec)
        elif self.wanted_msg['frequency'] == 'M':
            start_of_next_month = TimeUtils().add_months(base_time, 1) \
                                    .replace(day=1, hour=0, minute=0, second=0)
            end_of_month = TimeUtils().add_seconds(start_of_next_month, -1)
            return TimeUtils().add_seconds(end_of_month, 0-self.offset_sec)
        else:
            raise ValueError('frequency available: {}'.format(self.valid_freq))

    def get_match_handler(self):
        return Match


class Match:

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

# -*- coding: UTF-8 -*-
from airflow.utils.decorators import apply_defaults

from event_plugins.common.schedule.time_utils import TimeUtils
from event_plugins.base.base_connector import BaseConnector
from event_plugins.base.base_handler import BaseHandler
from event_plugins.base.base_handler import BaseAllMessageHandler
from event_plugins.base.base_handler import BaseSingleMessageHandler
from event_plugins.base.base_consumer_plugin import BaseConsumerOperator


class MockBaseConsumerOperator(BaseConsumerOperator):

    name = "test"

    @apply_defaults
    def __init__(self,
                 *args,
                 **kwargs):
        super(MockBaseConsumerOperator, self).__init__(*args, **kwargs)

    def set_all_msgs_handler(self, msgs):
        self.all_msgs_handler = MockBaseHandler('test').all_msgs_handler(msgs)

    def initialize_conn_handler(self):
        self.conn_handler = MockBaseHandler('test').conn_handler()


class MockBaseConnector(BaseConnector):

    def set_consumer(self):
        pass

    def set_producer(self):
        pass

    def get_messages(self):
        ''' return message in list format '''
        pass

    def close(self):
        pass


class MockBaseHandler(BaseHandler):

    valid_mtypes = ['wanted', 'receive']

    def __init__(self, name):
        self.name = name

    def all_msgs_handler(self, wanted_msgs):
        return MockBaseAllMessageHandler(wanted_msgs)

    def msg_handler(self, msg, mtype):
        if mtype == 'wanted':
            return MockBaseSingleMessageHandler().set_wanted_msg(msg)
        elif mtype == 'receive':
            return MockBaseSingleMessageHandler().set_receive_msg(msg)
        else:
            raise ValueError('Avaliable mtype:', self.valid_mtypes)

    def conn_handler(self):
        return MockBaseConnector()


class MockBaseAllMessageHandler(BaseAllMessageHandler):

    def __init__(self, wanted_msgs):
        self.wanted_msgs = wanted_msgs

    def get_task_ids(self):
        return self.wanted_msgs

    def match(self, receive_msg, receive_dt=None):
        ''' Check if incoming message match one of the wanted_msgs '''
        try:
            receive_dt = receive_dt or TimeUtils().get_now()
            for wmsg in self.wanted_msgs:
                if (wmsg['task_id'] == receive_msg and
                    receive_dt.strftime('%Y%m%d') == TimeUtils().get_now().strftime('%Y%m%d')):
                    return wmsg, receive_msg
        except:
            raise


class MockBaseSingleMessageHandler(BaseSingleMessageHandler):

    class WantedMessage:

        def __init__(self, msg):
            self.msg = msg

        def timeout(self):
            if self.msg['frequency'] == 'D':
                return TimeUtils().get_now().replace(hour=23, minute=59, second=59)
            elif self.msg['frequency'] == 'M':
                start_of_next_month = TimeUtils().add_months(TimeUtils().get_now(), 1) \
                                        .replace(day=1, hour=0, minute=0, second=0)
                return TimeUtils().add_seconds(start_of_next_month, -1)

    class ReceiveMessage:

        def __init__(self, msg):
            self.msg = msg

        def value(self):
            return self.msg

    def set_wanted_msg(self, msg):
        return self.WantedMessage(msg)

    def set_receive_msg(self, msg):
        ''' use msg.value() to get value of kafka message'''
        return self.ReceiveMessage(msg)

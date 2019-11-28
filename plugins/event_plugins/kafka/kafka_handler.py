# -*- coding: UTF-8 -*-
from __future__ import print_function

import six
import json

from event_plugins.base.base_handler import BaseHandler
from event_plugins.base.base_handler import BaseAllMessageHandler
from event_plugins.base.base_handler import BaseSingleMessageHandler

from event_plugins.kafka.kafka_connector import KafkaConnector
from event_plugins.kafka.consume.topic import topic_factory
from event_plugins.kafka.consume.utils import MsgRenderUtils


class KafkaHandler(BaseHandler):

    valid_mtypes = ['wanted', 'receive']

    def __init__(self, name):
        self.name = name

    def all_msgs_handler(self, wanted_msgs):
        return KafkaAllMessageHandler(wanted_msgs)

    def msg_handler(self, msg, mtype):
        if mtype == 'wanted':
            return KafkaSingleMessageHandler().set_wanted_msg(msg)
        elif mtype == 'receive':
            return KafkaSingleMessageHandler().set_receive_msg(msg)
        else:
            raise ValueError('Avaliable mtype:', self.valid_mtypes)

    def conn_handler(self, broker):
        return KafkaConnector(broker)


class KafkaAllMessageHandler(BaseAllMessageHandler):

    def __init__(self, wanted_msgs):
        self.wanted_msgs = wanted_msgs

    def get_wanted_msgs(self, topic=None, render=False):
        ''' Get wanted msgs

        If topic is None, get all wanted msgs, else wanted msgs in topic

        Args:
            topic(str): kafka topic name
            render(boolean): get the messages before or after rendering
        Returns:
            list of json object messages
        '''
        msgs = self.__render_msgs() if render else self.wanted_msgs
        return msgs if not topic else filter(lambda m: m['topic'] == topic, msgs)

    def get_task_ids(self, msgs=None):
        ''' Get task id from messages
            Args:
                msgs (list): list of json object messages
            Returns:
                task_ids (list): list of task_id (string)
        '''
        if msgs:
            return [m['task_id'] for m in msgs]
        return [m['task_id'] for m in self.wanted_msgs]

    def subscribe_topics(self):
        ''' Get distinct subscribe topics from all wanted messages
            Returns:
                topics (list): all topics that need to subscribe
        '''
        return list(set([msg['topic'] for msg in self.wanted_msgs]))

    def __render_msgs(self):
        ''' Render all wanted msgs '''
        if hasattr(self, 'render_msgs'):
            return self.render_msgs
        else:
            self.render_msgs = [KafkaHandler('kafka').msg_handler(msg, 'wanted').render()
                                    for msg in self.wanted_msgs]
        return self.render_msgs

    def match(self, receive_msg, receive_dt):
        ''' Check if incoming message match one of the wanted_msgs

            Args:
                receive_msg(confluent_kafka.Message): incoming message
                receive_dt(datetime): receiving time

            Returns:
                json or None. return wanted_msg and receive_msg if matched, None otherwise
        '''
        try:
            receive = KafkaHandler('kafka').msg_handler(receive_msg, 'receive')
            receive_msg_topic = receive.topic()
            receive_msg_value =  receive.convert2json()

            topic_wanted_msgs = self.get_wanted_msgs(topic=receive_msg_topic, render=True)
            for wanted_msg in topic_wanted_msgs:
                topic_handler = topic_factory(receive_msg_topic).msg_handler(wanted_msg)
                if topic_handler.match(receive_msg_value, receive_dt):
                    return wanted_msg, receive_msg_value
            return None, None
        except:
            raise


class KafkaSingleMessageHandler(BaseSingleMessageHandler):
    '''Handle single msg (json format), might be used to handle wanted message or received message
        Example:
            wanted_msg:
                {'frequency': 'D', 'topic': 'etl-finish', 'db': 'db1', 'table': 'table1',
                    'partition_values': "{{yyyymm|dt.format(format='%Y%m')}}", 'task_id': "etl-finish-tblb"}
            received_msg:
                {"db": "db1", "table": "table1", "partition_fields": "yyyymm",
                    "partition_values": "201906", "timestamp":1560925430}"""
    '''
    class WantedMessage:

        def __init__(self, msg):
            self.msg = msg

        def render(self):
            '''Render msg
                Note:
                    render functions need to be in topic handler class
                More Info:
                    value of topic.render_match_keys:
                        [('partition_values', {'yyyymm': '_get_exec_partition'})]
                    render_results:
                        [('partition_values', {'yyyymm': '201904'})]
            '''
            topic_handler = topic_factory(self.msg['topic']).msg_handler(wanted_msg=self.msg)
            if len(topic_handler.render_match_keys) > 0:
                render_result = MsgRenderUtils.get_render_dict(topic_handler, topic_handler.render_match_keys)
                for rkey, rdict in render_result:
                    if self.msg.get(rkey):
                        new_msg = self.msg.copy()
                        new_msg[rkey] = MsgRenderUtils.render(new_msg[rkey], rdict)
                        return new_msg
            return self.msg

        def timeout(self):
            topic_handler = topic_factory(self.msg['topic']).msg_handler(self.msg)
            return topic_handler.timeout()


    class ReceiveMessage:

        def __init__(self, msg):
            self.msg = msg

        def value(self):
            ''' msg.value() will get the value of kafka message '''
            return self.msg.value()

        def topic(self):
            return self.msg.topic()

        def convert2json(self):
            try:
                if isinstance(self.value(), six.string_types):
                    return json.loads(self.value())
                elif isinstance(self.value(), dict):
                    return self.value()
            except:
                raise ValueError('[MessageFormatError] msg not in json format')

    def set_wanted_msg(self, msg):
        return self.WantedMessage(msg)

    def set_receive_msg(self, msg):
        return self.ReceiveMessage(msg)

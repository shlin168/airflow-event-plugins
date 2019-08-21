# -*- coding: UTF-8 -*-
from __future__ import print_function

import json
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from event_plugins.factory import plugin_factory
from event_plugins.kafka.produce.utils import merge_multiple_files
from event_plugins.kafka.produce.factory import topic_factory


class KafkaProducerOperator(BaseOperator):

    ui_color = '#4db8ff'

    name = "kafka"

    @apply_defaults
    def __init__(self,
                 broker,
                 topic,
                 data,
                 *args,
                 **kwargs):
        super(KafkaProducerOperator, self).__init__(*args, **kwargs)
        self.broker = broker
        self.topic = topic
        self.data = data

    def initialize_conn_handler(self):
        self.conn_handler = plugin_factory(self.name).conn_handler(self.broker)
        self.conn_handler.set_producer()

    def execute(self, context):
        self.initialize_conn_handler()
        for data in self.data:
            self.conn_handler.produce(self.topic, data)


class KafkaProducerFromFileOperator(BaseOperator):

    ui_color = '#809fff'

    name = "kafka"

    @apply_defaults
    def __init__(self,
                 broker,
                 file_list,
                 *args,
                 **kwargs):
        super(KafkaProducerFromFileOperator, self).__init__(*args, **kwargs)
        self.broker = broker
        self.file_list = file_list

    def initialize_conn_handler(self):
        self.conn_handler = plugin_factory(self.name).conn_handler(self.broker)
        self.conn_handler.set_producer()

    def execute(self, context):
        self.initialize_conn_handler()
        result = merge_multiple_files(self.file_list)
        for topic, msgs in result.iteritems():
            for msg in msgs:
                self.log.info("try producing {data} to {topic}".format(data=msg, topic=topic))
                self.conn_handler.produce(topic, json.dumps(msg))


class KafkaProducerFromMergeFileOperator(KafkaProducerFromFileOperator):

    ui_color = '#A9BCF5'

    name = "kafka"

    @apply_defaults
    def __init__(self,
                 match_dict,
                 *args,
                 **kwargs):
        super(KafkaProducerFromMergeFileOperator, self).__init__(*args, **kwargs)
        self.match_dict = match_dict

    def execute(self, context):
        self.initialize_conn_handler()
        result = merge_multiple_files(self.file_list)
        for topic, msgs in result.iteritems():
            handler_cls = topic_factory(topic)
            if handler_cls:
                msg_iterator = self.get_merge_msg(handler_cls, topic, msgs)
                for msg in msg_iterator:
                    if msg:
                        msg_str = json.dumps(msg, ensure_ascii=False)
                        self.conn_handler.produce(topic, msg_str)

    def get_merge_msg(self, handler_cls, topic, msgs):
        match_grps = self.match_dict.get(handler_cls(None).__class__.__name__)
        if not match_grps:
            self.log.warning("{h} not found in match_dict, but there are messages with topic: {t}"
                " that can be processed by {h}. Define (key: value) in match_dict"
                " to merge messages".format(
                    h=handler_cls(None).__class__.__name__,
                    t=topic))
        else:
            for match_method in match_grps:
                handler = handler_cls(match_method)
                groups = filter(handler.match, msgs)
                msg = handler.merge_messages(groups)
                yield msg

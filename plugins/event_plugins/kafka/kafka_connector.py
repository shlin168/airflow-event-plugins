# -*- coding: UTF-8 -*-
from __future__ import print_function

import time
from confluent_kafka import Consumer, KafkaError, Producer

from event_plugins.base.base_connector import BaseConnector


class KafkaConnector(BaseConnector):

    consumer = None
    producer = None

    def __init__(self, broker):
        super(KafkaConnector, self).__init__()
        self.broker = broker

    def set_consumer(self, group_id, client_id, topics, timeout=5):
        self._set_consumer(self.broker, group_id, client_id, timeout)
        self._subscribe(topics)
        # TODO: sleep to wait for assigned finish, but not in a good way
        time.sleep(2)

    def set_producer(self):
        self._set_producer()

    def get_messages(self):
        all_msgs = list()
        msgs = self._consume_valid_messages()
        while msgs:
            all_msgs.extend(msgs)
            msgs = self._consume_valid_messages()
        return all_msgs

    def close(self):
        if self.consumer:
            self.consumer.close()

    def _set_consumer(self, broker, group_id, client_id, timeout=5):
        def on_commit(err, part):
            print("[commit]", part)

        if not self.consumer:
            self.consumer = Consumer({
                'bootstrap.servers': broker,
                'group.id': group_id,
                'client.id': client_id,
                'auto.offset.reset': 'earliest',
                'session.timeout.ms': (timeout + 1) * 1000,   # [magic] add this line for reschedule consumer to work...
                'enable.auto.commit': True,
                'on_commit': on_commit
            })
        return self

    def _subscribe(self, topics):
        def on_assign(consumer, part):
            print('[on assign]', part)

        if self.consumer:
            self.consumer.subscribe(topics, on_assign=on_assign)
        else:
            raise ValueError('consumer not set, can not assigined to any topic')

    def _is_valid_msg(self, msg):
        if msg is None:
            return False
        elif msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                self.log.warning(msg.error())
            return False
        return True

    def _consume_valid_messages(self, num_messages=1000, timeout=5):
        msg_list = self.consumer.consume(num_messages=num_messages, timeout=timeout)
        if msg_list is not None or len(msg_list) > 0:
            return [m for m in msg_list if self._is_valid_msg(m)]
        return None

    def _set_producer(self):
        self.producer = Producer({
            'bootstrap.servers': self.broker
        })
        return self

    def produce(self, topic, data, callback=None):
        # Trigger any available delivery report callbacks from previous produce() calls
        self.producer.poll(0)

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        if not callback:
            callback = self.delivery_report
        self.producer.produce(
            topic,
            data,
            callback=callback
        )
        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        self.producer.flush()

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            self.log.info('Message delivery failed: {}'.format(err))
        else:
            self.log.info('Message delivered to {} [{}]'.format(
                msg.topic(), msg.partition()))

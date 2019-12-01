# -*- coding: UTF-8 -*-
import json
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from event_plugins.factory import plugin_factory
from event_plugins.base.base_consumer_plugin import BaseConsumerOperator


class KafkaConsumerOperator(BaseConsumerOperator):

    ui_color = '#16a085'

    source_type = 'kafka'

    @apply_defaults
    def __init__(self,
                 broker,
                 group_id,
                 client_id,
                 *args,
                 **kwargs):
        super(KafkaConsumerOperator, self).__init__(*args, **kwargs)
        self.broker = broker
        self.group_id = group_id
        self.client_id = client_id

    def initialize_conn_handler(self):
        topics = self.all_msgs_handler.subscribe_topics()
        self.conn_handler = plugin_factory(self.source_type).conn_handler(self.broker)
        self.conn_handler.set_consumer(self.group_id, self.client_id, topics)

    def initialize_db_handler(self):
        # Initialize status DB, clear last_receive_time if msg timeout
        rmsgs = self.all_msgs_handler.get_wanted_msgs(render=True)
        self.db_handler.initialize(rmsgs)
        if self.debug_mode:
            self.log.info(self.db_handler.tabulate_data())

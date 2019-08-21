# -*- coding: UTF-8 -*-

from airflow.utils.log.logging_mixin import LoggingMixin


class BaseConnector(LoggingMixin):

    def set_consumer(self):
        raise NotImplementedError('''
            implement how to set connection to receive messages,
            assined the connector to self.consumer
        ''')

    def set_producer(self):
        ''' set connection and assined the connector to self.producer '''
        pass

    def get_messages(self):
        raise NotImplementedError('''
            implement how to get messages, and return message in list format
        ''')

    def close(self):
        raise NotImplementedError('''
            implement how to close connection, such as self.consumer.close()
        ''')

# -*- coding: UTF-8 -*-

from event_plugins.kafka.kafka_handler import KafkaHandler


def plugin_factory(plugin_name):
    '''
        add handler if there's other plugin that need to use functions in common module
    '''
    if plugin_name == 'kafka':
        handler = KafkaHandler
    else:
        raise ValueError('unknown handler')

    return handler(plugin_name)

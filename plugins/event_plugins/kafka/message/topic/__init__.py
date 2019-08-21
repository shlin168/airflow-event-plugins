# -*- coding: UTF-8 -*-
from event_plugins.kafka.message.topic.frontier_adw import FrontierAdw
from event_plugins.kafka.message.topic.hippo_finish import HippoFinish


topic_map = {
    'frontier-adw': FrontierAdw,
    'hippo-finish': HippoFinish
}

def topic_factory(topic_name):
    if topic_map.get(topic_name):
        return topic_map[topic_name](topic_name)
    else:
        raise ValueError('kafka topic {t} is undefined'.format(t=topic_name))

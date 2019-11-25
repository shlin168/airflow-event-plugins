# -*- coding: UTF-8 -*-
from event_plugins.kafka.consume.topic.etl_finish import ETLFinish
from event_plugins.kafka.consume.topic.job_finish import JobFinish


topic_map = {
    'etl-finish': ETLFinish,
    'job-finish': JobFinish
}

def topic_factory(topic_name):
    if topic_map.get(topic_name):
        return topic_map[topic_name](topic_name)
    else:
        raise ValueError('kafka topic {t} is undefined'.format(t=topic_name))

# -*- coding: UTF-8 -*-
from event_plugins.kafka.produce.topic.hive_sink_finish import HiveSinkFinish
from event_plugins.kafka.produce.topic.job_finish import JobFinish


topic_map = {
    'hive-sink-finish': HiveSinkFinish,
    'job-finish': JobFinish
}

def topic_factory(topic_name):
    if topic_map.get(topic_name):
        return topic_map[topic_name]
    elif topic_map.get('-'.join(topic_name.split('-')[1:])):
        return topic_map['-'.join(topic_name.split('-')[1:])]
    else:
        print('No handler class for topic {t}'.format(t=topic_name))

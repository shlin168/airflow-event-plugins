from event_plugins.kafka.produce.topic.base import Base
from event_plugins.kafka.produce.utils import merge_dicts


class HiveSinkFinish(Base):
    '''
        match_method = [
            {   # group 1: produce 1 message
                'db': 'btmp_cmd',
                'table': 'test1',
                'partition_fields': 'group'},
            {   # group 2: produce 1 message
                'db': 'btmp_cmd',
                'table': 'test2',
                'partition_fields': 'data_date/group'
        ]
    '''

    def __init__(self, match_method):
        super(HiveSinkFinish, self).__init__(match_method)

    def merge_messages(self, msg_list):
        if len(msg_list) > 0:
            new_msg = msg_list[0]
            new_msg["partition_values"] = "+".join(v["partition_values"] for v in msg_list)
            new_msg["system_datetime"] = max([v["system_datetime"] for v in msg_list])
            new_msg["count"] = sum([v["count"] for v in msg_list])
            new_msg["duration_time"] = sum([v["duration_time"] for v in msg_list])
            new_msg["options"] = merge_dicts([v["options"] for v in msg_list])
            return new_msg

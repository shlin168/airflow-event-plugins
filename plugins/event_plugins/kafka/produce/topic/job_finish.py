from event_plugins.kafka.produce.topic.base import Base


class JobFinish(Base):
    '''
        match_method = [{
            'job_name': 'jb0',
            'is_success': True
        }]
    '''

    def __init__(self, match_method):
        super(JobFinish, self).__init__(match_method)

    def merge_messages(self, msg_list):
        if len(msg_list) > 0:
            new_msg = msg_list[0]
            new_msg["finish_time"] = max([v["finish_time"] for v in msg_list])
            new_msg["duration_time"] = sum([v["duration_time"] for v in msg_list])
            new_msg["job_name"] = "+".join([v["job_name"] for v in msg_list])
            return new_msg

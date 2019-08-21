from event_plugins.kafka.produce.topic.base import Base


class HippoFinish(Base):
    '''
        match_method = [{
            'hippo_name': 'hn0',
            'is_success': True
        }]
    '''

    def __init__(self, match_method):
        super(HippoFinish, self).__init__(match_method)

    def merge_messages(self, msg_list):
        if len(msg_list) > 0:
            new_msg = msg_list[0]
            new_msg["finish_time"] = max([v["finish_time"] for v in msg_list])
            new_msg["duration_time"] = sum([v["duration_time"] for v in msg_list])
            new_msg["job_id"] = "{}_{}".format(new_msg["hippo_name"], new_msg["finish_time"])
            new_msg["job_name"] = new_msg["hippo_name"]
            return new_msg

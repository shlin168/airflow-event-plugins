

class Base(object):
    '''
        match_method = [{
            key1: value1,
            key2: value2
        }]
    '''

    def __init__(self, match_method):
        self.match_method = match_method

    def match(self, item):
        '''
            return True if (key: value) in match_methods match the (key:value) in item
        '''
        return all([item[k] == v for k, v in self.match_method.iteritems()])

    def merge_messages(self, msg_list):
        '''
            Implement how to merge multiple messages
        '''
        if len(msg_list) > 0:
            return msg_list[0]

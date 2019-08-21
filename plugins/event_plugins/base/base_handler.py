# -*- coding: UTF-8 -*-

from event_plugins.base.base_connector import BaseConnector


class BaseHandler(object):

    valid_mtypes = ['wanted', 'receive']

    def __init__(self, name):
        self.name = name

    def all_msgs_handler(self, wanted_msgs):
        return BaseAllMessageHandler(wanted_msgs)

    def msg_handler(self, msg, mtype):
        if mtype == 'wanted':
            return BaseSingleMessageHandler().set_wanted_msg(msg)
        elif mtype == 'receive':
            return BaseSingleMessageHandler().set_receive_msg(msg)
        else:
            raise ValueError('Avaliable mtype:', self.valid_mtypes)

    def conn_handler(self):
        return BaseConnector


class BaseAllMessageHandler(object):

    def __init__(self, wanted_msgs):
        self.wanted_msgs = wanted_msgs

    def get_wanted_msgs(self):
        ''' Get wanted msgs, override if some preprocessing such as rendering is needed '''
        return self.wanted_msgs

    def get_task_ids(self):
        raise NotImplementedError("""
            implement how to get task ids from wanted messages,
            if would be invoked to skip unexecuted tasks when soft_fail=True
        """)

    def match(self, receive_msg, receive_dt):
        raise NotImplementedError("""
            implement how to check if receive message match any message in wanted messages,
            if match, return that 'match' wanted message and received message in pair
            else None
        """)


class BaseSingleMessageHandler(object):
    ''' Handle single msg (json format)
        - WantedMessage is used to handle wanted message given by user
        - ReceiveMessage is used to handle received message from source
    '''
    class WantedMessage:

        def __init__(self, msg):
            self.msg = msg

        def timeout(self):
            raise NotImplementedError("""
                implement how to get timeout of single message,
                it is invoked by storage/shelve_db.py through factory to set message timeout
            """)

    class ReceiveMessage:

        def __init__(self, msg):
            self.msg = msg

        def value(self):
            '''
                Print out what is received in base_plugin, different source might have
                different way to get the value, override if needed
            '''
            return self.msg

    def set_wanted_msg(self, msg):
        return self.WantedMessage(msg)

    def set_receive_msg(self, msg):
        return self.ReceiveMessage(msg)

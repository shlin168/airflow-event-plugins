# -*- coding: UTF-8 -*-

import json
import os
import pytest

from event_plugins.common.schedule.time_utils import TimeUtils
from event_plugins import factory
from event_plugins.kafka.kafka_consumer_plugin import KafkaConsumerOperator
from event_plugins.kafka.kafka_handler import KafkaHandler
from event_plugins.kafka.kafka_connector import KafkaConnector


def patch_now(mocker, now):
    mocker.patch.object(TimeUtils, 'get_now', return_value=now)

def remove_db(db):
    def get_exist_db(db):
        # shelve db might have different extension depends on 'anydbm' module
        extension_choices = ['', '.db', '.dir', '.pag']
        for ext in extension_choices:
            if os.path.exists(db + ext):
                return db + ext
        return None

    exist_db = get_exist_db(db)
    if exist_db:
        os.remove(exist_db)


class FakeKafkaMsg:
    def __init__(self, topic, value):
        self.t = topic
        self.v = value

    def value(self):
        return self.v

    def topic(self):
        return self.t


def TestMsg(name, dt):
    ts = int((dt - TimeUtils().datetime(1970, 1, 1)).total_seconds())
    if name == 'a':
        msg = FakeKafkaMsg(
                topic='frontier-adw',
                value={'db': 'db0', 'table': 'table0', 'partition_values': ['']})
        msg.v.update({'exec_date': ts})
    elif name == 'b':
        msg = FakeKafkaMsg(
                topic='frontier-adw',
                value={'db': 'db1', 'table': 'table1', 'partition_values': ['201907']})
        msg.v.update({'exec_date': ts})
    elif name == 'c':
        msg = FakeKafkaMsg(
                topic='hippo-finish',
                value={'hippo_name': 'hn0', 'job_name': 'jn0', 'is_success': True})
        msg.v.update({'finish_time': ts})
    else:
        raise ValueError('not defined message type')
    msg.v = json.dumps(msg.v)
    return msg


class TestKafkaConsumerOperator:
    '''
        Test the logic of kafka_consumer_plugin. Check if the result of poke function
        which receive and match the messages (update shelve db) works as desired.
        It also test the matching mechanism for different kafka topics.

        Note:
            1. Both frontier-adw and hippo-finish have 2 hours offset, which means that
               message received after 22:00 is regard as next-day message. The timeout
               would be set to 21:59:59
            2. Task time out and reschedule in execute() is not included here.
    '''

    def test_poke_all_D_messages(self, mocker):
        ######################
        #  prepare for test  #
        ######################
        # tmp file for status db
        status_fpath = os.path.join(os.environ.get('SERVICE_HOME'), 'test_plugins/sensor')
        remove_db(status_fpath)

        # define wanted messages
        a = {'frequency': 'D', 'topic': 'frontier-adw', 'db': 'db0', 'table': 'table0',
                'partition_values': "", 'task_id': "tbla"}
        b = {'frequency': 'D', 'topic': 'frontier-adw', 'db': 'db1', 'table': 'table1',
                'partition_values': "{{yyyymm|dt.format(format='%Y%m')}}", 'task_id': "tblb"}
        c = {'frequency': 'D', 'topic': 'hippo-finish', 'hippo_name':'hn0',
                'job_name': 'jn0', 'is_success': True, 'task_id': "tblc"}
        wanted_msgs = [a, b, c]

        # initialize operator
        operator = KafkaConsumerOperator(
            task_id='test',
            broker=None,
            group_id='test',
            client_id='test',
            msgs=wanted_msgs,
            poke_interval=2,
            timeout=10,
            mark_success=False,
            status_file=status_fpath,
            debug_mode=True
        )
        # consumer with set_consumer being patch to return None
        consumer = KafkaConnector(broker=None)

        ###############################
        #  [Time Changed] 2019/07/07  #
        ###############################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 7, 8, 0, 0))
        fake_now = TimeUtils.get_now()
        assert fake_now == TimeUtils().datetime(2019, 7, 7, 8, 0, 0)

        # Initializing connection is set in execute function,
        # but need to invoke here for poke to work
        operator.initialize_conn_handler()

        ####################################################
        #  situation: received a and c (NOT_ALL_RECEIVED)  #
        ####################################################
        mocker.patch.object(KafkaConnector, 'set_consumer', return_value=None)
        mocker.patch.object(KafkaConnector, 'get_messages', return_value=[TestMsg('a', fake_now), TestMsg('c', fake_now)])

        # context can be None if mark_success=False
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == False
        assert len(operator.status_db.get_unreceived_msgs()) == 1

        ##########################################
        #  situation: received b (ALL_RECEIVED)  #
        ##########################################
        mocker.patch.object(KafkaConnector, 'get_messages', return_value=[TestMsg('b', fake_now)])
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == True

        #####################################
        #  [Time Changed] 2019/07/07 22:15  #
        #####################################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 7, 22, 15, 0))
        fake_now = TimeUtils().get_now()
        assert fake_now == TimeUtils().datetime(2019, 7, 7, 22, 15, 0)

        ##############################################
        #  situation: received c (NOT_ALL_RECEIVED)  #
        ##############################################
        mocker.patch.object(KafkaConnector, 'get_messages', return_value=[TestMsg('c', fake_now)])
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        # clear the messages in status db from 2019/7/7 since it's regard as 2019/7/8 after 22:00
        # timeout for all messages should be set to 2019/07/08 21:59:59
        assert operator.status_db.get(json.dumps(a, sort_keys=True))['timeout'] == TimeUtils().datetime(2019, 7, 8, 21, 59, 59)
        assert is_criteria_met == False
        assert len(operator.status_db.get_unreceived_msgs()) == 2

        ################################################
        #  situation: received a and b (ALL_RECEIVED)  #
        ################################################
        mocker.patch.object(KafkaConnector, 'get_messages', return_value=[TestMsg('a', fake_now), TestMsg('b', fake_now)])
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == True


    def test_poke_D_M_messages(self, mocker):
        ######################
        #  prepare for test  #
        ######################
        # tmp file for status db
        status_fpath = os.path.join(os.environ.get('SERVICE_HOME'), 'test_plugins/sensor')
        remove_db(status_fpath)

        # define wanted messages
        a = {'frequency': 'D', 'topic': 'frontier-adw', 'db': 'db0', 'table': 'table0',
                'partition_values': "", 'task_id': "tbla"}
        b = {'frequency': 'M', 'topic': 'frontier-adw', 'db': 'db1', 'table': 'table1',
                'partition_values': "{{yyyymm|dt.format(format='%Y%m')}}", 'task_id': "tblb"}
        c = {'frequency': 'M', 'topic': 'hippo-finish', 'hippo_name':'hn0',
                'job_name': 'jn0', 'is_success': True, 'task_id': "tblc"}
        wanted_msgs = [a, b, c]

        # initialize operator
        operator = KafkaConsumerOperator(
            task_id='test',
            broker=None,
            group_id='test',
            client_id='test',
            msgs=wanted_msgs,
            poke_interval=2,
            timeout=10,
            mark_success=False,
            status_file=status_fpath,
            debug_mode=True
        )

        # consumer with set_consumer being patch to return None
        consumer = KafkaConnector(broker=None)

        ###############################
        #  [Time Changed] 2019/07/07  #
        ###############################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 7, 8, 0, 0))
        fake_now = TimeUtils().get_now()
        assert fake_now == TimeUtils().datetime(2019, 7, 7, 8, 0, 0)

        ####################################################
        #  situation: received a and c (NOT_ALL_RECEIVED)  #
        ####################################################
        mocker.patch.object(KafkaConnector, 'get_messages', return_value=[TestMsg('a', fake_now), TestMsg('c', fake_now)])

        # Initializing connection is set in execute function,
        # but need to invoke here for poke to work
        operator.initialize_conn_handler()

        # context can be None if mark_success=False
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == False
        assert len(operator.status_db.get_unreceived_msgs()) == 1

        #####################################
        #  [Time Changed] 2019/07/07 22:15  #
        #####################################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 7, 22, 15, 0))
        fake_now = TimeUtils().get_now()
        assert fake_now == TimeUtils().datetime(2019, 7, 7, 22, 15, 0)

        ##############################################
        #  situation: received b (NOT_ALL_RECEIVED)  #
        ##############################################
        mocker.patch.object(KafkaConnector, 'get_messages', return_value=[TestMsg('b', fake_now)])
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        # clear last_receive of a(D)
        # c(M) is monthly message so the last_receive won't be cleared
        rb = factory.plugin_factory('kafka').msg_handler(b, mtype='wanted').render()
        assert operator.status_db.get(json.dumps(a, sort_keys=True))['timeout'] == TimeUtils().datetime(2019, 7, 8, 21, 59, 59)
        assert operator.status_db.get(json.dumps(rb, sort_keys=True))['timeout'] == TimeUtils().datetime(2019, 7, 31, 21, 59, 59)
        assert is_criteria_met == False
        assert len(operator.status_db.get_unreceived_msgs()) == 1

        #####################################
        #  [Time Changed] 2019/07/08 22:15  #
        #####################################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 8, 22, 15, 0))
        fake_now = TimeUtils().get_now()
        assert fake_now == TimeUtils().datetime(2019, 7, 8, 22, 15, 0)

        ##############################################
        #  situation: received a (ALL_RECEIVED)  #
        ##############################################
        mocker.patch.object(KafkaConnector, 'get_messages', return_value=[TestMsg('a', fake_now)])
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        # clear last_receive of a(D)
        # b(M) and c(M) are monthly message so the last_receive won't be cleared
        assert operator.status_db.get(json.dumps(a, sort_keys=True))['timeout'] == TimeUtils().datetime(2019, 7, 9, 21, 59, 59)
        assert operator.status_db.get(json.dumps(c, sort_keys=True))['timeout'] == TimeUtils().datetime(2019, 7, 31, 21, 59, 59)
        assert is_criteria_met == True

        #####################################
        #  [Time Changed] 2019/07/31 22:15  #
        #####################################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 31, 22, 15, 0))
        fake_now = TimeUtils().get_now()
        assert fake_now == TimeUtils().datetime(2019, 7, 31, 22, 15, 0)

        mocker.patch.object(KafkaConnector, 'get_messages', return_value=[TestMsg('a', fake_now)])
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == False
        assert len(operator.status_db.get_unreceived_msgs()) == 2

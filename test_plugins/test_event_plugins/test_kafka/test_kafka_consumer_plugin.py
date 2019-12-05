# -*- coding: UTF-8 -*-
import json
import os
import pytest

from event_plugins import factory
from event_plugins.common.schedule.time_utils import TimeUtils, AIRFLOW_EVENT_PLUGINS_TIMEZONE
from event_plugins.common.storage.db import get_session
from event_plugins.common.storage.event_message import EventMessage
from event_plugins.kafka.kafka_consumer_plugin import KafkaConsumerOperator
from event_plugins.kafka.kafka_handler import KafkaHandler
from event_plugins.kafka.kafka_connector import KafkaConnector


def patch_now(mocker, now):
    mocker.patch.object(TimeUtils, 'get_now', return_value=now)


class FakeKafkaMsg:
    def __init__(self, topic, value):
        self.t = topic
        self.v = value

    def value(self):
        return self.v

    def topic(self):
        return self.t


def TestMsg(name, dt):
    ts = int((dt - TimeUtils().datetime(1970, 1, 1, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE)).total_seconds())
    if name == 'a':
        msg = FakeKafkaMsg(
                topic='etl-finish',
                value={'db': 'db0', 'table': 'table0', 'partition_values': ''})
        msg.v.update({'timestamp': ts})
    elif name == 'b':
        msg = FakeKafkaMsg(
                topic='etl-finish',
                value={'db': 'db1', 'table': 'table1', 'partition_values': '201907'})
        msg.v.update({'timestamp': ts})
    elif name == 'c':
        msg = FakeKafkaMsg(
                topic='job-finish',
                value={'job_name': 'jn0', 'is_success': True})
        msg.v.update({'timestamp': ts})
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
            1. if setting offset_seconds for specific topic. e.g., offset_seconds = 7200,
               which means that message received after 22:00 is regard as next-day message.
               Then timeout of messages would be set to 21:59:59
            2. Task time out and reschedule in execute() is not included here.
    '''
    def teardown_method(self, method):
        session = get_session()
        session.query(EventMessage).delete()
        session.commit()
        session.close()

    def test_poke_all_D_messages(self, mocker):
        ######################
        #  prepare for test  #
        ######################
        # define wanted messages
        a = {'frequency': 'D', 'topic': 'etl-finish', 'db': 'db0', 'table': 'table0',
                'partition_values': "", 'task_id': "tbla"}
        b = {'frequency': 'D', 'topic': 'etl-finish', 'db': 'db1', 'table': 'table1',
                'partition_values': "{{yyyymm|dt.format(format='%Y%m')}}", 'task_id': "tblb"}
        c = {'frequency': 'D', 'topic': 'job-finish', 'job_name': 'jn0', 'is_success': True,
                'task_id': "tblc"}
        wanted_msgs = [a, b, c]

        # initialize operator
        operator = KafkaConsumerOperator(
            task_id='test',
            broker=None,
            sensor_name="test",
            group_id='test',
            client_id='test',
            msgs=wanted_msgs,
            poke_interval=2,
            timeout=10,
            mark_success=False,
            debug_mode=True
        )
        # consumer with set_consumer being patch to return None
        consumer = KafkaConnector(broker=None)

        ###############################
        #  [Time Changed] 2019/07/07  #
        ###############################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 7, 8, 0, 0, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE))
        fake_now = TimeUtils().get_now()
        assert fake_now == TimeUtils().datetime(2019, 7, 7, 8, 0, 0, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE)

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
        assert len(operator.db_handler.get_unreceived_msgs()) == 1

        # get id of message
        str_a = json.dumps(a, sort_keys=True)
        rb = factory.plugin_factory('kafka').msg_handler(b, mtype='wanted').render()
        str_rb = json.dumps(rb, sort_keys=True)
        str_c = json.dumps(c, sort_keys=True)

        a_id, b_id, c_id = None, None, None
        for record in operator.db_handler.get_sensor_messages():
            if record.msg == str_a:
                a_id = record.id
            elif record.msg == str_rb:
                b_id = record.id
            elif record.msg == str_c:
                c_id = record.id

        ##########################################
        #  situation: received b (ALL_RECEIVED)  #
        ##########################################
        mocker.patch.object(KafkaConnector, 'get_messages', return_value=[TestMsg('b', fake_now)])
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == True

        #####################################
        #  [Time Changed] 2019/07/08 00:15  #
        #####################################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 8, 0, 15, 0, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE))
        fake_now = TimeUtils().get_now()
        assert fake_now == TimeUtils().datetime(2019, 7, 8, 0, 15, 0, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE)

        ##############################################
        #  situation: received c (NOT_ALL_RECEIVED)  #
        ##############################################
        mocker.patch.object(KafkaConnector, 'get_messages', return_value=[TestMsg('c', fake_now)])
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        # clear the messages in status db from 2019/7/7 since it's 2019/7/8
        # timeout for all messages should be set to 2019/07/08 23:59:59
        msgs = operator.db_handler.get_sensor_messages()
        record_a = msgs.filter(EventMessage.id == a_id).first()
        assert record_a.timeout == TimeUtils().datetime(2019, 7, 8, 23, 59, 59, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE)
        assert is_criteria_met == False
        assert len(operator.db_handler.get_unreceived_msgs()) == 2

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
        # define wanted messages
        a = {'frequency': 'D', 'topic': 'etl-finish', 'db': 'db0', 'table': 'table0',
                'partition_values': "", 'task_id': "tbla"}
        b = {'frequency': 'M', 'topic': 'etl-finish', 'db': 'db1', 'table': 'table1',
                'partition_values': "{{yyyymm|dt.format(format='%Y%m')}}", 'task_id': "tblb"}
        c = {'frequency': 'M', 'topic': 'job-finish', 'job_name': 'jn0', 'is_success': True,
                'task_id': "tblc"}
        wanted_msgs = [a, b, c]

        # initialize operator
        operator = KafkaConsumerOperator(
            task_id='test',
            broker=None,
            sensor_name="test",
            group_id='test',
            client_id='test',
            msgs=wanted_msgs,
            poke_interval=2,
            timeout=10,
            mark_success=False,
            debug_mode=True
        )

        # consumer with set_consumer being patch to return None
        consumer = KafkaConnector(broker=None)

        ###############################
        #  [Time Changed] 2019/07/07  #
        ###############################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 7, 8, 0, 0, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE))
        fake_now = TimeUtils().get_now()
        assert fake_now == TimeUtils().datetime(2019, 7, 7, 8, 0, 0, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE)

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
        assert len(operator.db_handler.get_unreceived_msgs()) == 1

        # get id of message
        str_a = json.dumps(a, sort_keys=True)
        rb = factory.plugin_factory('kafka').msg_handler(b, mtype='wanted').render()
        str_rb = json.dumps(rb, sort_keys=True)
        str_c = json.dumps(c, sort_keys=True)

        a_id, b_id, c_id = None, None, None
        for record in operator.db_handler.get_sensor_messages():
            if record.msg == str_a:
                a_id = record.id
            elif record.msg == str_rb:
                b_id = record.id
            elif record.msg == str_c:
                c_id = record.id

        #####################################
        #  [Time Changed] 2019/07/08 00:15  #
        #####################################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 8, 0, 15, 0, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE))
        fake_now = TimeUtils().get_now()
        assert fake_now == TimeUtils().datetime(2019, 7, 8, 0, 15, 0, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE)

        ##############################################
        #  situation: received b (NOT_ALL_RECEIVED)  #
        ##############################################
        mocker.patch.object(KafkaConnector, 'get_messages', return_value=[TestMsg('b', fake_now)])
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        # clear last_receive of a(D)
        # c(M) is monthly message so the last_receive won't be cleared
        msgs = operator.db_handler.get_sensor_messages()
        record_a = msgs.filter(EventMessage.id == a_id).first()
        record_b = msgs.filter(EventMessage.id == b_id).first()
        assert record_a.timeout == TimeUtils().datetime(2019, 7, 8, 23, 59, 59, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE)
        assert record_b.timeout== TimeUtils().datetime(2019, 7, 31, 23, 59, 59, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE)
        assert is_criteria_met == False
        assert len(operator.db_handler.get_unreceived_msgs()) == 1

        #####################################
        #  [Time Changed] 2019/07/09 00:15  #
        #####################################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 9, 0, 15, 0, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE))
        fake_now = TimeUtils().get_now()
        assert fake_now == TimeUtils().datetime(2019, 7, 9, 0, 15, 0, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE)

        ##############################################
        #  situation: received a (ALL_RECEIVED)  #
        ##############################################
        mocker.patch.object(KafkaConnector, 'get_messages', return_value=[TestMsg('a', fake_now)])
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        # clear last_receive of a(D)
        # b(M) and c(M) are monthly message so the last_receive won't be cleared
        msgs = operator.db_handler.get_sensor_messages()
        record_a = msgs.filter(EventMessage.id == a_id).first()
        record_c = msgs.filter(EventMessage.id == c_id).first()
        assert record_a.timeout == TimeUtils().datetime(2019, 7, 9, 23, 59, 59, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE)
        assert record_c.timeout == TimeUtils().datetime(2019, 7, 31, 23, 59, 59, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE)
        assert is_criteria_met == True

        #####################################
        #  [Time Changed] 2019/08/01 00:15  #
        #####################################
        patch_now(mocker, TimeUtils().datetime(2019, 8, 1, 0, 15, 0, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE))
        fake_now = TimeUtils().get_now()
        assert fake_now == TimeUtils().datetime(2019, 8, 1, 0, 15, 0, tzinfo=AIRFLOW_EVENT_PLUGINS_TIMEZONE)

        mocker.patch.object(KafkaConnector, 'get_messages', return_value=[TestMsg('a', fake_now)])
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == False
        assert len(operator.db_handler.get_unreceived_msgs()) == 2

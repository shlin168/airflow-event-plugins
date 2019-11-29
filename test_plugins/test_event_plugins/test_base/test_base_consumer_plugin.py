# -*- coding: UTF-8 -*-
import os
import pytest

from event_plugins.common.schedule.time_utils import TimeUtils
from event_plugins.common.storage.db import get_session, STORAGE_CONF

from test_event_plugins.test_base.mocks_base_consumer_plugin import MockBaseConsumerOperator
from test_event_plugins.test_base.mocks_base_consumer_plugin import MockBaseHandler
from test_event_plugins.test_base.mocks_base_consumer_plugin import MockBaseConnector


TEST_TABLE_NAME = STORAGE_CONF.get("Storage", "table_name")

def patch_now(mocker, now):
    mocker.patch.object(TimeUtils, 'get_now', return_value=now)


class TestBaseConsumerOperator:
    '''
        Test the logic of base_consumer_plugin. Check if the result of poke function
        which receive and match the messages (update shelve db) works as desired.

        Note:
            1. Most of base class are interace, we implemented mocks class in mocks_base_plugin.py
            2. Task time out and reschedule in execute() is not included here.
    '''
    def test_poke_all_D_messages(self, mocker):
        ######################
        #  prepare for test  #
        ######################
        # define wanted_msgs
        wanted_msgs = [
            {'task_id': 'taskA', 'frequency': 'D'},
            {'task_id': 'taskB', 'frequency': 'D'},
            {'task_id': 'taskC', 'frequency': 'D'}
        ]

        # initial operator
        operator = MockBaseConsumerOperator(
            task_id='test',
            source_type='base',
            sensor_name="test",
            msgs=wanted_msgs,
            poke_interval=2,
            timeout=10,
            mark_success=False,
            debug_mode=True,
        )
        # patch factory since 'test' is not one of official event plugins
        mocker.patch('event_plugins.factory.plugin_factory', return_value=MockBaseHandler('test'))

        # get mock consumer
        consumer = MockBaseConnector()

        ###############################
        #  [Time Changed] 2019/07/07  #
        ###############################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 7, 8, 0, 0))
        assert TimeUtils().get_now() == TimeUtils().datetime(2019, 7, 7, 8, 0, 0)

        # Initializing connection is set in execute function,
        # but need to invoke here for poke to work
        operator.initialize_conn_handler()

        ############################################################
        #  situation: received taskA and taskB (NOT_ALL_RECEIVED)  #
        ############################################################
        mocker.patch.object(MockBaseConnector, 'get_messages', return_value=['taskA', 'taskB'])

        # param context can be None if mark_success=False
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == False
        assert len(operator.db_handler.get_unreceived_msgs()) == 1

        ##############################################
        #  situation: received taskC (ALL_RECEIVED)  #
        ##############################################
        mocker.patch.object(MockBaseConnector, 'get_messages', return_value=['taskC'])
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        print operator.db_handler.tabulate_data()
        assert is_criteria_met == True

        ###############################
        #  [Time Changed] 2019/07/08  #
        ###############################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 8, 8, 0, 0))
        assert TimeUtils().get_now() == TimeUtils().datetime(2019, 7, 8, 8, 0, 0)

        ##################################################
        #  situation: received taskC (NOT_ALL_RECEIVED)  #
        ##################################################
        mocker.patch.object(MockBaseConnector, 'get_messages', return_value=['taskC'])
        # clear the messages in status db from 2019/7/7 since it's 2019/7/8
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == False
        assert len(operator.db_handler.get_unreceived_msgs()) == 2

        #####################################################
        #  situation: received taskA, taskB (ALL_RECEIVED)  #
        #####################################################
        mocker.patch.object(MockBaseConnector, 'get_messages', return_value=['taskA', 'taskB'])
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == True

    def test_poke_D_M_messages(self, mocker):
        ######################
        #  prepare for test  #
        ######################
        # define wanted messages
        wanted_msgs = [
            {'task_id': 'taskA', 'frequency': 'D'},
            {'task_id': 'taskB', 'frequency': 'D'},
            {'task_id': 'taskC', 'frequency': 'M'},
            {'task_id': 'taskD', 'frequency': 'M'}
        ]
        # initialize operator
        operator = MockBaseConsumerOperator(
            task_id='test',
            source_type='base',
            sensor_name="test",
            msgs=wanted_msgs,
            poke_interval=2,
            timeout=10,
            mark_success=False,
            debug_mode=True,
        )
        # patch factory since 'test' is not one of official event plugins
        mocker.patch('event_plugins.factory.plugin_factory', return_value=MockBaseHandler('test'))

        # get mock consumer
        consumer = MockBaseConnector()

        ###############################
        #  [Time Changed] 2019/07/07  #
        ###############################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 7, 8, 0, 0))
        assert TimeUtils().get_now() == TimeUtils().datetime(2019, 7, 7, 8, 0, 0)

        # Initializing connection is set in execute function,
        # but need to invoke here for poke to work
        operator.initialize_conn_handler()

        ############################################################################
        #  situation: received taskA(D), taskB(D) and taskC(M) (NOT_ALL_RECEIVED)  #
        ############################################################################
        mocker.patch.object(MockBaseConnector, 'get_messages', return_value=['taskA', 'taskB', 'taskC'])

        # context can be None if mark_success=False
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == False
        assert len(operator.db_handler.get_unreceived_msgs()) == 1

        ###############################
        #  [Time Changed] 2019/07/08  #
        ###############################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 8, 8, 0, 0))
        assert TimeUtils().get_now() == TimeUtils().datetime(2019, 7, 8, 8, 0, 0)

        #####################################################
        #  situation: received taskD(M) (NOT_ALL_RECEIVED)  #
        #####################################################
        mocker.patch.object(MockBaseConnector, 'get_messages', return_value=['taskD'])
        # clear last_receive of taskA(D) and task(B)
        # taskC(M) is monthly message so the last_receive won't be cleared
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == False
        assert len(operator.db_handler.get_unreceived_msgs()) == 2

        ###############################
        #  [Time Changed] 2019/07/09  #
        ###############################
        patch_now(mocker, TimeUtils().datetime(2019, 7, 9, 8, 0, 0))
        assert TimeUtils().get_now() == TimeUtils().datetime(2019, 7, 9, 8, 0, 0)

        ###########################################################
        #  situation: received taskA(D), taskB(D) (ALL_RECEIVED)  #
        ###########################################################
        mocker.patch.object(MockBaseConnector, 'get_messages', return_value=['taskA', 'taskB'])
        # clear last_receive of taskA(D) and task(B)
        # taskC(M) and taskD(M) are monthly messages so the last_receive won't be cleared
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == True

        ###############################
        #  [Time Changed] 2019/08/01  #
        ###############################
        patch_now(mocker, TimeUtils().datetime(2019, 8, 1, 8, 0, 0))
        assert TimeUtils().get_now() == TimeUtils().datetime(2019, 8, 1, 8, 0, 0)

        ###############################################################
        #  situation: received taskA(D), taskB(D) (NOT_ALL_RECEIVED)  #
        ###############################################################
        mocker.patch.object(MockBaseConnector, 'get_messages', return_value=['taskA', 'taskB'])
        # clean all messages since's it both new day and new month
        is_criteria_met = operator.poke(context=None, consumer=consumer)
        assert is_criteria_met == False
        assert len(operator.db_handler.get_unreceived_msgs()) == 2

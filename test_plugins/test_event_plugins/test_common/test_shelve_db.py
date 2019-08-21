import os
import shelve
import pytest
import mock

from event_plugins.common.schedule.time_utils import TimeUtils
from event_plugins.common.storage.shelve_db import MessageRecordCRUD
from event_plugins.common.storage.shelve_db import MessageRecord
from event_plugins.common.storage.shelve_db import DBStatus


def patch_now(mocker, now):
    mocker.patch.object(TimeUtils, 'get_now', return_value=now)

@pytest.fixture()
def shelve_name():
    return 'test'

def remove_db(db):
    if os.path.exists(db + '.db'):
        os.remove(db + '.db')

@pytest.fixture()
def db(shelve_name):
    # use kafka_plugin for testing
    return MessageRecordCRUD(shelve_name, 'kafka')

@pytest.fixture()
def msg_list():
    return [
        {'frequency': 'D', 'topic': 'frontier-adw', 'db': 'badw', 'table': 'chp_event_cust',
            'partition_values': "{{yyyymm|dt.format(format='%Y%m')}}", 'task_id': "tbla"},
        {'frequency': 'M', 'topic': 'frontier-adw', 'db': 'badw', 'table': 'chp_test',
            'partition_values': "", 'task_id': "tblc"}
    ]

@pytest.fixture()
def overwrite_msg_list():
    return [
        {'frequency': 'D', 'topic': 'frontier-adw', 'db': 'badw', 'table': 'chp_event_cust',
            'partition_values': "{{yyyymm|dt.format(format='%Y%m')}}", 'task_id': "tbla"},
        {'frequency': 'M', 'topic': 'frontier-adw', 'db': 'badw', 'table': 'chp_event_cti',
            'partition_values': "", 'task_id': "tblb"},
        {'frequency': 'D', 'topic': 'hippo-finish', 'hippo_name': 'hn1',
            'job_name': 'jn1', 'task_id': "job1"},
    ]


class TestMessageRecordCRUD:

    @mock.patch('event_plugins.common.storage.shelve_db.open_shelve')
    @pytest.mark.usefixtures("db", "msg_list")
    def test_initialize_insert(self, open_shelve, db, msg_list):
        '''
            Check if successfully insert three rows in msg_list while db is not exist,
            and if timeout of all message is set
        '''
        remove_db(db.shelve_name)

        f = shelve.open(db.shelve_name, flag='c', writeback=True)
        open_shelve.return_value.__enter__.return_value = f
        db.initialize(msg_list)
        timeouts = [row['timeout'] for row in db.list_all()]
        open_shelve.return_value.__exit__.return_value = f.close()
        assert len(timeouts) == 2
        assert all(t is not None for t in timeouts)


    @mock.patch('event_plugins.common.storage.shelve_db.open_shelve')
    @pytest.mark.usefixtures("db", "msg_list", "overwrite_msg_list")
    def test_initialize_update(self, open_shelve, db, msg_list, overwrite_msg_list):
        '''
            Check if successfully overwrite three rows in msg_list while db exists
            Note: overwrite_msg_list is different from msg_list,
                check if function remove unused message records and add new message records
        '''
        f = shelve.open(db.shelve_name, flag='c', writeback=True)
        open_shelve.return_value.__enter__.return_value = f
        db.initialize(msg_list)
        db.initialize(overwrite_msg_list)
        keys = db.list_keys()
        open_shelve.return_value.__exit__.return_value = f.close()
        assert len(keys) == 3


    @pytest.mark.usefixtures("db", "msg_list")
    def test_get_timeout(self, db, msg_list, mocker):
        patch_now(mocker, TimeUtils().datetime(2019, 6, 5, 8, 0, 0))
        for msg in msg_list:
            # since topic of message is frontier-adw, timeout should minus 2 hours
            timeout = db.get_timeout(msg)
            if msg['frequency'] == 'D':
                assert timeout == TimeUtils().datetime(2019, 6, 5, 21, 59, 59)
            elif msg['frequency'] == 'M':
                assert timeout == TimeUtils().datetime(2019, 6, 30, 21, 59, 59)


    @mock.patch('event_plugins.common.storage.shelve_db.open_shelve')
    @pytest.mark.usefixtures("db")
    def test_check_and_reset_timeout(self, open_shelve, db, mocker):
        '''
            Check if last_receive and last_receive_time are assigned to None
            when time > message's timeout time
        '''
        # clean db for inserting testing records
        remove_db(db.shelve_name)

        f = shelve.open(db.shelve_name, flag='c', writeback=True)
        open_shelve.return_value.__enter__.return_value = f

        # mock TimeUtils().get_now()
        patch_now(mocker, TimeUtils().datetime(2019, 6, 16, 0, 0, 0))
        fake_now = TimeUtils().datetime(2019, 6, 16, 0, 0, 0)

        # this first record is timeout since fake_now > timeout
        msg1 = '{"test_memo":"timeout", "frequency":"D", "topic":"frontier-adw"}'
        db.insert(
            MessageRecord(
                key=msg1,
                frequency='D',
                last_receive={'test':1},
                last_receive_time=TimeUtils().datetime(2019, 6, 15),
                timeout=TimeUtils().datetime(2019, 6, 15, 21, 59, 59)
            )
        )
        # this second record is not timeout since fake_now < timeout
        msg2 = '{"test_memo":"not_timeout", "frequency":"M", "topic":"frontier-adw"}'
        db.insert(
            MessageRecord(
                key=msg2,
                frequency='M',
                last_receive={'test':2},
                last_receive_time=TimeUtils().datetime(2019, 6, 13),
                timeout=TimeUtils().datetime(2019, 6, 30, 21, 59, 59)
            )
        )
        # the first record should be effected (clean last_receive and last_receive_time)
        db.check_and_reset_timeout()
        assert (
            db.get(msg1)['last_receive'] is None and
            db.get(msg1)['last_receive_time'] is None and
            db.get(msg1)['timeout'] == TimeUtils().datetime(2019, 6, 16, 21, 59, 59)
        ) and (
            db.get(msg2)['last_receive'] is not None and
            db.get(msg2)['last_receive_time'] is not None
        )
        open_shelve.return_value.__exit__.return_value = f.close()


    @mock.patch('event_plugins.common.storage.shelve_db.open_shelve')
    @pytest.mark.usefixtures("db")
    def test_status(self, open_shelve, db):
        # clean db for inserting testing records
        remove_db(db.shelve_name)

        f = shelve.open(db.shelve_name, flag='c', writeback=True)
        open_shelve.return_value.__enter__.return_value = f

        # insert records - emulate second message not receiving yet
        msg1 = '{"test_memo":"received", "frequency":"D", "topic":"frontier-adw"}'
        db.insert(
            MessageRecord(
                key=msg1,
                frequency='D',
                last_receive={'test':1},
                last_receive_time=TimeUtils().datetime(2019, 6, 15),
                timeout=TimeUtils().datetime(2019, 6, 15, 23, 59, 59)
            )
        )
        msg2 = '{"test_memo":"not_received", "frequency":"M", "topic":"frontier-adw"}'
        db.insert(
            MessageRecord(
                key=msg2,
                frequency='M',
                last_receive=None,
                last_receive_time=None,
                timeout=TimeUtils().datetime(2019, 6, 30, 23, 59, 59)
            )
        )
        assert db.status() == DBStatus.NOT_ALL_RECEIVED

        # last_receive_time and last_receive should be both NOT NONE,
        # or the status would be still NOT_ALL_RECEIVED
        db.update(msg2, {'last_receive_time': TimeUtils().datetime(2019, 6, 13)})
        assert db.status() == DBStatus.NOT_ALL_RECEIVED

        # change second record to emulate message received
        db.update(msg2, {'last_receive': {'test':1}})
        assert db.status() == DBStatus.ALL_RECEIVED

        # if receive zero value as message, it is RECEIVED when checking status
        # TODO this might change since only json format is valid message so far
        db.update(msg2, {'last_receive': 0})
        assert db.status() == DBStatus.ALL_RECEIVED

        open_shelve.return_value.__exit__.return_value = f.close()


    @mock.patch('event_plugins.common.storage.shelve_db.open_shelve')
    @pytest.mark.usefixtures("db")
    def test_get_unreceived_msgs(self, open_shelve, db):
        # clean db for inserting testing records
        remove_db(db.shelve_name)

        f = shelve.open(db.shelve_name, flag='c', writeback=True)
        open_shelve.return_value.__enter__.return_value = f

        # insert records - emulate second message not receiving yet
        db.insert(
            MessageRecord(
                key='{"test_memo":"have_receive"}',
                frequency='D',
                last_receive={'test':1},
                last_receive_time=TimeUtils().datetime(2019, 6, 15),
                timeout=TimeUtils().datetime(2019, 6, 15, 23, 59, 59)
            )
        )
        db.insert(
            MessageRecord(
                key='{"test_memo":"not_receive"}',
                frequency='M',
                last_receive=None,
                last_receive_time=None,
                timeout=TimeUtils().datetime(2019, 6, 30, 23, 59, 59)
            )
        )
        # unreceived: last_receive_time and last_receive are both NONE
        unreceived_msg = db.get_unreceived_msgs()[0]
        assert unreceived_msg['test_memo'] == 'not_receive'

        open_shelve.return_value.__exit__.return_value = f.close()


    @mock.patch('event_plugins.common.storage.shelve_db.open_shelve')
    @pytest.mark.usefixtures("db")
    def test_have_successed_msgs(self, open_shelve, db):
        # clean db for inserting testing records
        remove_db(db.shelve_name)

        f = shelve.open(db.shelve_name, flag='c', writeback=True)
        open_shelve.return_value.__enter__.return_value = f

        # emulate received message
        received_msgs = [{"test": "received"}]

        # insert records - emulate second message have received
        db.insert(
            MessageRecord(
                key='{"test": "received"}',
                frequency='D',
                last_receive={'test': 1},
                last_receive_time=TimeUtils().datetime(2019, 6, 15, 1, 0, 0),
                timeout=TimeUtils().datetime(2019, 6, 15, 23, 59, 59)
            )
        )
        db.insert(
            MessageRecord(
                key='{"test": "have_received"}',
                frequency='M',
                last_receive={'test': 2},
                last_receive_time=TimeUtils().datetime(2019, 6, 20, 2, 0, 0),
                timeout=TimeUtils().datetime(2019, 6, 30, 23, 59, 59)
            )
        )
        #from datetime import datetime
        #import pytz
        db.insert(
            MessageRecord(
                key='{"test": "not_received"}',
                frequency='M',
                last_receive=None,
                last_receive_time=None,
                timeout=TimeUtils().datetime(2019, 6, 30, 23, 59, 59)
                #datetime(2019, 6, 30, 23, 59, 59, tzinfo=pytz.timezone('Asia/Taipei'))
                #TimeUtils().datetime(2019, 6, 30, 23, 59, 59)
            )
        )
        # have successed: last_receive_time is not None and last_recieve_time < timeout
        have_successed = db.have_successed_msgs(received_msgs)
        assert have_successed == [{"test": "have_received"}]

        open_shelve.return_value.__exit__.return_value = f.close()

# coding=utf-8
import os
import mock
import pytest
import pytz

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_

from airflow.models.base import Base

from event_plugins.common.schedule.time_utils import TimeUtils
from event_plugins.common.storage.event_message import EventMessage, EventMessageCRUD
from event_plugins.common.status import DBStatus


TEST_SENSOR_NAME = 'test'
TEST_SOURCE_TYPE = 'kafka'

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
# engine = create_engine('sqlite:///{home}/unittests.db'.format(home=AIRFLOW_HOME))
engine = create_engine('sqlite:///:memory:')

# create 'event_plugin' table if not exist
Base.metadata.create_all(engine)


def patch_now(mocker, now):
    mocker.patch.object(TimeUtils, 'get_now', return_value=now)

@pytest.fixture()
def session():
    session = sessionmaker(bind=engine)()
    session.execute(Base.metadata.tables['event_plugins'].delete())
    yield session
    session.close()

@pytest.fixture()
def db():
    # use source type:kafka and sensor_name:test for testing
    return EventMessageCRUD(source_type=TEST_SOURCE_TYPE, sensor_name=TEST_SENSOR_NAME)

@pytest.fixture()
def msg_list():
    return [
        {'frequency': 'D', 'topic': 'etl-finish', 'db': 'db0', 'table': 'tbl0',
            'partition_values': "{{yyyymm|dt.format(format='%Y%m')}}", 'task_id': "tbla"},
        {'frequency': 'M', 'topic': 'etl-finish', 'db': 'db0', 'table': 'tbl1',
            'partition_values': "", 'task_id': "tblc"}
    ]

@pytest.fixture()
def overwrite_msg_list():
    return [
        {'frequency': 'D', 'topic': 'etl-finish', 'db': 'db0', 'table': 'tbl1',
            'partition_values': "{{yyyymm|dt.format(format='%Y%m')}}", 'task_id': "tbla"},
        {'frequency': 'M', 'topic': 'etl-finish', 'db': 'db0', 'table': 'tbl2',
            'partition_values': "", 'task_id': "tblb"},
        {'frequency': 'D', 'topic': 'job-finish', 'job_name': 'jn1', 'task_id': "job1"},
    ]


class TestEventMessageCRUD:

    @pytest.mark.usefixtures("db", "session", "msg_list")
    def test_initialize_insert(self, db, session, msg_list):
        '''
            Check if successfully insert three rows in msg_list while db is not exist,
            and if timeout of all message is set
        '''
        db.initialize(msg_list, session=session)
        msgs = db.get_sensor_messages(session=session)
        assert msgs.count() == 2
        assert msgs.filter(EventMessage.timeout is None).count() == 0

    @pytest.mark.usefixtures("db", "session", "msg_list", "overwrite_msg_list")
    def test_initialize_update(self, db, session, msg_list, overwrite_msg_list):
        '''
            Check if successfully overwrite three rows in msg_list while db exists
            Note: overwrite_msg_list is different from msg_list,
                check if function remove unused message records and add new message records
        '''
        db.initialize(msg_list, session=session)
        msgs = db.get_sensor_messages(session=session)
        assert msgs.count() == 2
        db.initialize(overwrite_msg_list, session=session)
        msgs = db.get_sensor_messages(session=session)
        assert msgs.count() == 3

    @pytest.mark.usefixtures("db", "msg_list")
    def test_get_timeout(self, db, msg_list, mocker):
        patch_now(mocker, TimeUtils().datetime(2019, 6, 5, 8, 0, 0, tzinfo=pytz.utc))
        for msg in msg_list:
            timeout = db.get_timeout(msg)
            if msg['frequency'] == 'D':
                assert timeout == TimeUtils().datetime(2019, 6, 5, 23, 59, 59, tzinfo=pytz.utc)
            elif msg['frequency'] == 'M':
                assert timeout == TimeUtils().datetime(2019, 6, 30, 23, 59, 59, tzinfo=pytz.utc)

    @pytest.mark.usefixtures("db", "session")
    def test_reset_timeout(self, db, session, mocker):
        '''
            Check if last_receive and last_receive_time are assigned to None
            when time > message's timeout time
        '''
        # mock TimeUtils().get_now()
        patch_now(mocker, TimeUtils().datetime(2019, 6, 16, 0, 0, 0))
        fake_now = TimeUtils().datetime(2019, 6, 16, 0, 0, 0)

        # this record is timeout since fake_now > timeout
        msg1 = {"test_memo":"timeout", "frequency":"D", "topic":"etl-finish"}
        record1 = EventMessage(
            name=TEST_SENSOR_NAME,
            msg=msg1,
            source_type=TEST_SOURCE_TYPE,
            frequency=msg1['frequency'],
            last_receive={'test':1},
            last_receive_time=TimeUtils().datetime(2019, 6, 15),
            timeout=TimeUtils().datetime(2019, 6, 15, 23, 59, 59)
        )
        # this second record is not timeout since fake_now < timeout
        msg2 = {"test_memo":"not_timeout", "frequency":"M", "topic":"etl-finish"}
        record2 = EventMessage(
            name=TEST_SENSOR_NAME,
            msg=msg2,
            source_type=TEST_SOURCE_TYPE,
            frequency='M',
            last_receive={'test':2},
            last_receive_time=TimeUtils().datetime(2019, 6, 13),
            timeout=TimeUtils().datetime(2019, 6, 30, 23, 59, 59)
        )
        session.add_all([record1, record2])
        session.commit()

        db.reset_timeout(session=session)
        records = db.get_sensor_messages(session=session)
        timeout_record = records.filter(EventMessage.msg == msg1).first()
        assert (
            timeout_record.last_receive is None and
            timeout_record.last_receive_time is None and
            timeout_record.timeout == TimeUtils().datetime(2019, 6, 16, 23, 59, 59)
        )
        untimeout_record = records.filter(EventMessage.msg == msg2).first()
        assert (
            untimeout_record.last_receive == {'test':2} and
            untimeout_record.last_receive_time == TimeUtils().datetime(2019, 6, 13)
        )

    @pytest.mark.usefixtures("db", "session")
    def test_get_sensor_messages(self, db, session):
        assert db.get_sensor_messages(session=session).count() == 0

    @pytest.mark.usefixtures("db", "session")
    def test_status(self, db, session):
        # insert records - emulate second message not receiving yet
        msg1 = {"test_memo":"received", "frequency":"D", "topic":"etl-finish"}
        record1 = EventMessage(
            name=TEST_SENSOR_NAME,
            msg=msg1,
            source_type=TEST_SOURCE_TYPE,
            frequency='D',
            last_receive={'test':1},
            last_receive_time=TimeUtils().datetime(2019, 6, 15),
            timeout=TimeUtils().datetime(2019, 6, 15, 23, 59, 59)
        )
        msg2 = {"test_memo":"not_received", "frequency":"M", "topic":"etl-finish"}
        record2 = EventMessage(
            name=TEST_SENSOR_NAME,
            msg=msg2,
            source_type=TEST_SOURCE_TYPE,
            frequency='M',
            last_receive=None,
            last_receive_time=None,
            timeout=TimeUtils().datetime(2019, 6, 30, 23, 59, 59)
        )
        session.add_all([record1, record2])
        session.commit()
        assert db.get_sensor_messages(session=session).count() == 2
        assert db.status(session=session) == DBStatus.NOT_ALL_RECEIVED

        # last_receive_time and last_receive should be both NOT NONE,
        # or the status would be still NOT_ALL_RECEIVED
        session.query(EventMessage).filter(
            and_(
                EventMessage.name == TEST_SENSOR_NAME,
                EventMessage.msg == msg2
            )
        ).update({'last_receive_time': TimeUtils().datetime(2019, 6, 13)})
        session.commit()
        assert db.get_sensor_messages(session=session).count() == 2
        assert db.status(session=session) == DBStatus.NOT_ALL_RECEIVED

        # change second record to emulate message received
        session.query(EventMessage).filter(
            and_(
                EventMessage.name == TEST_SENSOR_NAME,
                EventMessage.msg == msg2
            )
        ).update({'last_receive': {'test':1}})
        session.commit()
        assert db.get_sensor_messages(session=session).count() == 2
        assert db.status(session=session) == DBStatus.ALL_RECEIVED

        # if receive zero value as message, it is RECEIVED when checking status
        # TODO this might change since only json format is valid message so far
        session.query(EventMessage).filter(
            and_(
                EventMessage.name == TEST_SENSOR_NAME,
                EventMessage.msg == msg2
            )
        ).update({'last_receive': 0})
        assert db.status(session=session) == DBStatus.ALL_RECEIVED

    @pytest.mark.usefixtures("db", "session")
    def test_get_unreceived_msgs(self, db, session):
        # insert records - emulate second message not receiving yet
        msg1 = {"test_memo":"have_receive"}
        record1 = EventMessage(
            name=TEST_SENSOR_NAME,
            msg=msg1,
            source_type=TEST_SOURCE_TYPE,
            frequency='D',
            last_receive={'test':1},
            last_receive_time=TimeUtils().datetime(2019, 6, 15),
            timeout=TimeUtils().datetime(2019, 6, 15, 23, 59, 59)
        )
        msg2 = {"test_memo":"not_receive"}
        record2 = EventMessage(
            name=TEST_SENSOR_NAME,
            msg=msg2,
            source_type=TEST_SOURCE_TYPE,
            frequency='M',
            last_receive=None,
            last_receive_time=None,
            timeout=TimeUtils().datetime(2019, 6, 30, 23, 59, 59)
        )
        session.add_all([record1, record2])
        session.commit()

        # unreceived: last_receive_time and last_receive are both NONE
        unreceived_msg = db.get_unreceived_msgs(session)[0]
        assert unreceived_msg['test_memo'] == 'not_receive'

    @pytest.mark.usefixtures("db", "session")
    def test_have_successed_msgs(self, db, session):
        # emulate received message
        received_msgs = [{"test": "received"}]

        msg1 = {"test": "received"}
        record1 = EventMessage(
            name=TEST_SENSOR_NAME,
            msg=msg1,
            source_type=TEST_SOURCE_TYPE,
            frequency='D',
            last_receive={'test':1},
            last_receive_time=TimeUtils().datetime(2019, 6, 15, 1, 0, 0),
            timeout=TimeUtils().datetime(2019, 6, 15, 23, 59, 59)
        )
        msg2 = {"test": "have_received"}
        record2 = EventMessage(
            name=TEST_SENSOR_NAME,
            msg=msg2,
            source_type=TEST_SOURCE_TYPE,
            frequency='M',
            last_receive={'test': 2},
            last_receive_time=TimeUtils().datetime(2019, 6, 20, 2, 0, 0),
            timeout=TimeUtils().datetime(2019, 6, 30, 23, 59, 59)
        )
        msg3 = {"test": "not_received"}
        record3 = EventMessage(
            name=TEST_SENSOR_NAME,
            msg=msg3,
            source_type=TEST_SOURCE_TYPE,
            frequency='M',
            last_receive=None,
            last_receive_time=None,
            timeout=TimeUtils().datetime(2019, 6, 30, 23, 59, 59)
        )
        session.add_all([record1, record2, record3])
        session.commit()

        # have successed: last_receive_time is not None and last_recieve_time < timeout
        have_successed = db.have_successed_msgs(received_msgs, session)
        assert have_successed == [{"test": "have_received"}]

    @pytest.mark.usefixtures("db", "session")
    def test_update_on_receive(self, db, session, mocker):
        # mock TimeUtils().get_now()
        patch_now(mocker, TimeUtils().datetime(2019, 6, 15, 14, 0, 0))
        fake_now = TimeUtils().datetime(2019, 6, 15, 14, 0, 0)

        msg1 = {"test": "received"}
        record1 = EventMessage(
            name=TEST_SENSOR_NAME,
            msg=msg1,
            source_type=TEST_SOURCE_TYPE,
            frequency='D',
            last_receive=None,
            last_receive_time=None,
            timeout=TimeUtils().datetime(2019, 6, 15, 23, 59, 59)
        )
        session.add(record1)
        session.commit()

        db.update_on_receive(msg1, msg1, session)
        record = db.get_sensor_messages(session).first()
        assert record.last_receive == msg1
        assert record.last_receive_time == fake_now

    @pytest.mark.usefixtures("db", "session")
    def test_delete(self, db, session):
        msg1 = {"test": "received"}
        record1 = EventMessage(
            name=TEST_SENSOR_NAME,
            msg=msg1,
            source_type=TEST_SOURCE_TYPE,
            frequency='D',
            last_receive=None,
            last_receive_time=None,
            timeout=TimeUtils().datetime(2019, 6, 15, 23, 59, 59)
        )
        session.add(record1)
        session.commit()
        assert db.get_sensor_messages(session).count() == 1
        db.delete(session)
        assert db.get_sensor_messages(session).count() == 0

    @pytest.mark.usefixtures("db", "session")
    def test_tabluate_data(self, db, session, capsys):
        msg1 = {"test": "received"}
        record1 = EventMessage(
            name=TEST_SENSOR_NAME,
            msg=msg1,
            source_type=TEST_SOURCE_TYPE,
            frequency='D',
            last_receive=None,
            last_receive_time=None,
            timeout=TimeUtils().datetime(2019, 6, 15, 23, 59, 59)
        )
        msg2 = {"test": "have_received"}
        record2 = EventMessage(
            name=TEST_SENSOR_NAME,
            msg=msg2,
            source_type=TEST_SOURCE_TYPE,
            frequency='M',
            last_receive={'test': 2},
            last_receive_time=TimeUtils().datetime(2019, 6, 20, 2, 0, 0),
            timeout=TimeUtils().datetime(2019, 6, 30, 23, 59, 59)
        )
        session.add_all([record1, record2])
        session.commit()

        result = db.tabulate_data(session=session)
        print result
        captured = capsys.readouterr()
        expected_result = (
"""╒══════╤════════╤═════════════════════════════╤═══════════════╤═════════════╤════════════════╤═════════════════════╤═════════════════════╕
│   id │ name   │ msg                         │ source_type   │ frequency   │ last_receive   │ last_receive_time   │ timeout             │
╞══════╪════════╪═════════════════════════════╪═══════════════╪═════════════╪════════════════╪═════════════════════╪═════════════════════╡
│    1 │ test   │ {u'test': u'received'}      │ kafka         │ D           │ None           │ None                │ 2019-06-15 23:59:59 │
├──────┼────────┼─────────────────────────────┼───────────────┼─────────────┼────────────────┼─────────────────────┼─────────────────────┤
│    2 │ test   │ {u'test': u'have_received'} │ kafka         │ M           │ {u'test': 2}   │ 2019-06-20 02:00:00 │ 2019-06-30 23:59:59 │
╘══════╧════════╧═════════════════════════════╧═══════════════╧═════════════╧════════════════╧═════════════════════╧═════════════════════╛
""".decode('utf8'))
        assert captured.out == expected_result
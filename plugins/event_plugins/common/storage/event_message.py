import ConfigParser
import json
import os
import pytz
import six
from datetime import datetime
from tabulate import tabulate

from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import and_
from sqlalchemy.orm import validates

from airflow.models import Base
from airflow.utils.db import provide_session

from event_plugins import factory
from event_plugins.common.status import DBStatus
from event_plugins.common.schedule.time_utils import TimeUtils
from event_plugins.common.storage.db import STORAGE_CONF, db_commit


def get_string_if_json(msg):
    if msg is None:
        return
    elif isinstance(msg, dict):
        return json.dumps(msg, sort_keys=True)
    elif isinstance(msg, six.string_types):
        return msg
    else:
        raise TypeError("msg should be either string or dict type")


class EventMessage(Base):

    __tablename__ = STORAGE_CONF.get("Storage", "table_name")
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    msg = Column(String, nullable=False)
    source_type = Column(String(32), nullable=False)
    frequency = Column(String(4), nullable=False)
    last_receive = Column(String)
    last_receive_time = Column(DateTime(timezone=True))
    timeout = Column(DateTime(timezone=True))

    # available options for fields
    available_frequency = ['D', 'M']
    available_source_type = ['base', 'kafka']  # base option is for testing

    def __init__(self, name, msg, source_type, frequency, last_receive, last_receive_time, timeout):
        '''
            name(string): sensor name, to identify different sensors in airflow
            msg(string|dict): store as 'string' type in database
                wanted message. json dumps(sort_keys=True) if input type is dict,
                remain input if input type is string(json format).
            source_type(string): consume source name. e.g., kafka
            frequency(string): string such as 'D' or 'M' to show received frequency of message
            last_receive(string|dict|None): store as 'string' type or None in database
                the last received message. json dumps(sort_keys=True) if input type is dict,
                remain input if input type is string(can be non-json format) or None.
            last_receive_time(datetime): the last time received message in 'last_receive' column
            timeout(datetime): when will the received message time out
        '''
        self.msg = self.check_and_get_json_string(msg)
        self.name = name
        self.source_type = source_type
        self.frequency = frequency
        self.last_receive = get_string_if_json(last_receive)
        self.last_receive_time = last_receive_time
        self.timeout = timeout

    def check_and_get_json_string(self, msg):
        if isinstance(msg, dict):
            return json.dumps(msg, sort_keys=True)
        elif isinstance(msg, six.string_types):
            try:
                json.loads(msg)
                return msg
            except ValueError:
                print("string should be json format")
                raise
        else:
            raise TypeError("msg should be either string or dict type")

    @validates('frequency')
    def validate_frequency(self, key, frequency):
        if frequency not in self.available_frequency:
            raise ValueError("frequency should be in " + str(self.available_frequency))
        return frequency

    @validates('source_type')
    def validate_source_type(self, key, source_type):
        if source_type not in self.available_source_type:
            raise ValueError("source_type {} not in {}".format(source_type, self.available_source_type))
        return source_type


class EventMessageCRUD:

    @provide_session
    def __init__(self, source_type, sensor_name, session=None):
        self.source_type = source_type
        self.sensor_name = sensor_name
        self.session = session

    @db_commit
    def initialize(self, msg_list, dt=None):
        if self.get_sensor_messages().count() > 0:
            dt = dt or TimeUtils().get_now()
            self.update_msgs(msg_list)
            self.reset_timeout(base_time=dt)
        else:
            for msg in msg_list:
                str_msg = get_string_if_json(msg)
                record = EventMessage(
                    name=self.sensor_name,
                    msg=str_msg,
                    source_type=self.source_type,
                    frequency=msg['frequency'],
                    last_receive=None,
                    last_receive_time=None,
                    timeout=self.get_timeout(msg)
                )
                self.session.add(record)

    def get_sensor_messages(self):
        ''' get messages of self.sensor_name '''
        records = self.session.query(EventMessage).filter(EventMessage.name == self.sensor_name)
        return records

    @db_commit
    def update_msgs(self, msg_list):
        '''Compare msgs in msg_list to msgs in db. If there are msgs only exist in db,
            we assume that user do not need old msg, it would delete msgs in db, and
            insert new msgs which is not in db.
            Args:
                msg_list(list of json object): messages that need to be record in db
        '''
        exist_records = self.session.query(EventMessage).filter(EventMessage.name == self.sensor_name)
        str_msg_list = map(get_string_if_json, msg_list)
        del_record_list = filter(lambda r: r.msg not in str_msg_list, exist_records)
        del_ids = map(lambda r: r.id, del_record_list)
        exist_records.filter(EventMessage.id.in_(del_ids)).delete(synchronize_session='fetch')

        new_msgs = list()
        for msg in msg_list:
            str_msg = get_string_if_json(msg)
            if str_msg not in [r.msg for r in exist_records]:
                new_msgs.append(msg)
        for new_msg in new_msgs:
            str_new_msg = get_string_if_json(new_msg)
            record = EventMessage(
                name=self.sensor_name,
                msg=str_new_msg,
                source_type=self.source_type,
                frequency=new_msg['frequency'],
                last_receive=None,
                last_receive_time=None,
                timeout=self.get_timeout(new_msg)
            )
            self.session.add(record)

    @db_commit
    def reset_timeout(self, base_time=None):
        '''Clear last_receive_time and last_receive if base time > timeout of msgs in db
            Args:
                time(time-aware datetime): base time to handle timeout, use now if not given

            e.g., If frequncy is 'D', the system will expect for new message coming everyday,
            clean the last received information of the message.
            ----- 2019/06/15 -----
            | msg | frequency | last_receive_time        |  last_receive |  timeout  |
            | a   | D         | dt(2019, 6, 15, 9, 0, 0) | 'test'        | dt(2019, 6, 15, 23, 59, 59) |
            ----- 2019/06/16 -----
            | msg | frequency | last_receive_time        |  last_receive |  timeout  |
            | a   | D         | None                     |  None         | dt(2019, 6, 16, 23, 59, 59) |
        '''
        base_time = base_time or TimeUtils().get_now()
        update_records = self.session.query(EventMessage).filter(
            and_(
                EventMessage.name == self.sensor_name,
                EventMessage.timeout < base_time
            )
        )
        for record in update_records:
            record.last_receive_time = None
            record.last_receive = None
            record.timeout = self.get_timeout(json.loads(record.msg))

    def get_timeout(self, msg):
        '''Get timeout defined by each plugin
            Args:
                msg(dict): wanted message
        '''
        return factory.plugin_factory(self.source_type) \
                .msg_handler(msg=msg, mtype='wanted').timeout()

    def status(self):
        '''Status of self.sensor_name
            Return(define in status.py):
                ALL_RECEIVED: if all messages get last_receive and last_receive_time
                NOT_ALL_RECEIVED: there're messages that haven't gotten received time
        '''
        records = self.session.query(EventMessage).filter(EventMessage.name == self.sensor_name)
        for r in records:
            if r.last_receive is None or r.last_receive_time is None:
                return DBStatus.NOT_ALL_RECEIVED
        return DBStatus.ALL_RECEIVED

    def get_unreceived_msgs(self):
        '''
        Return:
            json object list of not-received messages
        '''
        records = self.session.query(EventMessage).filter(EventMessage.name == self.sensor_name)
        unreceive_msgs = [json.loads(r.msg) for r in records if r.last_receive is None \
                                                            and r.last_receive_time is None]
        return unreceive_msgs

    def have_successed_msgs(self, received_msgs):
        '''This function is used to skip messages that have received before
            and not timeout. e.g. monthly source.
            Since monthly messages might received more that once within a month
            This function should be invoked after consuming source messages.
            Args:
                received_msgs(list of messages in json format):
                    messages that received and match from source
            Returns:
                json object list of messages that have received
        '''
        records = self.session.query(EventMessage).filter(EventMessage.name == self.sensor_name)
        successed_msgs = [json.loads(r.msg) for r in records if r.last_receive_time is not None \
                                                and r.last_receive_time < r.timeout]
        successed_but_not_receive = list()
        for successed in successed_msgs:
            if successed not in received_msgs:
                successed_but_not_receive.append(successed)
        return successed_but_not_receive

    @db_commit
    def update_on_receive(self, match_wanted, receive_msg):
        ''' Update last receive time and object when receiving wanted message '''
        str_match_wanted = get_string_if_json(match_wanted)
        records = self.session.query(EventMessage).filter(EventMessage.name == self.sensor_name)
        for record in records:
            if record.msg == str_match_wanted:
                record.last_receive_time = TimeUtils().get_now()
                record.last_receive = get_string_if_json(receive_msg)

    @db_commit
    def delete(self):
        ''' delete all messages rows of self.sensor_name '''
        self.session.query(EventMessage) \
            .filter(EventMessage.name == self.sensor_name) \
            .delete(synchronize_session='fetch')

    def tabulate_data(self, threshold=None, tablefmt='fancy_grid'):
        headers = [str(c).split('.')[1] for c in EventMessage.__table__.columns]
        data = list()
        records = self.session.query(EventMessage).filter(EventMessage.name == self.sensor_name)
        for r in records:
            rows = list()
            for col in headers:
                str_val = str(getattr(r, col))
                if threshold:
                    rows.append(str_val if len(str_val) <= threshold else str_val[:threshold] + '...')
                else:
                    rows.append(str_val)
            data.append(rows)
        return tabulate(data, headers=headers, tablefmt=tablefmt)

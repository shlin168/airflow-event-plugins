from __future__ import print_function

import os
import json
import shelve
from tabulate import tabulate
from contextlib import contextmanager

from event_plugins import factory
from event_plugins.common.schedule.time_utils import TimeUtils


@contextmanager
def open_shelve(name, *args, **kwargs):
    f = shelve.open(name, *args, **kwargs)
    yield f
    f.close()


class DBStatus:

    ALL_RECEIVED = 'all_received'
    NOT_ALL_RECEIVED = 'not_all_received'


class MessageRecord:

    def __init__(self, key, frequency, last_receive, last_receive_time, timeout):
        '''
            key(string): json dumps of wanted message
            frequency(string): string such as 'D' or 'M' to show received frequency of message
            last_receive(json_obj): the last received message
            last_receive_time(datetime): the last time received message in 'last_receive' column
            timeout(datetime): when will the received message time out
        '''
        self.key = key
        self.frequency = frequency
        self.last_receive = last_receive
        self.last_receive_time = last_receive_time
        self.timeout = timeout


class ShelveCRUD(object):

    def __init__(self, shelve_name):
        self.shelve_name = shelve_name

    def is_exist(self):
        # shelve db might have different extension depends on 'anydbm' module
        extension_choices = ['', '.db', '.dir', '.pag']
        for ext in extension_choices:
            if os.path.exists(self.shelve_name + ext):
                return True
        return False

    def insert(self, obj):
        with open_shelve(self.shelve_name, flag='c') as db:
            if obj.key in db:
                print('{} already in db, overwrie'.format(obj.key))
            else:
                print('{} not in db, insert'.format(obj.key))

            db[obj.key] = {
                'frequency': obj.frequency,
                'last_receive_time': obj.last_receive_time,
                'last_receive': obj.last_receive,
                'timeout': obj.timeout
            }
            db.sync()
        self.tabulate_data()

    def update(self, key, data_dict):
        # need writeback parameter for updating value
        with open_shelve(self.shelve_name, flag='c', writeback=True) as db:
            if db.get(key):
                for k, v in data_dict.items():
                    db.get(key)[k] = v
            else:
                print("key {} not found".format(key))
                return None

    def get(self, key):
        with open_shelve(self.shelve_name, flag='c') as db:
            if db.get(key):
                return db.get(key)
            else:
                print("key {} not found".format(key))
                return None

    def delete(self, key):
        with open_shelve(self.shelve_name, flag='c') as db:
            val = db.pop(key)
            print('delete key: {k}, val:{v}'.format(k=key, v=val))

    def delete_all(self):
        with open_shelve(self.shelve_name, flag='c') as db:
            for key in self.list_keys():
                db.pop(key)

    def list_all(self):
        with open_shelve(self.shelve_name, flag='c') as db:
            return [db[key] for key in db]
        return []

    def list_keys(self):
        with open_shelve(self.shelve_name, flag='c') as db:
            return db.keys()

    def tabulate_data(self, threshold=None, tablefmt='fancy_grid'):
        headers = ['key', 'frequency', 'last_receive', 'last_receive_time', 'timeout']
        data = list()
        for key in self.list_keys():
            tkey = key[:threshold] + '...' if threshold and len(key) > threshold else key
            rows = [tkey]
            for col in headers[1:]:
                str_val = str(self.get(key)[col])
                if threshold:
                    rows.append(str_val if len(str_val) <= threshold else str_val[:threshold] + '...')
                else:
                    rows.append(str_val)
            data.append(rows)
        return tabulate(data, headers=headers, tablefmt=tablefmt)


class MessageRecordCRUD(ShelveCRUD):

    valid_freq = ['D', 'M']

    def __init__(self, shelve_name, plugin_name):
        super(MessageRecordCRUD, self).__init__(shelve_name)
        self.plugin_name = plugin_name

    def initialize(self, msg_list, dt=None):
        if self.is_exist():
            dt = dt or TimeUtils().get_now()
            self.remove_deprecated_msgs(msg_list)
            self.add_new_msgs(msg_list)
            self.check_and_reset_timeout(dt)
        else:
            for msg in msg_list:
                msg_str = json.dumps(msg, sort_keys=True)
                kmsg = MessageRecord(
                    key=msg_str,
                    frequency=msg['frequency'],
                    last_receive=None,
                    last_receive_time=None,
                    timeout=self.get_timeout(msg)
                )
                self.insert(kmsg)

    def get_timeout(self, msg):
        return factory.plugin_factory(self.plugin_name) \
                .msg_handler(msg=msg, mtype='wanted').timeout()

    def clear_by_key_col(self, key, col):
        val = self.get(key)
        if val:
            self.update(key, {col: None})

    def check_and_reset_timeout(self, base_time=None):
        '''
        Clear last_receive_time and last_receive if base time > timeout of msgs in db
        Args:
            time(datetime): base time to handle timeout, use now if not given
        Note:
            e.g., If frequncy is 'D', the system will expect for new message coming everyday,
            clean the last received information of the message.
            ----- 2019/06/15 -----
            | msg_key | frequency | last_receive_time        |  last_receive |  timeout  |
            | a       | D         | dt(2019, 6, 15, 9, 0, 0) | 'test'        | dt(2019, 6, 15, 23, 59, 59) |
            ----- 2019/06/16 -----
            | msg_key | frequency | last_receive_time        |  last_receive |  timeout  |
            | a       | D         | None                     |  None         | dt(2019, 6, 16, 23, 59, 59) |
        '''
        base_time = base_time or TimeUtils().get_now()
        for key in self.list_keys():
            if base_time >= self.get(key)['timeout']:
                print('[clear last receive] {}'.format(key))
                self.clear_by_key_col(key, 'last_receive_time')
                self.clear_by_key_col(key, 'last_receive')
                self.update(key, {'timeout': self.get_timeout(json.loads(key))})

    def remove_deprecated_msgs(self, msg_list):
        '''
        Args:
            msg_list(list of json object): messages that need to be record in db
        Note:
            Compare msgs in msg_list to msgs in db. If there are msgs only exist in db,
            delete msgs in db
        '''
        key = self.list_keys()
        deprecated_keys = list(set(key) - set([json.dumps(m, sort_keys=True) for m in msg_list]))
        for dkey in deprecated_keys:
            self.delete(dkey)

    def add_new_msgs(self, msg_list):
        '''
        Args:
            msg_list(list of json object): messages that need to be record in db
        Note:
            Compare msgs in msg_list to msgs in db. If there are new msgs not in db,
            insert new msgs and set timeout
        '''
        key = self.list_keys()
        new_keys = list(set([json.dumps(m, sort_keys=True) for m in msg_list]) - set(key))
        for nkey in new_keys:
            kmsg = MessageRecord(
                key=nkey,
                frequency=json.loads(nkey)['frequency'],
                last_receive=None,
                last_receive_time=None,
                timeout=self.get_timeout(json.loads(nkey))
            )
            self.insert(kmsg)

    def status(self):
        '''
        Return:
            STATUS through all messages in db
                ALL_RECEIVED: if all messages get last_receive and last_receive_time
                NOT_ALL_RECEIVED: there're messages that haven't gotten received time
        '''
        if all((m['last_receive_time'] is not None and m['last_receive'] is not None)
            for m in self.list_all()):
            return DBStatus.ALL_RECEIVED
        else:
            return DBStatus.NOT_ALL_RECEIVED

    def get_unreceived_msgs(self):
        '''
        Return:
            json object list of not-received messages
        '''
        not_received_list = list()
        for msg in self.list_keys():
            if (self.get(msg)['last_receive'] is None
                 and self.get(msg)['last_receive_time'] is None):
                not_received_list.append(json.loads(msg))
        return not_received_list

    def have_successed_msgs(self, received_msgs):
        '''
        Note:
            This function is used to skip messages that have received before
            and not timeout. e.g. monthly source.
            since monthly messages might received more that once within a month
            This function should be invoked after consuming source messages.
        Args:
            received_msgs(list of messages in json format):
                messages that received and match from source
        Returns:
            json object list of messages that have received
        '''
        received_msgs = [json.dumps(msg, sort_keys=True) for msg in received_msgs]
        have_successed = list()
        for key in self.list_keys():
            val = self.get(key)
            if val['last_receive_time'] and val['last_receive_time'] < val['timeout']:
                if key not in received_msgs:
                    have_successed.append(json.loads(key))
        return have_successed

    def update_on_receive(self, match_wanted, receive_msg):
        '''
            update last receive time and object when receiving wanted message
        '''
        self.update(json.dumps(match_wanted, sort_keys=True), {
            'last_receive_time': TimeUtils().get_now(),
            'last_receive': receive_msg
        })

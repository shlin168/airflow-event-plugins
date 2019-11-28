# -*- coding: UTF-8 -*-
import json
import os
import pytest

from event_plugins.kafka.produce.utils import merge_multiple_files
from event_plugins.kafka.produce.factory import topic_factory
from event_plugins.kafka.kafka_producer_plugin import KafkaProducerFromMergeFileOperator


test_home = os.path.join(os.environ.get('SERVICE_HOME'), 'test_plugins')
file_list = [os.path.join(test_home, 'testa.txt'),
            os.path.join(test_home, 'testb.txt')]

@pytest.fixture(scope="class", autouse=True)
def prepare_test_files():
    merge_a = [
        {"topic": "job-finish", "data": [
            {"timestamp": 1564568760,
             "proj_name": "test_gp",
             "duration_time": 50,
             "is_success": True,
             "job_name": "gp_001"},
            {"timestamp": 1564569620,
             "proj_name": "test_ds",
             "duration_time": 100,
             "is_success": True,
             "job_name": "ds1"}]},
        {"topic": "hive-sink-finish", "data": [
            {"db": "tmp",
             "table": "test",
             "partition_fields": "group",
             "partition_values": "gp_001",
             "count": 60,
             "data_date": "2019-07-14",
             "system_datetime": "2019-07-29 14:29:26",
             "duration_time": 300,
             "options": {"items_info":
                [{"item_id": "ctct0001", "item_name": "abc", "count": 123},
                 {"item_id": "ctct0002", "item_name": "def", "count": 456}]}
            }]
        }]
    merge_b = [
        {"topic": "job-finish", "data": [
            {"timestamp": 1564568770,
             "proj_name": "test_gp",
             "duration_time": 150,
             "is_success": True,
             "job_name": "gp_002"},
            {"timestamp": 1564569650,
             "proj_name": "test_ds",
             "duration_time": 200,
             "is_success": True,
             "job_name": "ds2"}]},
        {"topic": "hive-sink-finish", "data": [
            {"db": "tmp",
             "table": "test",
             "partition_fields": "group",
             "partition_values": "gp_002",
             "count": 40,
             "data_date": "2019-07-14",
             "system_datetime": "2019-07-29 14:30:00",
             "duration_time": 200,
             "options": {"items_info":
                [{"item_id": "ctct0003", "item_name": "ghi", "count": 789},
                 {"item_id": "ctct0004", "item_name": "jkl", "count": 100}]}
            }]
        }]
    with open(file_list[0], 'w') as f:
        f.write("\n".join([json.dumps(v) for v in merge_a]))
    with open(file_list[1], 'w') as f:
        f.write("\n".join([json.dumps(v) for v in merge_b]))

    yield
    # clean files after test
    for f in file_list:
        os.remove(f)


class TestKafkaProducerFromMergeFileOperator:

    def test_merge_multiple_files(self):
        result = merge_multiple_files(file_list)
        assert len(result) == 2
        assert len(result["job-finish"]) == 4
        assert len(result["hive-sink-finish"]) == 2

    def test_factory(self):
        assert topic_factory("job-finish") == topic_factory("uat-job-finish")
        assert topic_factory("hive-sink-finish") == topic_factory("uat-hive-sink-finish")
        assert topic_factory("test_none") is None

    def test_get_merge_msg(self):
        match_dict = {
            'HiveSinkFinish': [
                {'db': 'tmp',
                 'table': 'test',
                 'partition_fields': 'group'}],
            'JobFinish': [
                {'proj_name': 'test_gp',
                 'is_success': True},
                {'proj_name': 'test_ds',
                 'is_success': True},
            ]
        }
        result = merge_multiple_files(file_list)
        op = KafkaProducerFromMergeFileOperator(
            task_id='test_send_from_merge_file',
            broker='localhost:9092',
            match_dict=match_dict,
            file_list=file_list
        )
        for topic, msgs in result.iteritems():
            merge_msgs = op.get_merge_msg(
                topic_factory(topic), topic, result[topic])
            if merge_msgs:
                merge_msgs = list(merge_msgs)
                if topic == 'hive-sink-finish':
                    assert len(merge_msgs) == 1
                    assert cmp(merge_msgs[0], {
                        'db': 'tmp',
                        'table': 'test',
                        'partition_fields': 'group',
                        'partition_values': 'gp_001+gp_002',
                        'count': 100,
                        'data_date': '2019-07-14',
                        'system_datetime': '2019-07-29 14:30:00',
                        'duration_time': 500,
                        'options': {'items_info':
                            [{'item_id': 'ctct0003', 'item_name': 'ghi', 'count': 789},
                             {'item_id': 'ctct0004', 'item_name': 'jkl', 'count': 100}]
                        }
                    }) == 0
                elif topic == 'job-finish':
                    assert len(merge_msgs) == 2
                    assert cmp(merge_msgs[0], {
                        "duration_time": 200,
                        "timestamp": 1564568770,
                        "is_success": True,
                        "job_name": "gp_001+gp_002",
                        "proj_name": "test_gp"
                    }) == 0
                    assert cmp(merge_msgs[1], {
                        "duration_time": 300,
                        "timestamp": 1564569650,
                        "is_success": True,
                        "job_name": "ds1+ds2",
                        "proj_name": "test_ds"
                    }) == 0

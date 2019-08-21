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
        {"topic": "hippo-finish", "data": [
            {"job_id": "gp_001_1564568760",
             "finish_time": 1564568760,
             "duration_time": 50,
             "hippo_name": "uat.project",
             "is_success": True,
             "job_name": "gp_001"},
            {"job_id": "ds_1_1564569620",
             "finish_time": 1564569620,
             "duration_time": 100,
             "hippo_name": "ut.ds",
             "is_success": True,
             "job_name": "ds_1"}]},
        {"topic": "hive-sink-finish", "data": [
            {"partition_values": ["gp_001"],
             "db": "btmp_cmd",
             "table": "test",
             "exec_date": "2019-07-14",
             "job_type": "single",
             "system_datetime": "2019-07-29 14:29:26",
             "partition_fields": ["exec_group"],
             "count": 60,
             "duration_time": 300,
             "options": {"items_info":
                [{"item_id": "ctct0001", "item_name": "abc", "count": 123},
                 {"item_id": "ctct0002", "item_name": "def", "count": 456}]}
            }]
        }]
    merge_b = [
        {"topic": "hippo-finish", "data": [
            {"job_id": "gp_002_1564568770",
             "finish_time": 1564568770,
             "duration_time": 150,
             "hippo_name": "uat.project",
             "is_success": True,
             "job_name": "gp_002"},
            {"job_id": "ds_2_1564569650",
             "finish_time": 1564569650,
             "duration_time": 200,
             "hippo_name": "ut.ds",
             "is_success": True,
             "job_name": "ds_2"}]},
        {"topic": "hive-sink-finish", "data": [
            {"partition_values": ["gp_002"],
             "db": "btmp_cmd",
             "table": "test",
             "exec_date": "2019-07-14",
             "job_type": "single",
             "system_datetime": "2019-07-29 14:30:00",
             "partition_fields": ["exec_group"],
             "count": 40,
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
        assert len(result["hippo-finish"]) == 4
        assert len(result["hive-sink-finish"]) == 2

    def test_factory(self):
        assert topic_factory("hippo-finish") == topic_factory("uat-hippo-finish")
        assert topic_factory("hive-sink-finish") == topic_factory("uat-hive-sink-finish")
        assert topic_factory("test_none") is None

    def test_get_merge_msg(self):
        match_dict = {
            'HiveSinkFinish': [
                {'db': 'btmp_cmd',
                 'table': 'test',
                 'partition_fields': ['exec_group']}],
            'HippoFinish': [
                {'hippo_name': 'uat.project',
                 'is_success': True},
                {'hippo_name': 'ut.ds',
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
                    assert (json.dumps(merge_msgs[0], sort_keys=True) ==
                        '{"count": 100, "db": "btmp_cmd", "duration_time": 500, '
                        '"exec_date": "2019-07-14", "job_type": "single", '
                        '"options": {"items_info": '
                            '[{"count": 789, "item_id": "ctct0003", "item_name": "ghi"}, '
                            '{"count": 100, "item_id": "ctct0004", "item_name": "jkl"}]}, '
                        '"partition_fields": ["exec_group"], "partition_values": [""], '
                        '"system_datetime": "2019-07-29 14:30:00", "table": "test"}')
                elif topic == 'hippo-finish':
                    assert len(merge_msgs) == 2
                    assert (json.dumps(merge_msgs[0], sort_keys=True) ==
                        '{"duration_time": 200, "finish_time": 1564568770, '
                        '"hippo_name": "uat.project", "is_success": true, '
                        '"job_id": "uat.project_1564568770", "job_name": "uat.project"}')
                    assert (json.dumps(merge_msgs[1], sort_keys=True) ==
                        '{"duration_time": 300, "finish_time": 1564569650, '
                        '"hippo_name": "ut.ds", "is_success": true, '
                        '"job_id": "ut.ds_1564569650", "job_name": "ut.ds"}')

# -*- coding: UTF-8 -*-
from __future__ import print_function

import json
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.operators.kafka_plugin import KafkaProducerOperator, KafkaConsumerOperator
from airflow.operators.kafka_plugin import KafkaStatusEmailOperator

import pendulum

# Set timezone
local_tz = pendulum.timezone("Asia/Taipei")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 4, 2, 0, 0, tzinfo=local_tz),
    'depends_on_past': False,   # run even if previous task failed
    'retries': 0,
    'retry_delay': timedelta(seconds=30)
}

# Define wanted messages, remember to create these two topics in kafka before testing
kafka_msgs = [
    # two messages from 'etl-finish' topic
    {'frequency': 'D', 'topic': 'etl-finish', 'db': 'db0', 'table': 'table0', 'partition_values': "", 'task_id': "tbl0"},
    {'frequency': 'D', 'topic': 'etl-finish', 'db': 'db1', 'table': 'table1',
        'partition_values': "{{yyyymm|dt.format(format='%Y%m')}}", 'task_id': "tbl1"},
    # one message from 'job-finish' topic
    {'frequency': 'D', 'topic': 'job-finish', 'job_name': "jn0", 'is_success': True,
        'task_id': 'job0'}
]

# Define kafka broker location
broker = 'localhost:9092'

# Define DAG
with DAG('kafka_trigger_test',
         default_args=default_args,
         schedule_interval=None,
         max_active_runs=1,
         catchup=False) as dag:

    # kafka sensor
    my_consumer = KafkaConsumerOperator(
        task_id='my_consumer',
        # sensor_name="kafka_trigger_test.my_consumer", # use dag_id.task_id if not given in KafkaConsumerOperator
        broker=broker,
        group_id='airflow-test',
        client_id='airflow-test',
        msgs=kafka_msgs,
        poke_interval=10,
        timeout=120,
        mark_success=True,
        soft_fail=False,
        mode='reschedule',
        debug_mode=True,
    )

    # dummy tasks to show event status
    showA = DummyOperator(task_id='tbl0')
    showB = DummyOperator(task_id='tbl1')
    showC = DummyOperator(task_id='job0')

    # the task that would be done if getting all three messages
    my_task = BashOperator(
        task_id='my_task',
        bash_command="echo {{ execution_date }}",
        trigger_rule=TriggerRule.NONE_FAILED
    )

    # send email for the messages status if failed
    # need to set smtp server in airflow.cfg for sending email first
    send_status_email = KafkaStatusEmailOperator(
        task_id="send_status_mail",
        sensor_name="kafka_trigger_test.my_consumer",
        to="<email address>",
        trigger_rule=TriggerRule.ALL_FAILED
    )
    success_msg = {'job_name': 'my_task', 'is_success':True, 'timestamp': int(time.time())}
    send_success_msg = KafkaProducerOperator(
        task_id='send_success_msg',
        topic='job-finish',
        broker=broker,
        data=[json.dumps(success_msg, sort_keys=True)],
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    failed_msg = {'job_name': 'my_task', 'is_success':False, 'timestamp': int(time.time())}
    send_failed_msg = KafkaProducerOperator(
        task_id='send_failed_msg',
        topic='job-finish',
        broker=broker,
        data=[json.dumps(failed_msg, sort_keys=True)],
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # define workflow
    my_consumer >> [showA, showB, showC]
    my_consumer >> send_status_email
    [showA, showB, showC] >> my_task
    my_task >> [send_success_msg, send_failed_msg]

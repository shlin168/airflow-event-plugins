# Airflow Plugins

## Event Trigger Plugin
> available source type: kafka

### To use kafka plugin
> install requires: <br/>
`airflow`<br/>
`croniter`<br/>
`python-dateutil`<br/>
`confluent-kafka`<br/>
`jinja`<br>
`sqlalchemy`<br>

### Kafka Consumer Plugin
> available match topic: frontier_adw, hippo_finish
* how to use in DAG
```python
from airflow.operators.kafka_plugin import KafkaConsumerOperator

kafka_msgs = [
    {'frequency': 'D', 'topic': 'etl-finish', 'db': 'db0', 'table': 'table0', 'partition_values': "", 'task_id': "tbla"},
    {'frequency': 'D', 'topic': 'etl-finish', 'db': 'db1', 'table': 'table1', 'partition_values': "{{yyyymm|dt.format(format='%Y%m')}}", 'task_id': "tblb"}
]

my_consumer = KafkaConsumerOperator(
    task_id='my_consumer',
    sensor_name=Optional[string], # use dag_id.task_id if not given, it's sensor identification in db.
    source_type='kafka',
    broker='localhost:9092',
    group_id='test',
    client_id='test',
    msgs=kafka_msgs,
    poke_interval=10,
    timeout=60,
    mark_success=True,
    soft_fail=False,
    mode='reschedule',
    debug_mode=False,
    session=Optional[Session]  # given if not using airflow db to store sensor status
)

# task_id should be same as value in kafka_msgs
A = DummyOperator(task_id='tbla')
B = DummyOperator(task_id='tblb')

my_consumer >> [A, B]
```

### Kafka Producer Plugin
1. KafkaProducerOperator: produce messages to one topic
2. KafkaProducerFromFileOperator: produce messages to topics depends on file content

* how to use in DAG - KafkaProducerOperator
```python
from airflow.operators.kafka_plugin import KafkaProducerOperator

# produce 'test' message to job-finish topic
send = KafkaProducerOperator(
    task_id='send',
    broker='localhost:9092',
    topic='job-finish',
    data=['test']           # list of messages to produce
)
```
* how to use in DAG - KafkaProducerFromFileOperator
```python
from airflow.operators.kafka_plugin import KafkaProducerFromFileOperator

# file format
'''
{"topic": "finish", "data": ["finish"]}
{"topic": "test", "data": ["test1", "test2"]}
'''

# read file and produce messages to topics
send_from_file = KafkaProducerFromFileOperator(
    task_id='send_from_file',
    broker='localhost:9092',
    file_list=[file_path1, file_path2]  # file list which record topic and messages to produce
)
```

* how to use in DAG - KafkaProducerFromMergeFileOperator
> TODO: the merging method of each (key, value) in messages is hard-coded in the class.
```python
from airflow.operators.kafka_plugin import KafkaProducerFromMergeFileOperator

# match_dict defines multiple rules to find messages in specific topic that match
# messages in each group would be merged into one message, which is produced to kafka later
match_dict = {
    'HiveSinkFinish': [{
        # group 1: produce 1 message to represent that the table is done
        'db': 'db1',
        'table': 'table1',
        'partition_fields': 'exec_group'}, {
        # group 2: produce 1 message to represent that the table is done
        'db': 'db2',
        'table': 'table2',
        'partition_fields': 'timestamp/exec_group'
    }],
    ...
}

# read file and produce messages to topics
send_from_merge_file = KafkaProducerFromMergeFileOperator(
    task_id='send_from_merge_file',
    broker='localhost:9092',
    match_dict=match_dict,
    file_list=[file_path1, file_path2]
)
```

### Kafka Status Email plugin
* how to use in DAG
```python
from airflow.operators.kafka_plugin import KafkaStatusEmailOperator

# read sensor status from status path and send email
send_email = KafkaStatusEmailOperator(
    task_id="send_status_mail",
    to="<your email>",
    status_path="<your path to store sensor status>"
)
```

### TODO
* move usage of each plugin to corresponding folder
* add kafka integration test with DAG
* add images for detailed introduction

### How to run test
> test requires: <br/>
`pytest`<br/>
`mock`<br/>
`pytest-mock`<br/>
`pytest-cov`<br/>
```
./run_test.sh
```
* It's available to add test arguments
```python
# show detail
./run_test.sh -vvv
# show coverage in console
./run_test.sh --cov-config=.coveragerc --cov=./
```

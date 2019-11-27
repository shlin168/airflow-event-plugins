# Kafka Producer Plugin
1. KafkaProducerOperator: produce messages to one topic
2. KafkaProducerFromFileOperator: produce messages to topics depends on file content
3. KafkaProducerFromMergeFileOperator: produce messages from files but merge content in file before producing

## KafkaProducerOperator
### Usage in DAG
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

## KafkaProducerFromFileOperator
### Usage in DAG
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

## KafkaProducerFromMergeFileOperator
### Usage in DAG
> TODO: the merging method of each (key, value) in messages is hard-coded in the class so far.
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
        'partition_fields': 'exec_date/exec_group'
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
# Kafka Consumer Plugin
Available match topic:
* etl-finish
* job-finish

Note: you can define your own topic and corresponding match method

## Usage in DAG
```python
from airflow.operators.kafka_plugin import KafkaConsumerOperator

kafka_msgs = [
    {'frequency': 'D', 'topic': 'etl-finish', 'db': 'db0', 'table': 'table0', 'partition_values': "", 'task_id': "tbla"},
    {'frequency': 'D', 'topic': 'etl-finish', 'db': 'db1', 'table': 'table1', 'partition_values': "{{yyyymm|dt.format(format='%Y%m')}}", 'task_id': "tblb"},
    {'frequency': 'D', 'topic': 'job-finish', 'job_name': 'jn1', 'task_id': "joba"}
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
C = DummyOperator(task_id='joba')

my_task = BashOperator(
    task_id='my_task',
    bash_command='echo task: "run_id={{ run_id }} | dag_run={{ dag_run }}"',
)

my_consumer >> [A, B, C]
[A, B, C] >> my_task
```

## How DAG with above code looks like
```
                      ╒═════════╕
                  +---|  tba a  |---+
                  |   | (dummy) |   |
                  |   ╘═════════╛   |
                  |                 |
╒═════════════╕   |   ╒═════════╕   |   ╒═════════╕
| my_consumer |---+---|  tba b  |---+---| my_task |
| (sensor)    |   |   | (dummy) |   |   | (bash)  |
╘═════════════╛   |   ╘═════════╛   |   ╘═════════╛
                  |                 |
                  |   ╒═════════╕   |
                  +---|  job a  |---+
                      | (dummy) |
                      ╘═════════╛
```

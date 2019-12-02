# How to test example DAG

1. Set plugins<br/>
Copy [`event_plugins`](plugins/) into `$AIRFLOW_HOME/plugins` folder.

2. Set example DAG<br/>
Copy `kafka_event_plugin.py` into `$AIRFLOW_HOME/dags` folder and modify.
    1. Set timezone to local timezone
    ```python
    local_tz = pendulum.timezone("<local timezone>")
    ```
    2. Set kafka broker
    ```python
    broker = 'localhost:9092'
    ```
    3. Change `<email address>` in `KafkaStatusEmailOperator`
    > also need to set `smtp` server in `$AIRFLOW_HOME/airflow.cfg`
    ```python
    send_status_email = KafkaStatusEmailOperator(
        task_id="send_status_mail",
        sensor_name="kafka_trigger_test.my_consumer",
        to="<email address>",
        trigger_rule=TriggerRule.ALL_FAILED
    )
    ```

3. Create `etl-finish` and `job-finish` topic in kafka (or modify value of `topic` field within wanted messages in `kafka_event_plugin.py`)

4. Set config for event plugins<br/>
    1. Copy [`event_plugins/common/storage/default.cfg`](../plugins/event_plugins/common/storage/default.cfg) to your config folder and modify the setting if needed. e.g., set `create_table_if_not_exist = True` if you want event_plugins to automatically create table if not exists.
    2. Set `AIRFLOW_EVENT_PLUGINS_CONFIG` environment variable to the location of config. (Or directly modify `default.cfg` which is not recommended)
    ```shell
    export AIRFLOW_EVENT_PLUGINS_CONFIG=<your config>
    ```

5. Start airflow services. DAG should look like picture below but without status.

![](../images/ExampleDagSuccess.png)

6. Since `schedule_interval` is `None` in example DAG, manually trigger DAG from UI.

7. Modify and send test messages.
    1. Change value of `timestamp` and `partition_values` to the day of execution
    2. Produce the messages to kafka, check if status of task in DAG changed.

```json
# topic: etl-finish
{"db": "db0", "table": "table0", "partition_fields": "", "partition_values": "", "timestamp": 1575190675}
{"db": "db1", "table": "table1", "partition_fields": "yyyymm", "partition_values": "201911", "timestamp": 1575190675}

# topic: job-finish
{"job_name": "jn0", "is_success": true, "duration": 10, "timestamp": 1575190675}
```

Note: if there're messages unreceived or not matched. It may look like picture below, and mail will be sent to the address.
![](../images/ExampleDagFailed.png)

### More information about the sensor
Check the execution workflow in [KafkaComsumerOperator](../docs/kafka_consumer.md#DAG-flow-example)
```python
# kafka sensor
my_consumer = KafkaConsumerOperator(
    # the name displayed in UI
    task_id='my_consumer',

    # use dag_id.task_id if not given, should be unique in database
    sensor_name="kafka_trigger_test.my_consumer",

    # parameters for kafka
    broker=broker,
    group_id='airflow-test',
    client_id='airflow-test',

    # wanted messages
    msgs=kafka_msgs,

    # the frequency to start kafka consumer and listening for messages
    poke_interval=10,

    # ttl in seconds, if not receiving messages, it would timeout and mark the task failed. crontab string is also available. e.g., "0 22 * * *" for daily triggered DAG
    timeout=120,

    # marking dummy tasks success or not
    mark_success=True,

    # skip the task without fail if set to True, check airflow sensor for more information
    soft_fail=False,
    # there's poke and reschedule mode for sensor, check airflow sensor for more information
    mode='reschedule',

    # print the db status or not
    debug_mode=True,
)
```

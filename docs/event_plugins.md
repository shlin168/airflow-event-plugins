# Event Plugins
## Introduction

First of all, we need to know that Airflow aims for batch processing, not event-based processing. However, most of the time we need the events to trigger our jobs.

### Event Message's type
Event Messages might be a HTTP request, kafka message, SQS, ...

### Event-based Airflow task
> Definition: The airflow task waits for single or multiple events message.

We all know that there's `sensor` in airflow that can be used for handling event. The problem is, if using airflow sensors for each event message like below picture. (`msg X` will success when receiving the message it needs, else timeout after configured seconds.) There would be lots of sensors **waiting most of the time** and occupy worker slots.
> Imagine that there's a task that need 20 events..., and we usally define lots of DAGs in AIRFLOW.

Set `mode:reschedule` in operator combine with putting all the sensors to defined `pool` to control max occupied worker slots can be a solution but there're still some way to enhance from how we contruct a DAG.
```
╒═════════╕
|  msg 1  |---+
╘═════════╛   |
              |
╒═════════╕   |   ╒════════╕
|  msg 2  |---+---|  task  |--> other tasks
╘═════════╛   |   ╘════════╛
              |
╒═════════╕   |
|  msg 3  |---+
╘═════════╛
```

### Solution 1: One sensor success when received multiple events
We develop a sensor that will listen to 3 messages. All of them is received and the sensor will be marked to `SUCCESS` and move on to the next task.
It's simple and intuitive. However, we don't know which msg haven't received when we open the DAG from UI. We can only access the log to check, which is not convenient.
```
╒═══════════╕   ╒════════╕
| msg 1+2+3 |---|  task  |--> other tasks
╘═══════════╛   ╘════════╛
```

### Solution 2: One sensor with dummy tasks to show event status
Now we develop a sensor that will listen to 3 messages, and with 3 dummy task behind. It the sensor get `msg 1`, the `msg 1 dummy task` would be **marked to success from airflow db**, which can show the status from the UI, same as others. After 3 dummy task is all marked to SUCCESS, the task would be executed since it's dependency met.
```
                    ╒═════════╕
                +---|  msg 1  |---+
                |   | (dummy) |   |
                |   ╘═════════╛   |
                |                 |
╒═══════════╕   |   ╒═════════╕   |   ╒═════════╕
| msg 1+2+3 |---+---|  msg 2  |---+---| task A  |--> other tasks
| (sensor)  |   |   | (dummy) |   |   |         |
╘═══════════╛   |   ╘═════════╛   |   ╘═════════╛
                |                 |
                |   ╒═════════╕   |
                +---|  msg 3  |---+
                    | (dummy) |
                    ╘═════════╛
```

#### scheduler concept
In Airflow, scheduler's job can be devided into two parts:
1. DagFileProcessorAgent: Parse the DAGs in `$AIRFLOW_HOME/dags`, create dat_runs and task instances and save them into database (set `STATUS=schedule` if dependencies met).
2. Scheduling: Continuously harvest DAG parsing result and find task instances with `STATUS=schedule`. It conditions met(there're some conditions such as queue isn't full, `pool` isn't full, ...), set status to `queue` and put [`airflow run <dag> <task> <execution_date> ...`](https://airflow.apache.org/cli.html#run) command to queue for executors to run tasks.

#### dependency
If we mark `msg X` directly in database to SUCCESS, the dependency of `task A` will met for scheduler. Scheduler won't find that `msg X(dummy)` is manually marked SUCCESS without execution.
> The dependencies between `msg(sensor)` and `msg X(dummy)` do not totally follow the dependency rules in Airflow. It is designed to let user know which event hasn't happened when checking DAG from UI.

#### timeout
[`event_plugins/common/schedule/timeout.py`](../plugins/event_plugins/common/schedule/timeout.py)

There would be a `timeout` parameter for `msg(sensor)`. If it didn't get all events before timeout, the `msg(sensor)` would be marked FAIDED. All `msg X(dummy)` that `Status=None` would be marked failed (it's controlled by [`trigger_rule`](http://airflow.apache.org/concepts.html#trigger-rules) in Airflow).
> Notice: if `msg X(dummy)` have marked SUCCESS before timeout, the status won't change even it's parent `msg(sensor)` is failed. So we can know that `msg X(sensor)` failed due to which event base on status of every `msg X(dummy)` task.

#### storage
[`event_plugins/common/storage/event_message.py`](../plugins/event_plugins/common/storage/event_message.py)

All the event message would be stored in database. It uses the same database with airflow if not specified `session` parameter in operator. (table name: `event_plugins`)
> `KafkaStatusEmailOperator` will send mail with below table to show the status of each event message

| id | name | msg             | source_type | frequency | last_receive    | last_receive_time   | timeout             |
|----|------|-----------------|-------------|-----------|-----------------|---------------------|---------------------|
| 1  | test | {"table": "abc"} | kafka       | D         | None            | None                | 2019-06-15 23:59:59 |
| 2  | test | {"table": "def"} | kafka       | D         | {"test": "def"} | 2019-06-15 08:00:00 | 2019-06-15 23:59:59 |
| 3  | test | {"job": "ghi"}   | kafka       | M         | None            | None                | 2019-06-30 23:59:59 |

Column description:
* `id(int)`: primary key, it's an auto increment integer. Do not need to provide.
* `name(string)`: should be unique for every sensors within airflow.
* `msg(string)`: the message that the sensors listen to. It's kafka message in `KafkaConsumerOperator`. we use json type of string to store the information. Check [`KafkaConsumerOperator`](kafka_consumer.md) for the example.
* `source_type(string)`: only `kafka` now, but add this column for flexibility.
* `frequency(string)`: the frequency of the event happening. `D` means `Day` and `M` means `Month`.
* `last_receive(json)`: the last event message received
* `last_receive_time(datetime)`: the last time event message received
* `timeout(datetime)`: the time that the value in `last_receive` column would be expired and should be removed.

Note: We used to use `shelve db` to store event records for each `KafkaConsumerOperator`. However, if using `CeleryExecutor` with Celery workers (multiple machines), the task might be executed in different machine each time. It's not feasible to store the status in local files. So we change to use `sqlalchemy` to store and manipulate the records in database, just like how airflow control the status of tasks in DAGs.

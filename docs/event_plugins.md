# Event Plugins
## Introduction

First of all, we need to know that Airflow aims for batch processing, not event-based processing. However, most of the time we need the events to trigger our jobs.

### Event Message's type
Event Messages might be a HTTP request, kafka message, SQS, ...

### Event-based Airflow task
> Definition: The airflow task waits for single or multiple events message.

We all know that there's `sensor` that can be used for handling incoming message. The problem is, if using airflow sensors for each event message like below picture. (msg X is  airflow sensor that will success if getting message if needs, else timeout after configured seconds.) There would be lots of sensors **waiting most of the time** and occupy worker slots.
> Imagine that there's a task that need 20 events..., and we usally define lots of DAGs in AIRFLOW.

Set `mode:reschedule` in operator combine with putting all the sensors to defined `pool` to control max occupied worker slots can be a solution but there're still some way to enhance the DAG.
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

### Solution 1: One Sensor success with multiple Events
We develop a sensor that will listen to 3 messages. All of them is received and the sensor will turn to `SUCCESS` status and move on to the next task.
It's simple and intuitive. However, we don't know which msg haven't received when we open the DAG from UI. We can only access the log to check, which is not convenient.
```
╒═══════════╕   ╒════════╕
| msg 1+2+3 |---|  task  |--> other tasks
╘═══════════╛   ╘════════╛
```

### Solution 2: One Sensor with dummy tasks to show event status
Now we develop a sensor that will listen to 3 messages, and with 3 dummy task behind. It the sensor get `msg 1`, the `msg 1 dummy task` would be **marked to success from airflow db**, which can show the status from the UI.
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
2. Scheduling: Continuously harvest DAG parsing result and find task instances with `STATUS=schedule`. It conditions met(queue isn't full, `pool` isn't full, ...), set status to `queue` and put [`airflow run <dag> <task> <execution_date>...`](https://airflow.apache.org/cli.html#run) command to queue for executors to run tasks.
#### dependency
If we mark `msg X` directly in database to SUCCESS, the dependency of `task A` will met for scheduler. Scheduler won't find that `msg X(dummy)` is manually marked SUCCESS without execution.
> The dependencies between `msg(sensor)` and `msg X(dummy)` do not follow the dependency rules in Airflow. It just let user know which event hasn't happened when checking DAG from UI.
#### timeout
There would be a `timeout` parameter for `msg(sensor)`. If it didn't get all events before timeout, the `msg(sensor)` would be marked FAIDED. All `msg X(dummy)` that `Status=None` would be marked failed (it's controlled by [`trigger_rule`](http://airflow.apache.org/concepts.html#trigger-rules) in Airflow).
> Notice: if `msg X(dummy)` have been received before timeout, the status would be set to `True`, and it won't be changed even it's parent `msg(sensor)` is failed. So we can know that `msg X(sensor)` failed due to which event base on status of every `msg X(dummy)` task.

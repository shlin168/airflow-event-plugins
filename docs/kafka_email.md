# Kafka Status Email plugin
It's a combination tool for [kafka_consumer_plugin](kafka_consumer.md). It will query the database and send email to show the status of each event.


## Usage in DAG
```python
from airflow.operators.kafka_plugin import KafkaStatusEmailOperator

# read sensor status from database with sensor_name and send email
send_email = KafkaStatusEmailOperator(
    task_id="send_status_mail",
    to="<target email address>",
    sensor_name="<sensor that you want to mail the status>"
)
```

## Result
The email example is like picture below. It records the DAG information, unreceived messages with corresponding kafka topics and the db status of sensor(given sensor_name).
![](../images/EventStatusEmailExample.png)

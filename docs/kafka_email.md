# Kafka Status Email plugin
It's a combination tool for [kafka_consumer_plugin](kafka_consumer.md). It will query the database and send email to show the status of each event.


## Usage in DAG
```python
from airflow.operators.kafka_plugin import KafkaStatusEmailOperator

# read sensor status from database with sensor_name and send email
send_email = KafkaStatusEmailOperator(
    task_id="send_status_mail",
    to="<your email>",
    sensor_name="<sensor that you want to mail the status>"
)
```
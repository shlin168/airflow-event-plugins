from airflow.plugins_manager import AirflowPlugin

from event_plugins.kafka.kafka_consumer_plugin import KafkaConsumerOperator
from event_plugins.kafka.kafka_producer_plugin import KafkaProducerOperator
from event_plugins.kafka.kafka_producer_plugin import KafkaProducerFromFileOperator
from event_plugins.kafka.kafka_producer_plugin import KafkaProducerFromMergeFileOperator
from event_plugins.kafka.kafka_email_plugin import KafkaStatusEmailOperator


class KafkaPlugin(AirflowPlugin):
    name = "kafka_plugin"
    operators = [KafkaProducerOperator, KafkaConsumerOperator,
                 KafkaStatusEmailOperator, KafkaProducerFromFileOperator,
                 KafkaProducerFromMergeFileOperator]
    sensors = []

import logging, os
from kafka import KafkaConsumer
from utils import get_logger
import dotenv

logger = get_logger()

consumer = KafkaConsumer(
    os.getenv("TOPIC_NAME"),
    bootstrap_servers=f'{os.getenv("KAFKA_ADDRESS")}:{os.getenv("KAFKA_PORT")}',
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="my-group",
    value_deserializer=lambda v: v.decode("utf-8"),
    key_deserializer=lambda k: k.decode("utf-8") if k else None
)

logger.info(f"Kafka consumer started. Listening on {os.getenv('TOPIC_NAME')}...")

for msg in consumer:
    logger.info(
        f"Consumed | Topic={msg.topic}, "
        f"Partition={msg.partition}, "
        f"Offset={msg.offset}, "
        f"Key={msg.key}, "
        f"Value={msg.value}"
    )

from kafka import KafkaProducer
import os, sys

producer = KafkaProducer(
    bootstrap_servers=f'{os.getenv("KAFKA_ADDRESS")}:{os.getenv("KAFKA_PORT")}',
    value_serializer=lambda v: v.encode('utf-8')
)

for i in range(5):
    print(f"{i}")
    producer.send(os.getenv("TOPIC_NAME"), f'hello-{i}')

producer.flush()
print("Done")

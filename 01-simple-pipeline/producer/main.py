from kafka import KafkaProducer
import os, sys

if __name__ == '__main__':
    try:
        msg, num = sys.argv[1], int(sys.argv[2])
    except IndexError:
        msg, num = f'hello', 10
    producer = KafkaProducer(
        bootstrap_servers=f'{os.getenv("KAFKA_ADDRESS")}:{os.getenv("KAFKA_PORT")}',
        value_serializer=lambda v: v.encode('utf-8')
    )

    for i in range(num):
        producer.send(os.getenv("TOPIC_NAME"), msg)

    producer.flush()
    print("Done")

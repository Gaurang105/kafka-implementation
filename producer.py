import random
import time
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

while True:
    key = f"{random.randrange(999)}".encode()
    value = f'{{"URL":"URL{random.randrange(999)}"}}'.encode()
    producer.produce('pageview', key=key, value=value, callback=delivery_report)
    producer.poll(0)
    time.sleep(1)

producer.flush()
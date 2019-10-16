from time import sleep
from json import dumps

from kafka import KafkaProducer

from log_gen import generate_log_line

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
    log = generate_log_line()
    data = {'log': log}
    producer.send('user_log', value=data)
    sleep(3)


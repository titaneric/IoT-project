from time import sleep
from json import dumps

from kafka import KafkaProducer

from gen_log import generate_log_line

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: x.encode('utf-8'))

while True:
    log = generate_log_line()
    print(log)
    producer.send('user_log', value=log)
    sleep(3)


#! /home/chenyee/anaconda3/bin/python
import random
from time import sleep
from json import dumps
import sys

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: x.encode('utf-8'))

for log in sys.stdin:
    print(log)
    producer.send('user_log', value=log)


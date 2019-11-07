#! /home/chenyee/anaconda3/bin/python
import random
from time import sleep
from json import dumps
import sys

#from kafka import KafkaProducer

from gen_log import generate_log_line

#producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: x.encode('utf-8'))

cnt = 1
while True:
    log = generate_log_line()
    key = "{}".format(cnt).encode('utf-8')
    sys.stdout.write(log + "\n")
    # print(log)
    sys.stdout.flush()
    #producer.send('user_log', value=log, key=key)
    cnt += 1
    sleep_sec = random.randint(1, 10)
    sleep(sleep_sec)


from json import loads
from kafka import KafkaConsumer

consumer = KafkaConsumer(
        'user_log',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
        )

for msg in consumer:
    message = msg.value
    log = message.split(',')
    print(log[0], log[1])

from json import loads, dumps

from kafka import KafkaConsumer, KafkaProducer

line = (
    "receive time,type,source ip,from port,dest ip,to port,"
    "application,action,session end,byte receive,byte send,"
    "ip protocol,packet receive,packet send,start time"
)

consumer = KafkaConsumer(
    'user_log',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8')
)

# split_producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

line = list(map(lambda x: x.replace(' ', '_'), line.split(',')))
for msg in consumer:
    message = msg.value
    log_dict = dict(zip(line, message.split(',')))
    print(log_dict['start_time'], log_dict['source_ip'])
    # split_producer.send('log_split', value=log_dict)


## Installation

### Install Anaconda

https://docs.anaconda.com/anaconda/install/linux/

### Install Confluent

https://docs.confluent.io/current/quickstart/ce-quickstart.html

Add to path by editing ~/.bashrc
```bash
export CONFLUENT_HOME=/home/user/confluent-xxx
export PATH=$PATH:$CONFLUENT_HOME/bin
```

Start all processes
```bash
confluent local start
```
### Install python libraries

```bash
pip install pyspark kafka-python
```

### Confluent Control Center

- Monitor the topic
- Status of Connector
- Topic create, delete and set retention
- KSQL

Navigate browser to http://127.0.0.1:9021

Create topics:
- user_log
- traffic_log
- threat_log
- traffic_windowed_appearance
- threat_windowed_appearance

## Execution

Connect to port and send to kafka topic
```bash
sudo python socket_client.py
```

Consume topic and calculate the windowed group appearance
```bash
spark-submit --packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,\org.apache.spark:spark-avro_2.12:3.0.0 structured_stream.py --type traffic
```
```bash
spark-submit --packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,\org.apache.spark:spark-avro_2.12:3.0.0 structured_stream.py --type threat
```

## Install Connector

### Install confluent-hub

https://docs.confluent.io/current/connect/managing/confluent-hub/client.html

```bash
confluent-hub install mongodb/kafka-connect-mongodb:latest
```

### Start connector

```bash
curl -X PUT http://localhost:8083/connectors/sink-mongodb-traffic/config -H "Content-Type: application/json" -d ' {
      "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
      "tasks.max":"1",
      "topics":"traffic_windowed_appearance",
      "connection.uri":"mongodb://localhost:27017",
      "database":"log",
      "collection":"traffic_windowed_appearance",
      "key.converter":"org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter":"org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
}' 
```

```bash
curl -X PUT http://localhost:8083/connectors/sink-mongodb-threat/config -H "Content-Type: application/json" -d ' {
      "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
      "tasks.max":"1",
      "topics":"threat_windowed_appearance",
      "connection.uri":"mongodb://localhost:27017",
      "database":"log",
      "collection":"threat_windowed_appearance",
      "key.converter":"org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter":"org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
}' 
```

```bash
curl -X PUT http://localhost:8083/connectors/sink-mongodb-severity/config -H "Content-Type: application/json" -d ' {
      "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
      "tasks.max":"1",
      "topics":"threat_log",
      "connection.uri":"mongodb://localhost:27017",
      "database":"log",
      "collection":"threat_log",
      "key.converter":"org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter":"org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
}' 
```

### Submit cronjob
```
crontab vector.crontab
```
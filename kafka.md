## Reference

[Confluent](https://docs.confluent.io/current/quickstart/cos-docker-quickstart.html#cos-docker-quickstart)

## Start Kafka, Zookeeper and KSQL

```bash
git clone https://github.com/confluentinc/examples
cd examples
git checkout 5.3.1-post
```

```
cd cp-all-in-one/
```

```
docker-compose up -d --build
```


## Docker Image Installation

https://docs.confluent.io/3.2.2/installation/docker/docs/quickstart.html

```bash
```
## Create Topic

```bash
docker-compose exec broker kafka-topics --create --zookeeper \
zookeeper:2181 --replication-factor 1 --partitions 1 --topic pageviews
```

## Start KSQL CLI

```bash
docker-compose exec ksql-cli ksql http://ksql-server:8088
```

## User Log Stream

```bash
CREATE STREAM logtest (receive_time BIGINT, type VARCHAR, \
source_ip VARCHAR, from_port INT, dest_ip VARCHAR, to_port INT, \
application VARCHAR, action VARCHAR, session_end VARCHAR, \
byte_receive INT, byte_send INT, ip_protocol VARCHAR, \
packet_receive INT, packet_send INT, start_time BIGINT) \
WITH (KAFKA_TOPIC='user_log', VALUE_FORMAT='DELIMITED', TIMESTAMP='start_time');
```

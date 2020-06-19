## Reference

[Confluent](https://docs.confluent.io/current/quickstart/cos-docker-quickstart.html#cos-docker-quickstart)
[Troubleshooting](https://www.confluent.io/blog/troubleshooting-ksql-part-1)
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
sudo docker-compose exec broker kafka-topics --create --zookeeper \
zookeeper:2181 --replication-factor 1 --partitions 1 --topic user_log
```

```bash
sudo docker-compose exec broker kafka-topics --create --zookeeper \
zookeeper:2181 --replication-factor 1 --partitions 1 --topic windowed_appearance
```

## Clean Topic

```bash
sudo docker-compose exec broker kafka-topics --zookeeper \
zookeeper:2181 --delete --topic someTopic
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

## Kafka Console Consumer

```bash
sudo docker run \
  --net=host \
  --rm \
  confluentinc/cp-kafka:3.2.2 \
  kafka-console-consumer --bootstrap-server localhost:9092 --topic user_log --new-consumer --from-beginning --max-messages 5
```

## Check Kafka logs

https://docs.confluent.io/current/ksql/docs/troubleshoot-ksql.html#ksql-check-server-logs

```bash
docker-compose ps -q # get container id
docker logs <container-id>
docker-compose logs ksql-server
```

## Submit spark job

```
pip instal pyspark
```

https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#deploying

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 structured_stream.py
```
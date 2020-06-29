## Reference

[Confluent](https://docs.confluent.io/current/quickstart/cos-docker-quickstart.html#cos-docker-quickstart)
[Troubleshooting](https://www.confluent.io/blog/troubleshooting-ksql-part-1)
## Start Kafka, Zookeeper and KSQL

```bash
git clone https://github.com/confluentinc/examples
cd examples
git checkout 5.5.0-post
```

```
cd cp-all-in-one/
```

```
docker-compose up -d --build
```

## Refs

- https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
- https://spark.apache.org/docs/latest/api/python/pyspark.sql.html
- https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types
- https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions
- https://docs.confluent.io/current/app-development/kafkacat-usage.html
- https://www.confluent.io/stream-processing-cookbook/ksql-recipes/changing-data-serialization-format-delimited-csv-avro/
- https://www.confluent.io/blog/troubleshooting-ksql-part-1/
- https://docs.confluent.io/current/schema-registry/schema_registry_tutorial.html
- https://spark.apache.org/docs/latest/sql-data-sources-avro.html#load-and-save-functions

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
## Purge Topic

Wait some time to take effect

```bash
kafka-configs --bootstrap-server 127.0.0.1:9092 --entity-type topics --alter --entity-name <topic name> --add-config retention.ms=1000
```

## Delete Topic

```bash
sudo docker-compose exec broker kafka-topics --zookeeper \
zookeeper:2181 --delete --topic someTopic
```

## Start KSQL CLI

weird

```bash
docker-compose exec ksqldb-cli ksql http://ksql-server:8088
```

## User Log Stream

```bash
CREATE STREAM TRAFFIC_LOG (type VARCHAR, threat/content_type VARCHAR, \
source_address VARCHAR, destination_address VARCHAR, application VARCHAR, source_port INT, \
application VARCHAR, action VARCHAR, session_end VARCHAR, \
byte_receive INT, byte_send INT, ip_protocol VARCHAR, \
packet_receive INT, packet_send INT, start_time BIGINT) \
WITH (KAFKA_TOPIC='traffic_log', VALUE_FORMAT='DELIMITED', TIMESTAMP='start_time');
```

## Kafka Console Consumer

```bash
sudo docker run \
  --net=host \
  --rm \
  confluentinc/cp-kafka:3.2.2 \
  kafka-console-consumer --bootstrap-server localhost:9092 --topic threat_log --from-beginning --max-messages 5 
```

## Check Kafka logs

https://docs.confluent.io/current/ksql/docs/troubleshoot-ksql.html#ksql-check-server-logs

```bash
docker-compose ps -q # get container id
docker logs <container-id>
docker-compose logs ksql-server
```

## Kafkacat debug

```bash
docker run --rm solsson/kafkacat \
      -b localhost:9092 \
      -t traffic_log -C \
      -f '\nKey (%K bytes): %k
  Value (%S bytes): %s
  Timestamp: %T
  Partition: %p
  Offset: %o
  Headers: %h\n'
```

## Log Producer

```
sudo python socket_client.py
```

## Submit spark job

```
pip instal pyspark kafka-python
```

https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#deploying

```
spark-submit --packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,\org.apache.spark:spark-avro_2.12:3.0.0 structured_stream.py
```

## Debug Spark

```
df.show()

logs.printSchema()
# logs.awaitTermination()

logs = logs.writeStream\
    .format("console")\
    .option('truncate', 'false')\
    .trigger(once=True) \
    .start()
```
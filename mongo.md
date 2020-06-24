## Dependecies

```bash
pip install pymongo
```

## Refs

- https://www.mongodb.com/blog/post/getting-started-with-the-mongodb-connector-for-apache-kafka-and-mongodb-atlas
- https://docs.confluent.io/current/connect/userguide.html#installing-plugins
- https://docs.confluent.io/current/connect/managing/extending.html
- https://docs.confluent.io/5.0.0/installation/docker/docs/installation/connect-avro-jdbc.html
- https://docs.confluent.io/current/installation/docker/config-reference.html
- https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/
- https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/
- https://docs.confluent.io/current/schema-registry/connect.html
- https://docs.mongodb.com/kafka-connector/master/kafka-sink/

## Schema-registry

- https://docs.confluent.io/3.0.0/schema-registry/docs/intro.html

```
pip install confluent-kafka
```

## Connect to Kafka

build the dockerfile for installing connector

```bash 
docker build . -t my-connector

docker run -d -it my-connector
```

### Use MongoDB as a source

```bash
curl -X PUT http://localhost:8083/connectors/source-mongodb-inventory/config -H "Content-Type: application/json" -d '{
      "tasks.max":1,
      "connector.class":"com.mongodb.kafka.connect.MongoSourceConnector",
      "key.converter":"org.apache.kafka.connect.storage.StringConverter",
      "value.converter":"org.apache.kafka.connect.storage.StringConverter",
      "connection.uri":"mongodb://localhost:27017",
      "database":"log",
      "collection":"traffic_windowed_appearance",
      "pipeline":"[{\"$match\": { \"$and\": [ { \"updateDescription.updatedFields.quantity\" : { \"$lte\": 5 } }, {\"operationType\": \"update\"}]}}]", 
      "topic.prefix": ""
}'
```

### Use MongoDB as a sink

Success!!

```bash
curl -X PUT http://localhost:8083/connectors/sink-mongodb-users/config -H "Content-Type: application/json" -d ' {
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

## Exec

- https://docs.mongodb.com/manual/tutorial/write-scripts-for-the-mongo-shell/

```
mongo vector.js
```

## array create

-https://docs.mongodb.com/manual/reference/operator/aggregation/bucket/

## TTL

- https://docs.mongodb.com/manual/tutorial/expire-data/


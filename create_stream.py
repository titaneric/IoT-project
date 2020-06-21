import json

from pyspark.sql import types as T
from ksql import KSQLAPI

from structured_stream import log_type_dict, datetime_convert

client = KSQLAPI('http://localhost:8088')

type_mapping = {
    T.StringType: "varchar",
    T.IntegerType: "int",
    datetime_convert: "bigint"
}
avro_type_mapping = {
    T.StringType: "string",
    T.IntegerType: "int",
    datetime_convert: "date"
}
base_schema = {
    "fields": [],
    "name": "",
    "namespace": "",
    "type": "record"
}


def create_stream():
    for log_type in ["traffic", "threat"]:
        header = log_type_dict[log_type]["header"]
        indices = log_type_dict[log_type]["indices"]
        types = log_type_dict[log_type]["types"]
        topic = log_type_dict[log_type]["topic"]
        selected_header = [header[i] for i in indices]

        # Add backtick ` to prevent the `extraneous input '/'` error
        # source from https://markhneedham.com/blog/2019/05/20/kql-create-stream-extraneous-input/
        columns_type = []
        for column, type_name in zip(selected_header, types):
            if column == "threat/content_type":
                column = "content_type"

            attr_type = type_mapping[type_name]
            columns_type.append(f"{column} {attr_type}")

        print(columns_type)
        client.create_stream(table_name=f"{topic}_delimited",
                             columns_type=columns_type,
                             topic=topic,
                             value_format="DELIMITED")

        """ Now back to confluent control center localhost:9091 to convert to avro type
        at ksqlDB -> Editor
        Source from https://www.confluent.io/stream-processing-cookbook/ksql-recipes/changing-data-serialization-format-delimited-csv-avro/

        CREATE STREAM traffic_log_avro WITH (KAFKA_TOPIC='traffic_log_avro',VALUE_FORMAT='AVRO') AS SELECT * FROM traffic_log_delimited;

        CREATE STREAM threat_log_avro WITH (KAFKA_TOPIC='threat_log_avro',VALUE_FORMAT='AVRO') AS SELECT * FROM threat_log_delimited;

         """


def dump_schema(value_schema, key_schema):
    for schema in [key_schema, value_schema]:
        with open(f"schemas/{schema['name']}.avsc", "w") as f:
            json.dump(schema, f, indent=4)


def dump_log_avro_schema():
    for log_type in ["traffic", "threat"]:
        header = log_type_dict[log_type]["header"]
        indices = log_type_dict[log_type]["indices"]
        topic = log_type_dict[log_type]["topic"]
        types = log_type_dict[log_type]["types"] + \
            [T.StringType, T.IntegerType]
        selected_header = [header[i]
                           for i in indices] + ["nctu_address", "appearance"]

        key_schema = base_schema.copy()
        key_schema["name"] = f"key_{topic}"
        key_schema["fields"] = [{"name": "nctu_address", "type": "string"}]

        value_schema = base_schema.copy()
        value_schema["name"] = f"value_{topic}"
        fields = []
        for column, type_name in zip(selected_header, types):
            attr_type = avro_type_mapping[type_name]
            if attr_type == "date":
                attr = {
                    "name": column,
                    "type": "int",
                    "logicalType": attr_type,
                }
            else:
                attr = {
                    "name": column,
                    "type": attr_type
                }
            fields.append(attr)

        value_schema["fields"] = fields
        dump_schema(value_schema, key_schema)


def dump_agg_avro_schema():
    base_schema = {
        "fields": [],
        "name": "",
        "namespace": "",
        "type": "record"
    }
    for log_type in ["traffic", "threat"]:
        features = log_type_dict[log_type]["features"]
        agg_topic = log_type_dict[log_type]["agg_topic"]

        key_schema = base_schema.copy()
        key_schema["name"] = f"key_{agg_topic}"
        key_schema["fields"] = [
            {
                "name": "nctu_address",
                "type": "string"
            },
            {
                "type": "int",
                "name": "window",
                "logicalType": "date",
            }]

        value_schema = base_schema.copy()
        value_schema["name"] = f"value_{agg_topic}"
        value_schema["fields"] = [{
            "name": column,
            "type": "int"
        } for column in features]
        dump_schema(value_schema, key_schema)


if __name__ == "__main__":
    dump_log_avro_schema()
    dump_agg_avro_schema()

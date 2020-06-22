from functools import partial

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.avro.functions import from_avro, to_avro

from config import debug

spark_datetime_format = "yyyy/MM/dd HH:mm:ss"
datetime_convert = partial(F.to_timestamp, format=spark_datetime_format)
# datetime_convert = partial(F.to_utc_timestamp, tz="Asia/Taipei")



def format_header(col):
    return col.replace(' ', '_').replace('/', '_').lower()


traffic_header = (
    "FUTURE_USE, Receive Time, Serial Number, Type, Threat/Content Type, "
    "FUTURE_USE, Generated Time, Source Address, Destination Address, "
    "NAT Source IP, NAT Destination IP, Rule Name, "
    "Source User, Destination User, Application, Virtual System, "
    "Source Zone, Destination Zone, Inbound Interface, Outbound Interface, "
    "Log Action, FUTURE_USE, Session ID, Repeat Count, "
    "Source Port, Destination Port, NAT Source Port, NAT Destination Port, "
    "Flags, Protocol, Action, Bytes, Bytes Sent, Bytes Received, "
    "Packets, Start Time, Elapsed Time, Category, FUTURE_USE, "
    "Sequence Number, Action Flags, Source Location, Destination Location, "
    "FUTURE_USE, Packets Sent, Packets Received, Session End Reason, "
    "Device Group Hierarchy Level 1, Device Group Hierarchy Level 2, "
    "Device Group Hierarchy Level 3, Device Group Hierarchy Level 4, "
    "Virtual System Name, Device Name, Action Source, "
    "Source VM UUID, Destination VM UUID, "
    "Tunnel ID/IMSI, Monitor Tag/IMEI, Parent Session ID, Parent Start Time, "
    "Tunnel Type, SCTP Association ID, SCTP Chunks, SCTP Chunks Sent, SCTP Chunks Received, "
    "UUID for rule, HTTP/2 Connection"
)
threat_header = (
    "FUTURE_USE, Receive Time, Serial Number, Type, Threat/Content Type, "
    "FUTURE_USE, Generated Time, Source Address, Destination Address, "
    "NAT Source IP, NAT Destination IP, Rule Name, Source User, Destination User, "
    "Application, Virtual System, Source Zone, Destination Zone, "
    "Inbound Interface, Outbound Interface, Log Action, FUTURE_USE, "
    "Session ID, Repeat Count, Source Port, Destination Port, "
    "NAT Source Port, NAT Destination Port, Flags, Protocol, "
    "Action, URL/Filename, Threat ID, Category, Severity, "
    "Direction, Sequence Number, Action Flags, "
    "Source Location, Destination Location, FUTURE_USE, "
    "Content Type, PCAP_ID, File Digest, Cloud, URL Index, "
    "User Agent, File Type, X-Forwarded-For, Referer, Sender, "
    "Subject, Recipient, Report ID, "
    "Device Group Hierarchy Level 1, Device Group Hierarchy Level 2, "
    "Device Group Hierarchy Level 3, Device Group Hierarchy Level 4, "
    "Virtual System Name, Device Name, FUTURE_USE, "
    "Source VM UUID, Destination VM UUID, HTTP Method, "
    "Tunnel ID/IMSI, Monitor Tag/IMEI, Parent Session ID, Parent Start Time, "
    "Tunnel Type, Threat Category, Content Version, FUTURE_USE, "
    "SCTP Association ID, Payload Protocol ID, HTTP Headers, "
    "URL Category List, UUID for rule, HTTP/2 Connection"
)


traffic_header = list(map(format_header, traffic_header.split(', ')))
threat_header = list(map(format_header, threat_header.split(', ')))

spark_types = [datetime_convert, T.StringType, T.StringType, T.StringType, T.StringType, T.StringType,
               T.StringType, T.StringType, T.StringType, T.IntegerType, T.IntegerType,
               T.StringType, T.IntegerType, T.IntegerType, datetime_convert]

traffic_types = [T.StringType, T.StringType, T.StringType, T.StringType, T.StringType,
                 T.StringType, T.StringType, T.StringType, T.StringType, T.IntegerType, T.IntegerType,
                 datetime_convert, T.IntegerType, T.IntegerType, T.StringType]

threat_types = [datetime_convert, T.StringType, T.StringType,
                T.StringType, T.StringType, T.StringType, T.StringType]


# traffic log 64 columns
traffic_indices = [3, 4, 7, 8, 14, 24, 25, 29, 30, 32, 33, 35, 44, 45, 46]
# threat log 78 columns
threat_indices = [1, 3, 4, 7, 8, 32, 34]

assert len(traffic_indices) == len(traffic_types)
assert len(threat_indices) == len(threat_types)

log_type_dict = {
    "traffic": {
        "indices": traffic_indices,
        "header": traffic_header,
        "types": traffic_types,
        "time_attr": "start_time",
        "features": ["appearance", "bytes_received", "bytes_sent"],
        "topic": "traffic_log",
        "agg_topic": "traffic_windowed_appearance",
    },
    "threat": {
        "indices": threat_indices,
        "header": threat_header,
        "types": threat_types,
        "time_attr": "receive_time",
        "features": ["appearance"],
        "topic": "threat_log",
        "agg_topic": "threat_windowed_appearance",
    },
}


def to_saved_record(df, key, value):
    df = df.withColumn("key", key)
    df = df.withColumn("value", value)
    save_df = df.select(F.col("key"), F.col("value"))
    return save_df


def preprocessing_df(df, log_type):
    header = log_type_dict[log_type]["header"]
    indices = log_type_dict[log_type]["indices"]
    types = log_type_dict[log_type]["types"]
    time_attr = log_type_dict[log_type]["time_attr"]
    features = log_type_dict[log_type]["features"]
    topic = log_type_dict[log_type]["topic"]
    agg_topic = log_type_dict[log_type]["agg_topic"]

    selected_header = [header[i] for i in indices]

    # Split line and convert type
    split_logs = df.withColumn(
        "_temp", F.split(df.value, ","))

    selected_column = (F.col("_temp").getItem(i).alias(header[i])
                       for i in indices)
    logs = split_logs.select(*selected_column)

    for column, type_name in zip(selected_header, types):
        if "time" in column:
            logs = logs.withColumn(column, datetime_convert(column))
        else:
            logs = logs.withColumn(column, F.col(column).cast(type_name()))

    # Create new column only match NCTU IP
    nctu_ip = F.when(F.col("source_address").rlike(r"140.113.\d+\.\d+"), F.col("source_address"))\
        .otherwise(F.col("destination_address"))
    logs = logs.withColumn("nctu_address", nctu_ip)
    logs = logs.withColumn("appearance", F.lit(1))

    # Save splited result
    key = F.to_json(F.struct(F.col("nctu_address")))
    selected_header = [F.col(column) for column in selected_header]
    selected_header += [F.col("appearance"), F.col("nctu_address")]
    value = F.to_json(F.struct(selected_header))

    save_logs = to_saved_record(logs, key, value)
    save_logs = save_logs.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", topic) \
        .option("checkpointLocation", "checkpoint_logs")\
        .start()

    # Group by specific time interval
    group_by_one_minute = logs.withWatermark(time_attr, "1 minute")\
        .groupBy(F.window(time_attr, "1 minute"), "nctu_address")

    feature_aggregation = (F.sum(F.col(feature)) for feature in features)
    windowed_aggregations = group_by_one_minute.agg(
        *feature_aggregation).orderBy('window', ascending=False)
    
    # TODO reduce is a good idea!
    for column in features:
        windowed_aggregations = windowed_aggregations.withColumnRenamed(
            f"SUM({column})", column
        )
    windowed_aggregations = windowed_aggregations.withColumn("window", F.col("window.start"))
    windowed_aggregations = windowed_aggregations.withColumn("window_ts",
         F.to_timestamp(F.col("window"), "yyyy-MM-dd'T'hh:mm:ss").cast(T.TimestampType()))

    if debug:
        # windowed_aggregations.printSchema()
        save_aggregations = windowed_aggregations.writeStream\
            .outputMode("complete")\
            .format("console")\
            .option('truncate', 'false')\
            .trigger(once=True) \
            .start()
    else:
        key = F.to_json(F.struct("nctu_address", "window", "window_ts"))
        selected_header = [F.col(column) for column in features]
        value = F.to_json(F.struct(selected_header))
        save_aggregations = to_saved_record(
            windowed_aggregations, key, value)
        save_aggregations = save_aggregations.writeStream \
            .outputMode("complete")\
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", agg_topic) \
            .option("checkpointLocation", "checkpoint_agg")\
            .start()

    if not debug:
        save_logs.awaitTermination()
        save_aggregations.awaitTermination()


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("StructuredLogAggragationWindowed")\
        .getOrCreate()

    if debug:
        logs = spark \
            .readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 6666) \
            .load()
    else:
        logs = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "user_log") \
            .option("failOnDataLoss", "false") \
            .load()

    traffic_logs = logs.filter(logs.value.rlike('TRAFFIC'))
    threat_logs = logs.filter(logs.value.rlike('THREAT'))

    preprocessing_df(traffic_logs, "traffic")
    preprocessing_df(threat_logs, "threat")

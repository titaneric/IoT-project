from functools import partial

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from config import debug

spark_datetime_format = "yyyy/MM/dd HH:mm:ss"
datetime_convert = partial(F.to_timestamp, format=spark_datetime_format)

header = (
    "receive time,type,source ip,from port,dest ip,to port,"
    "application,action,session end,byte receive,byte send,"
    "ip protocol,packet receive,packet send,start time"
)
header = list(map(lambda x: x.replace(' ', '_'), header.split(',')))

spark_types = [datetime_convert, T.StringType, T.StringType, T.StringType, T.StringType, T.StringType,
               T.StringType, T.StringType, T.StringType, T.IntegerType, T.IntegerType,
               T.StringType, T.IntegerType, T.IntegerType, datetime_convert]

features = "appearance byte_receive byte_send".split()

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

    split_column = logs.withColumn("_temp", F.split(logs.value, ","))
    # split_column.printSchema()

    # Split line and convert type
    selected_column = (F.col("_temp").getItem(i).alias(header[i])
                       for i in range(len(header)))
    logs = split_column.select(*selected_column)
    for column, type_name in zip(header, spark_types):
        if "time" in column:
            logs = logs.withColumn(column, datetime_convert(column))
        else:
            logs = logs.withColumn(column, F.col(column).cast(type_name()))

    # Create new column only match NCTU IP
    nctu_ip = F.when(F.col("source_ip").rlike(r"140.113.\d+\.\d+"), F.col("source_ip"))\
        .otherwise(F.col("dest_ip"))
    logs = logs.withColumn("NCTU_IP", nctu_ip)

    logs = logs.withColumn("appearance", F.lit(1))
    # logs.printSchema()

    # Group by specific time interval
    group_by_one_minute = logs.withWatermark("start_time", "1 minute")\
        .groupBy(F.window("start_time", "1 minute"), "NCTU_IP")

    feature_aggregation = (F.sum(F.col(feature)) for feature in features)
    windowed_aggregations = group_by_one_minute.agg(
        *feature_aggregation).orderBy('window', ascending=False)

    if debug:
        windowed_aggregations = windowed_aggregations.writeStream\
            .outputMode("complete")\
            .format("console")\
            .option('truncate', 'false')\
            .start()
    else:
        windowed_aggregations = windowed_aggregations.withColumnRenamed("window", "key")\
            .withColumnRenamed("SUM(appearance)", "value")
        windowed_aggregations = windowed_aggregations\
            .selectExpr("CAST(key AS string)", "CAST(value AS string)")
        windowed_aggregations = windowed_aggregations.writeStream \
            .outputMode("complete")\
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "windowed_appearance") \
            .option("checkpointLocation", "checkpoint")\
            .start()

    windowed_aggregations.awaitTermination()

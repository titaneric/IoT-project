from functools import partial

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, explode, split, to_timestamp
from pyspark.sql.types import *

from gen_log import ip_choices

py_datetime_format = r"%Y/%m/%d %H:%M:%S"
spark_datetime_format = "yyyy/MM/dd HH:mm:ss"
datetime_convert = partial(to_timestamp, format=spark_datetime_format)

header = (
    "receive time,type,source ip,from port,dest ip,to port,"
    "application,action,session end,byte receive,byte send,"
    "ip protocol,packet receive,packet send,start time,occurance"
)
header = list(map(lambda x: x.replace(' ', '_'), header.split(',')))

spark_types = [datetime_convert, StringType, StringType, StringType, StringType, StringType,
            StringType, StringType, StringType, IntegerType, IntegerType,
            StringType, IntegerType, IntegerType, datetime_convert, IntegerType]

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("StructuredLogAggragationWindowed")\
        .getOrCreate()

    logs = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 6666) \
        .load()

    split_column = logs.withColumn("_temp", split(logs.value, ","))
    split_column.printSchema()

    logs = split_column.select(\
        *(split_column["_temp"].getItem(i).alias(header[i])\
             for i in range(len(header))))
    for column, type_name in zip(header, spark_types):
        if "time" in column:
            logs = logs.withColumn(column, datetime_convert(column))
        else:
            logs = logs.withColumn(column, logs[column].cast(type_name()))
    logs.printSchema()
    
    query = logs.writeStream\
        .format("console").start()
    query.awaitTermination()


    # group_by_one_minute = logs.rdd.map(convert_type).groupBy(window("start_time", "5 seconds"))

    # windowedOccurances = group_by_one_minute.count().orderBy('window').writeStream\
    #     .format("console").start()
        
    # windowedOccurances.awaitTermination()



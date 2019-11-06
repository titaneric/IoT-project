from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import window

datetime_format = r"%Y/%m/%d %H:%M:%S"


def datetime_convert(datetime_str):
    return datetime.strptime(datetime_str, datetime_format)

def define_schema():
    header = (
        "receive time,type,source ip,from port,dest ip,to port,"
        "application,action,session end,byte receive,byte send,"
        "ip protocol,packet receive,packet send,start time,occurance"
    )
    header = list(map(lambda x: x.replace(' ', '_'), header.split(',')))
    types = [TimestampType, StringType, StringType, StringType, StringType, StringType,
             StringType, StringType, StringType, IntegerType, IntegerType,
             StringType, IntegerType, IntegerType, TimestampType, IntegerType]
    fields = [StructField(field_name, dataframe_type())
              for field_name, dataframe_type in zip(header, types)]
    schema = StructType(fields)
    return schema


def convert_type(line):
    # Add occurance to the last element
    elements = line.split(",") + [1]
    types = [datetime_convert, str, str, str, str, str,
             str, str, str, int, int,
             str, int, int, datetime_convert, int]
    for i, (element, type_func) in enumerate(zip(elements, types)):
        elements[i] = type_func(element)
    return elements


if __name__ == "__main__":
    schema = define_schema()

    spark = SparkSession\
        .builder\
        .appName("Test basic logs")\
        .getOrCreate()

    sc = spark.sparkContext
    # Load a text file and convert each line to a Row.
    logs = sc.textFile("/home/iot/output_abnormal_08-31.log")
    logs = logs.map(convert_type)
    # Apply the schema to the RDD.
    schema_logs = spark.createDataFrame(logs, schema)
    group_by_one_minute = schema_logs.groupBy(window("start_time", "1 minute"))

    # Occurances
    grouped_occurances = group_by_one_minute\
        .sum("occurance").orderBy('window')
    grouped_occurances.show(20, False)

    # Byte receive
    grouped_byte_receive = group_by_one_minute\
        .sum("byte_receive").orderBy('window')
    grouped_byte_receive.show(20, False)

    # Byte send
    grouped_send = group_by_one_minute\
        .sum("byte_send").orderBy('window')
    grouped_send.show(20, False)

    schema_logs.printSchema()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

header = (
    "receive time,type,source ip,from port,dest ip,to port,"
    "application,action,session end,byte receive,byte send,"
    "ip protocol,packet receive,packet send,start time"
)

header = list(map(lambda x: x.replace(' ', '_'), header.split(',')))

sc = SparkContext("local[2]", "LogAggregator")
ssc = StreamingContext(sc, 1)
ssc.checkpoint("checkpoint")

def split_log(log):
    log = log.split(",")
    log_dict = dict(zip(header, log))
    return log_dict["source_ip"], int(log_dict["byte_receive"])

def update_val(val, previous):
    if previous is None:
        previous = 0
    return sum(val, previous)

lines = ssc.socketTextStream("localhost", 6666)
state = lines.map(split_log)
running_cnts = state.updateStateByKey(update_val)

running_cnts.pprint()


ssc.start()
ssc.awaitTermination()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from gen_log import ip_choices

header = (
    "receive time,type,source ip,from port,dest ip,to port,"
    "application,action,session end,byte receive,byte send,"
    "ip protocol,packet receive,packet send,start time"
)

header = list(map(lambda x: x.replace(' ', '_'), header.split(',')))
window_len = 10
sc = SparkContext("local[2]", "LogAggregator")
ssc = StreamingContext(sc, 1)
ssc.checkpoint("checkpoint")

def split_log(log):
    log = log.split(",")
    log_dict = dict(zip(header, log))
    ip = log_dict["source_ip"] if log_dict["source_ip"] in ip_choices else log_dict["dest_ip"]
    # return ip, int(log_dict["byte_receive"])
    return ip, 1

def update_val(val, previous):
    if previous is None:
        previous = 0
    return previous + val

def remove_val(current_sum, previous):
    return current_sum - previous

lines = ssc.socketTextStream("localhost", 6666)
state = lines.map(split_log)
# running_cnts = state.updateStateByKey(update_val)
# running_cnts.pprint()
sum_each_minite = state.reduceByKeyAndWindow(update_val, remove_val, window_len)
sum_each_minite.pprint()


ssc.start()
ssc.awaitTermination()

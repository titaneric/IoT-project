from pathlib import Path
import re
from datetime import datetime
import locale

import pandas as pd

data_dir = Path("/home/chenyee/campus/ldap80")

started_pattern = r"(.*)\sproxy1.*\sconn=(\d+)\s.*\sfrom\sIP=(.*):(\d+)\s\(.*"
info_pattern = r'.*\sconn=(\d+)\s.*dn="cn=([^,]*).*,ou=([^\d]+),o=.*".*'
closed_pattern = r"(.*)\sproxy1.*\sconn=(\d+)\s.*\sclosed.*"

time_format = r"%b %d %H:%M:%S"


def get_datetime_obj(s):
    def true_year(month): return 2020 if month == 1 else 2019
    dt_obj = datetime.strptime(s, time_format)
    dt_obj = dt_obj.replace(year=true_year(dt_obj.month))
    return dt_obj


def process_line(line):
    global record_day
    # start connection
    started_result = re.match(started_pattern, line)
    if started_result is not None:
        started_time = get_datetime_obj(started_result.group(1))
        if record_day is None:
            record_day = str(started_time.date())
            print(record_day)
        connection_id = started_result.group(2)
        source_ip = started_result.group(3)
        source_port = started_result.group(4)

        if connection_id not in existed_connection:
            existed_connection[connection_id] = dict(
                source_ip=source_ip,
                source_port=source_port,
                connection_id=connection_id,
                started_time=str(started_time))

    # intermediate information
    info_result = re.match(info_pattern, line)
    if info_result is not None:
        connection_id = info_result.group(1)
        if connection_id in existed_connection:
            cn = info_result.group(2)
            ou = info_result.group(3)
            previous_record = existed_connection[connection_id]
            previous_record['cn'] = cn
            previous_record['ou'] = ou

    # finished connection
    closed_result = re.match(closed_pattern, line)
    if closed_result is not None:
        finished_time = get_datetime_obj(closed_result.group(1))
        connection_id = closed_result.group(2)
        if connection_id in existed_connection:
            previous_record = existed_connection[connection_id]
            previous_record['finished_time'] = str(finished_time)
            records.append(previous_record)
            del existed_connection[connection_id]


existed_connection = dict()
records = []
record_day = None

for d in sorted(data_dir.iterdir(), key=lambda f: int(f.name.split(".")[-1]), reverse=True):
    print(d.name)
    with open(d, "r") as f:
        for line in f.readlines():
            process_line(line)
    df = pd.DataFrame.from_records(records)
    df.to_csv(f"/home/chenyee/campus/server1/{record_day}.csv", index=False)
    records.clear()
    record_day = None

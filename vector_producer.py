import pprint
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pymongo import MongoClient

time_format = r"%Y-%m-%d %H:%M:%S"
delta = {
    "hours": 6
}


def aggregate_log(begin_ts, end_ts):
    matchStage = {
        "$match": {
            "window_unix_ts": {"$gte": begin_ts, "$lt": end_ts}
        }
    }

    groupByAddressStage = {
        "$group": {
            "_id": {
                "addr": "$nctu_address",
                "date": "$window",
            },
            "agg": {
                "$push": {
                    "appr": "$appearance",
                    # "bytes_received": "$bytes_received",
                    # "bytes_sent": "$bytes_sent",
                    "ts": "$window_unix_ts",
                }
            },
        }
    }

    maximumProjectStage = {
        "$project": {
            "appr": {"$max": "$agg.appr"}
        }
    }

    featureListStage = {
        "$group": {
            "_id": "$_id.addr",
            "list": {
                "$push": {
                    "appr": "$appr",
                    "date": "$_id.date",
                }
            }
        }
    }

    cursor = source.aggregate(
        [
            matchStage,
            groupByAddressStage,
            maximumProjectStage,
            featureListStage,
        ], allowDiskUse=True
    )

    return cursor


def form_vector(df):
    df["date"] = pd.to_datetime(df["date"]).dt.strftime(time_format)
    df.set_index("date", inplace=True)

    feature = "appr"
    full_agg = pd.DataFrame({feature: 0}, index=full_mins)
    full_agg[feature] = df[feature]
    full_agg.fillna(0, inplace=True)

    tmp_max = np.max(full_agg[feature].values)
    tmp_min = np.min(full_agg[feature].values)
    diff = (tmp_max - tmp_min)
    normalized_log = (full_agg[feature].values - tmp_min) / diff
    normalized_log = np.nan_to_num(normalized_log)
    assert len(normalized_log) == length
    assert all(np.isnan(normalized_log)) is False

    return normalized_log


if __name__ == "__main__":
    # Match window at certain range
    end = datetime.now().replace(second=0, microsecond=0)
    end_ts = time.mktime(end.timetuple())
    begin = end - timedelta(**delta)
    begin_ts = time.mktime(begin.timetuple())
    print(begin, end)

    # exclude end
    full_mins = pd.date_range(
        start=begin, end=end, freq="min", closed="left").strftime(time_format)
    length = len(full_mins)

    conn = MongoClient()
    db = conn["log"]

    for log_type in ["traffic", "threat"]:
        source = db[f"{log_type}_windowed_appearance"]
        sink = db[f"{log_type}_appearance_vector"]

        cursor = aggregate_log(begin_ts, end_ts)

        for log in cursor:
            addr = log["_id"]
            df = pd.DataFrame(log["list"])
            normalized_log = form_vector(df)

            feature_vector = {
                "feature": normalized_log.tolist(),
                "address": addr,
                "start": begin,
                "end": end
            }
            sink.insert_one(feature_vector)
            # pprint.pprint(feature_vector)
            # break

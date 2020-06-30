import pprint
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pymongo import MongoClient

from structured_stream import log_type_dict

time_format = r"%Y-%m-%d %H:%M:%S"
delta = {
    "hours": 6
}


def aggregate_windowed_log(begin_ts, end_ts, source, features):
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
                "$push": features
            },
        }
    }

    # This stage is neccesary although the mode is `update` in structured streaming
    project_features = {
        feature: {"$max": f"$agg.{feature}"}
        for feature in features.keys()
    }
    maximumProjectStage = {
        "$project": project_features
    }

    featureListStage = {
        "$group": {
            "_id": "$_id.addr",
            "list": {
                "$push": {
                    "date": "$_id.date",
                    **features
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


def form_windowed_vector(df, features):
    df["date"] = pd.to_datetime(df["date"]).dt.strftime(time_format)
    df.set_index("date", inplace=True)

    init_features = {
        feature: 0
        for feature in features.keys()
    }
    full_agg = pd.DataFrame(init_features, index=full_mins)

    for feature in features.keys():
        full_agg[feature] = df[feature]
        full_agg.fillna(0, inplace=True)

        tmp_max = np.max(full_agg[feature].values)
        tmp_min = np.min(full_agg[feature].values)
        diff = (tmp_max - tmp_min)
        normalized_log = (full_agg[feature].values - tmp_min) / diff
        normalized_log = np.nan_to_num(normalized_log)
        assert len(normalized_log) == length
        assert all(np.isnan(normalized_log)) is False
        full_agg[feature] = normalized_log

    return full_agg.to_dict("list")


def produce_windowed_vector():
    for log_type in ["traffic", "threat"]:
        features = log_type_dict[log_type]["features"]
        features = {
            feature: f"${feature}"
            for feature in features
        }

        source = db[f"{log_type}_windowed_appearance"]
        sink = db[f"{log_type}_appearance_vector"]

        cursor = aggregate_windowed_log(begin_ts, end_ts, source, features)

        for log in cursor:
            addr = log["_id"]
            df = pd.DataFrame(log["list"])
            normalized_logs = form_windowed_vector(df, features)

            feature_vector = {
                "features": normalized_logs,
                "address": addr,
                "start": begin,
                "end": end
            }
            sink.insert_one(feature_vector)


def aggregate_severity_log(begin_ts, end_ts, source):
    matchStage = {
        "$match": {
            "receive_time_unix_ts": {"$gte": begin_ts, "$lt": end_ts}
        }
    }
    groupByAddressStage = {
        "$group": {
            "_id": "$nctu_address",
            "severity": {
                "$push": "$severity"
            },
        }
    }
    cursor = source.aggregate(
        [
            matchStage,
            groupByAddressStage,
        ], allowDiskUse=True
    )

    return cursor


def produce_severity_vector():
    source = db["threat_log"]
    sink = db["threat_severity_vector"]
    cursor = aggregate_severity_log(begin_ts, end_ts, source)
    severity_types = ["low", "medium", "high", "critical"]
    for log in cursor:
        addr = log["_id"]
        severity_series = pd.Series(log["severity"])
        severity_dict = severity_series.value_counts().to_dict()
        severity_count_list = [severity_dict.get(
            severity_type, 0) for severity_type in severity_types]

        feature_vector = {
            "feature": severity_count_list,
            "address": addr,
            "start": begin,
            "end": end
        }
        sink.insert_one(feature_vector)


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

    produce_windowed_vector()
    produce_severity_vector()

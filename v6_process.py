import os
from pathlib import Path
from datetime import date, datetime, timedelta
import pickle
import argparse
import re

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()
np.seterr(divide='ignore', invalid='ignore')


parser = argparse.ArgumentParser(
    description='Extract the feature vector from log and dump to pickle')

parser.add_argument('features', type=str, nargs='?',
                    default='appearance', help='Features of log want to extract')
parser.add_argument('exclude_date', type=str, nargs='*', default='10-30 10-31',
                    help='Dates want to be excluded, which will be in test data')
parser.add_argument('header_file', type=str, nargs='?',
                    default='header.txt', help='Header file')
parser.add_argument('file_regex', type=str, nargs='?',
                    default=r'.*_.*_(\d+-\d+).*', help='Header file')
parser.add_argument('train_file_name', type=str, nargs='?',
                    default='v6_train.pickle', help='Pickle name of training data')
parser.add_argument('test_file_name', type=str, nargs='?',
                    default='v6_test.pickle', help='Pickle name of testing data')
args = parser.parse_args()

with open(args.header_file, 'r') as f:
    header = f.readline()
    header = header.lower().replace(' ', '-')
    header = header.split(',')


def get_unique_user(load=False, user_num=10):
    pickle_file = "unique_user.pickle"
    if not load:
        path = Path("/home/iot/v6")
        unique_user = set()
        for f in path.iterdir():
            log = pd.read_csv(f, header=None, names=header)

            source_ip = log["source-ip"]
            unique_source_user = source_ip[source_ip.str.contains(
                "nctu_ip")].unique()
            unique_user.update(unique_source_user.tolist())

            dest_ip = log["dest-ip"]
            unique_dest_user = dest_ip[dest_ip.str.contains(
                "nctu_ip")].unique()
            unique_user.update(unique_dest_user.tolist())

        with open(pickle_file, 'wb') as pi:
            pickle.dump(unique_user, pi)
    else:
        with open(pickle_file, 'rb') as f:
            unique_user = pickle.load(f)

    return list(unique_user)[:user_num]


features = list(map(lambda feature: feature.strip(), args.features.split()))
for feature in features:
    if feature != 'appearance':
        assert feature in set(header)

unique_users = get_unique_user(load=True)
user_num = len(unique_users)
print(f"Number of unique user is {user_num}")

train_valid_dict = {user: [] for user in range(user_num)}
test_valid_dict = {user: [] for user in range(user_num)}


train_dict = {feature: [] for feature in features}
test_dict = {feature: [] for feature in features}

exclude_date = set(map(lambda date: date.strip(), args.exclude_date.split()))

train_file_name = args.train_file_name
test_file_name = args.test_file_name

pattern = args.file_regex

unique_days = set()


def extract_feature_vector():
    path = Path("/home/iot/v6")
    one_hot_user_source = [0 for _ in range(user_num)]

    for f in path.iterdir():
        m = re.match(pattern, f.name)
        if m is not None:
            file_date = m.group(1)
            print(file_date)
            next_day = datetime.strptime(
                f"2019_{file_date}", r"%Y_%m-%d") + timedelta(days=1)
            next_day = next_day.strftime(r"%m-%d")

            log = pd.read_csv(f, header=None, names=header)
            log['start-time'] = log['start-time'].astype('datetime64')
            log['receive-time'] = log['receive-time'].astype('datetime64')
            log['appearance'] = 1

            start_date = f'2019-{file_date}'
            next_date = f'2019-{next_day}'
            unique_days.add(start_date)

            for feature in features:
                for user_index, user in enumerate(unique_users):
                    user_mask = (log['source-ip'] ==
                                 user) | (log['dest-ip'] == user)
                    user_aggregate = log[[feature, 'start-time', 'source-ip', 'dest-ip']][user_mask].groupby(
                        pd.Grouper(key='start-time', freq='1min')).sum().reset_index()
                    date_mask = (
                        user_aggregate['start-time'] >= start_date) & (user_aggregate['start-time'] < next_date)
                    user_aggregate = user_aggregate[date_mask]
                    user_aggregate.set_index('start-time', inplace=True)

                    full_mins = pd.date_range(
                        start=start_date, periods=1440, freq='min')
                    full_agg = pd.DataFrame({feature: 0}, index=full_mins)
                    full_agg[feature] = user_aggregate[feature]
                    full_agg.fillna(0, inplace=True)

                    try:
                        tmp_max = np.max(full_agg[feature].values)
                        tmp_min = np.min(full_agg[feature].values)

                        diff = (tmp_max - tmp_min)

                        normalized_log = (
                            full_agg[feature].values - tmp_min) / diff

                        normalized_log = np.nan_to_num(normalized_log)

                        assert len(normalized_log.tolist()) == 1440
                        assert all(np.isnan(normalized_log)) is False

                        one_hot_user = one_hot_user_source.copy()
                        one_hot_user[user_index] = 1

                        assert len(one_hot_user) == user_num
                        assert one_hot_user[user_index] == 1
                        assert one_hot_user.count(0) == (user_num - 1)

                        user_log = [normalized_log.tolist(), one_hot_user]

                        if file_date in exclude_date:
                            test_dict[feature].append(user_log)
                            test_valid_dict[user_index].append(1)
                        else:
                            train_dict[feature].append(user_log)
                            train_valid_dict[user_index].append(1)

                    except ValueError:
                        print(f.name, user, feature)
                        continue


def dump_to_pickle():
    print('Valid number for training')
    trained_days = len(unique_days) - len(exclude_date)
    tested_days = len(exclude_date)

    # print(len(unique_days), len(exclude_date))
    assert len(train_valid_dict) == user_num
    for user, l in train_valid_dict.items():
        # print(user, len(l) // len(features))
        assert len(l) // len(features) == trained_days

    print('Valid number for testing')
    assert len(test_valid_dict) == user_num
    for user, l in test_valid_dict.items():
        # print(user, len(l) // len(features))
        assert len(l) // len(features) == tested_days

    for feature, vector in train_dict.items():
        print(f"Valida number for feature {feature} in training")
        assert len(vector) == trained_days * user_num

    for feature, vector in test_dict.items():
        print(f"Valida number for feature {feature} in testing")
        assert len(vector) == tested_days * user_num

    with open(train_file_name, 'wb') as pi:
        pickle.dump(train_dict, pi)

    with open(test_file_name, 'wb') as pi:
        pickle.dump(test_dict, pi)


if __name__ == "__main__":
    print(f'Arguments are', args)
    extract_feature_vector()
    dump_to_pickle()

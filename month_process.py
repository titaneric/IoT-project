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


parser = argparse.ArgumentParser(description='Extract the feature vector from log and dump to pickle')
parser.add_argument('normal_user_name', type=str, nargs='?', default='nuser', help='Name of normal user')
parser.add_argument('abnormal_user_name', type=str, nargs='?', default='user', help='Name of abnormal user')
parser.add_argument('normal_user', type=int, nargs='?', default=8, help='Number of normal user')
parser.add_argument('abnormal_user', type=int, nargs='?', default=4, help='Number of abnormal user')
parser.add_argument('source_normal_path', type=str, nargs='?', default='log/normal', help='Directory of normal source logs')
parser.add_argument('source_abnormal_path', type=str, nargs='?', default='log/abnormal', help='Directory of abnormal source logs')

parser.add_argument('features', type=str, nargs='?', default='appearance byte-receive byte-send', help='Features of log want to extract')
parser.add_argument('exclude_date', type=str, nargs='*', default='11-28 11-29', help='Dates want to be excluded, which will be in test data')
parser.add_argument('header_file', type=str, nargs='?', default='header.txt', help='Header file')
parser.add_argument('file_regex', type=str, nargs='?', default=r'.*_(\d+_\d+).*', help='Header file')
parser.add_argument('train_file_name', type=str, nargs='?', default='train.pickle', help='Pickle name of training data')
parser.add_argument('test_file_name', type=str, nargs='?', default='test.pickle', help='Pickle name of testing data')
args = parser.parse_args()

with open(args.header_file, 'r') as f:
    header = f.readline()

config_dict = {
    True: {'user': args.abnormal_user_name, 'number': args.abnormal_user, 'base': 0},
    False: {'user': args.normal_user_name, 'number': args.normal_user, 'base': args.abnormal_user},
}

total_user = config_dict[True]['number'] + config_dict[False]['number']
train_valid_dict = {user: [] for user in range(total_user)}
test_valid_dict = {user: [] for user in range(total_user)}

features = list(map(lambda feature: feature.strip(), args.features.split()))
train_dict = {feature: [] for feature in features}
test_dict = {feature: [] for feature in features}

exclude_date = list(map(lambda date: date.replace('-', '_').strip(), args.exclude_date.split()))
source_normal_path = Path(args.source_normal_path)
source_abnormal_path = Path(args.source_abnormal_path)
train_file_name = args.train_file_name
test_file_name = args.test_file_name

pattern = args.file_regex

unique_days = set()

def extract_feature_vector():
    for d, is_abnormal in zip([source_normal_path, source_abnormal_path], [False, True]):
        user_number = config_dict[is_abnormal]['number']
        user_name = config_dict[is_abnormal]['user']
        base = config_dict[is_abnormal]['base']
        for f in d.iterdir():
            m = re.match(pattern, f.name)
            if m is not None:
                file_date = m.group(1)

                next_day = datetime.strptime(f"2018_{file_date}", r"%Y_%m_%d") + timedelta(days=1)
                next_day = next_day.strftime(r"%m_%d")

                log = pd.read_csv(f, header=None, names=list(map(lambda f: f.replace(' ', '-'), header.split(','))))
                log['start-time'] = log['start-time'].astype('datetime64')
                log['receive-time'] = log['receive-time'].astype('datetime64')
                log['appearance'] = 1

                start_date = f'2018-{file_date.replace("_", "-")}'
                next_date = f'2018-{next_day.replace("_", "-")}'

                unique_days.add(start_date)

                for feature in features:
                    for user_index in range(1, user_number + 1):
                        user = f'{user_name}{user_index}'
                        user_mask = (log['source-ip'] == user) | (log['dest-ip'] == user)
                        user_aggregate = log[[feature, 'start-time', 'source-ip', 'dest-ip']][user_mask].groupby(pd.Grouper(key='start-time', freq='1min')).sum().reset_index()
                        date_mask = (user_aggregate['start-time'] >= start_date ) & (user_aggregate['start-time'] < next_date)
                        user_aggregate = user_aggregate[date_mask]
                        user_aggregate.set_index('start-time', inplace=True)

                        full_mins = pd.date_range(start=start_date, periods=1440, freq='min')
                        full_agg = pd.DataFrame({feature: 0}, index=full_mins)
                        full_agg[feature] = user_aggregate[feature]
                        full_agg.fillna(0, inplace=True)

                        try:
                            tmp_max = np.max(full_agg[feature].values)
                            tmp_min = np.min(full_agg[feature].values)

                            diff = (tmp_max - tmp_min)

                            normalized_log = (full_agg[feature].values - tmp_min) / diff

                            normalized_log = np.nan_to_num(normalized_log)

                            assert len(normalized_log.tolist()) == 1440
                            assert all(np.isnan(normalized_log)) is False

                            absolute_user_index = base + user_index - 1
                            one_hot_user = [0 for _ in range(total_user)]
                            one_hot_user[absolute_user_index] = 1

                            if file_date in exclude_date:
                                test_dict[feature].append([normalized_log.tolist(), one_hot_user])
                                test_valid_dict[absolute_user_index].append(1)
                            else:
                                train_dict[feature].append([normalized_log.tolist(), one_hot_user])
                                train_valid_dict[absolute_user_index].append(1)

                        except ValueError:
                            print(f.name, user, feature)
                            continue


def dump_to_pickle():
    print('Valid number for training')
    for user, l in train_valid_dict.items():
        # print(user, len(l) // len(features))
        assert len(l) // len(features) == len(unique_days) - len(exclude_date)

    print('Valid number for testing')
    for user, l in test_valid_dict.items():
        # print(user, len(l) // len(features))
        assert len(l) // len(features) == len(exclude_date)

    with open(train_file_name, 'wb') as pi:
        pickle.dump(train_dict, pi)

    with open(test_file_name, 'wb') as pi:
        pickle.dump(test_dict, pi)


if __name__ == "__main__":
    print(f'Arguments are', args)
    extract_feature_vector()
    dump_to_pickle()




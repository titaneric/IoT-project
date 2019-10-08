import os
from pathlib import Path
from datetime import date, datetime, timedelta
import pickle
import argparse

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()
np.seterr(divide='ignore', invalid='ignore')


parser = argparse.ArgumentParser(description='Extract the feature vector from log and dump to pickle')
parser.add_argument('abnormal_user', type=int, nargs='?', default=4, help='Number of abnormal user')
parser.add_argument('normal_user', type=int, nargs='?', default=8, help='Number of normal user')
parser.add_argument('features', type=str, nargs='?', default='appearance byte-receive byte-send', help='Features of log want to extract')
parser.add_argument('exclude_date', type=str, nargs='*', default='11/28 11/29', help='Dates want to be excluded, which will be in test data')
parser.add_argument('source_path', type=str, nargs='?', default='log', help='Directory of source logs')
parser.add_argument('train_file_name', type=str, nargs='?', default='train.pickle', help='Pickle name of training data')
parser.add_argument('test_file_name', type=str, nargs='?', default='test.pickle', help='Pickle name of testing data')
args = parser.parse_args()

head = 'receive time,type,source ip,from port,dest ip,to port,application,action,session end,byte receive,byte send,ip protocol,packet receive,packet send,start time'
config_dict = {
    True: {'user': 'user', 'number': args.abnormal_user, 'ext': 'abnormal_', 'base': 0},
    False: {'user': 'nuser', 'number': args.normal_user, 'ext': '', 'base': args.abnormal_user},
}

total_user = config_dict[True]['number'] + config_dict[False]['number']
train_valid_dict = {user: [] for user in range(total_user)}
test_valid_dict = {user: [] for user in range(total_user)}

features = list(map(lambda feature: feature.strip(), args.features.split()))
train_dict = {feature: [] for feature in features}
test_dict = {feature: [] for feature in features}

exclude_date = list(map(lambda date: date.replace('/', '_').strip(), args.exclude_date.split()))
source_path = Path(args.source_path)
train_file_name = args.train_file_name
test_file_name = args.test_file_name

unique_days = set()

def extract_feature_vector():
    for f in source_path.iterdir():
        if f.is_file() and f.match('*.log'):
            is_abnormal = f.match('*abnormal*')
            user_number = config_dict[is_abnormal]['number']
            user_name = config_dict[is_abnormal]['user']
            ext = config_dict[is_abnormal]['ext']
            base = config_dict[is_abnormal]['base']

            file_date = f.name[len(f'output_{ext}'):-len('.log')]

            next_day = datetime.strptime(f"2018_{file_date}", r"%Y_%m_%d") + timedelta(days=1)
            next_day = next_day.strftime(r"%m_%d")

            log = pd.read_csv(f, header=None, names=list(map(lambda f: f.replace(' ', '-'), head.split(','))))
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




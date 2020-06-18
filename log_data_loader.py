import os
import pickle

import torch
from torch.utils.data import Dataset, DataLoader
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


"""
Revised from
https://pytorch.org/tutorials/beginner/data_loading_tutorial.html#dataset-class
"""
class NetworkLogDataset(Dataset):

    def __init__(self, pickle_file, feature, root_dir=None, transform=None):
        with open(pickle_file, 'rb') as f:
            self.meta = pickle.load(f)
        
        self.meta = self.meta[feature]
        self.root_dir = root_dir
        self.transform = transform

    def __len__(self):
        return len(self.meta)

    def __getitem__(self, idx):
        if torch.is_tensor(idx):
            idx = idx.tolist()
        '''
        vector version
        '''

        log_vector, condition = self.meta[idx]
        log_vector = torch.tensor(log_vector)
        condition = torch.tensor(condition)

        sample = {'vector': log_vector, 'condition': condition}

        return sample

if __name__ == "__main__":
    features = 'appearance'
    for feature in features.split():
        for pickle_file in ['v6_1000_train.pickle', 'v6_1000_test.pickle']:
            print(f'Valid for {pickle_file} in dataloader of feature {feature}')
            log_dataset = NetworkLogDataset(pickle_file=pickle_file, feature=feature)

            dataloader = DataLoader(log_dataset, batch_size=4,
                                shuffle=True, num_workers=4)
            for i_batch, sample_batched in enumerate(dataloader):
                # print(i_batch, sample_batched['vector'].size(),
                #     sample_batched['condition'].size())
                assert sample_batched['vector'].size()[1] == 1440
                nan_check = torch.isnan(sample_batched['vector']).byte().any()
                assert nan_check.item() == 0



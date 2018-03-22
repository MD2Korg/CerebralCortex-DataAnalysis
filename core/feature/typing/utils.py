from cerebralcortex.cerebralcortex import CerebralCortex
from pprint import pprint
from scipy.io import savemat
from datetime import timedelta, datetime
from collections import OrderedDict
from cerebralcortex.core.util.data_types import DataPoint
from collections import OrderedDict
from typing import List
from sklearn import ensemble
from collections import Counter
from keras.models import Sequential
from keras.layers import Conv2D, MaxPooling2D, LSTM, Dense, Dropout, Flatten
from keras.layers.core import Permute, Reshape
from keras import backend as K
from keras.models import load_model

import math
import datetime
import pandas as pd
import pytz
import numpy as np
import matplotlib.pyplot as plt
import scipy.io
import keras


def typing_episodes(dataset, offset):
    # this function detects typing episodes
    dataset = dataset.values
    dataset_cp = np.copy(dataset[:, 1:13])

    n_samples, d = dataset_cp.shape
    window = 20
    stride = 5

    # Data Reshaping
    data_slide = np.zeros((int((n_samples - window) / stride) + 1, window, d))
    time_t = np.zeros((int((n_samples - window) / stride) + 1, 1))
    k = 0
    for i in range(0, n_samples - window, stride):  # 400ms
        data_slide[k, :, :] = dataset_cp[i:i + window, :]
        time_t[k] = dataset[i, 0]
        k = k + 1

    z = 0
    X_test0 = data_slide[z:]

    # Load Trained Model
    model = load_model('Trained_Models/Typing_Detection/Convbn_LSTM_100.h5')
    network_type = 'ConvLSTM'
    _, win_len, dim = X_test0.shape

    X_test = _data_reshaping(X_test0, network_type)

    y_pred = np.argmax(model.predict(X_test), axis=1)

    # Smoothing
    indices_type = np.where(y_pred == 1)[0]
    time_type = time_t[indices_type]
    data = []

    if (len(indices_type) > 0):

        pred_l = len(y_pred)
        ind_l = len(indices_type)
        smooth_labels_3 = np.zeros((pred_l, 1))
        s = 0
        start_time = []
        end_time = []

        for i in range(0, ind_l - 1):
            if (s == 0):
                start_time.append(time_type[i])
                s = 1

            if ((time_type[i + 1] - time_type[i]) < 10000):
                smooth_labels_3[indices_type[i]:indices_type[i + 1]] = 1
            else:
                end_time.append(time_type[i] + 200)
                s = 0
        end_time.append(time_type[-1] + 200)

        for i in range(0, len(start_time)):
            st = datetime.fromtimestamp(int(float(start_time[i])))
            et = datetime.fromtimestamp(int(float(end_time[i])))
            data.append(DataPoint(start_time=st, end_time=et, offset=offset, sample='Typing'))
            print(
                'Start time : {0:f} , End Time: {1:f} , Label: Typing'.format(float(start_time[i]), float(end_time[i])))
    else:
        print("no typing")
    return data


def _data_reshaping(X_va, network_type):
    _, win_len, dim = X_va.shape

    if network_type == 'CNN' or network_type == 'ConvLSTM':
        # make it into (frame_number, dimension, window_size, channel=1) for convNet
        X_va = np.swapaxes(X_va, 1, 2)
        X_va = np.reshape(X_va, (-1, dim, win_len, 1))

    return X_va


def sync_left_right_accel(dl, dr):
    # this function syncs the datafarems of left and right accelerometers

    dl_new = dl
    dr_new = dr

    time_l = np.array(dl[dl.columns[0]])
    time_r = np.array(dr[dr.columns[0]])

    max_val = np.amax((time_r[0], time_l[0]))
    max_val = np.amax((time_r[0], time_l[0]))

    dl_new = dl_new[dl_new['time'] >= max_val].drop(['time'], axis=1)
    dr_new = dr_new[dr_new['time'] >= max_val].drop(['time'], axis=1)

    time_l = time_l[time_l >= max_val]
    time_r = time_r[time_r >= max_val]

    n_values = time_l.shape[0] - time_r.shape[0]
    d = dr_new.shape[1]

    if time_l.shape[0] > time_r.shape[0]:
        time_r = np.append(time_r, np.zeros((n_values,)))
        dr_new = np.append(dr_new, np.zeros((n_values, d)), axis=0)

    elif time_l.shape[0] < time_r.shape[0]:
        time_l = np.append(time_l, np.zeros((n_values,)))
        dl_new = np.append(dl_new, np.zeros((n_values, d)), axis=0)

    time_r = time_r.reshape((-1, 1))
    time_l = time_l.reshape((-1, 1))
    dataset = pd.DataFrame(np.concatenate((time_l, dl_new, dr_new), axis=1))
    dataset.columns = ['time', 'arx', 'ary', 'arz', 'grx', 'gry', 'grz',
                       'alx', 'aly', 'alz', 'glx', 'gly', 'glz']

    return (dataset)



def unique_days_of_one_stream(dict):
    # this function creates a unique list of days for each stream

    merged_dates = []

    for stream_id in dict:
        merged_dates = list(set(merged_dates + dict[stream_id]))

    merged_dates_set = set(merged_dates)
    return merged_dates_set


def get_dataframe(data: List[DataPoint], var_name):
    # this function takes a list of datapoints and make them into a dataframe
    if len(data) == 0:
        return None
    D = [[v.start_time.timestamp(), v.sample[0], v.sample[1], v.sample[2]] for v in data]
    data_frame = pd.DataFrame(D, columns=var_name)

    return data_frame

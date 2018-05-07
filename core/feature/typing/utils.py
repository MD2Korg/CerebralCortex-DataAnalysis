# Copyright (c) 2018, MD2K Center of Excellence
# -Mithun Saha <msaha1@memphis.edu>,JEYA VIKRANTH JEYAKUMAR <vikranth94@ucla.edu>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from cerebralcortex.cerebralcortex import CerebralCortex
from pprint import pprint
from datetime import timedelta, datetime
from cerebralcortex.core.util.data_types import DataPoint
from sklearn import ensemble
from collections import Counter
from keras.models import Sequential
from keras.layers import Conv2D, MaxPooling2D, LSTM, Dense, Dropout, Flatten
from keras.layers.core import Permute, Reshape
from keras import backend as K
from keras.models import load_model
from typing import List

import scipy.io
import pandas as pd
import numpy as np
import numbers
import tempfile
import os

from core.computefeature import get_resource_contents

# TYPING_MODEL_FILENAME = 'core/resources/models/typing/Convbn_LSTM_100.h5'
# TYPING_MODEL_FILENAME = 'core/resources/models/typing/CNN.h5'
TYPING_MODEL_FILENAME = 'core/resources/models/typing/CNN_all.h5'

WINDOW_SIZE = 25  # for a 1000ms window (at 25Hz we get a value every 40ms.)

STRIDE = 5  # we make a prediction every 200ms


def typing_episodes(dataset: pd.DataFrame, offset: int) -> List[DataPoint]:
    """
    This function detects typing episodes.

    Makes a prediction every 200ms using values from a window of 1000ms.
    This means there will be a overlap of 800ms between each sample window.

    :param pd.DataFrame dataset: the synced dataframe of left and right accl and gyro data
    :param int offset: offset for local time
    :return: DataPoints of typing episodes
    :rtype:List(DataPoint)
    """

    dataset = dataset.values

    # 12 columns of x,y,z values for accl and gyro data
    dataset_cp = np.copy(dataset[:, 1:13])

    n_samples, d = dataset_cp.shape

    # Data Reshaping
    # the following lines convert the data stream into a sliding window
    # with window size 1000ms and stride 200 ms

    data_slide = np.zeros((int((n_samples - WINDOW_SIZE) / STRIDE) + 1,
                           WINDOW_SIZE, d))

    # stores staring time for each window
    time_t = np.zeros((int((n_samples - WINDOW_SIZE) / STRIDE) + 1, 1))
    k = 0
    for i in range(0, n_samples - WINDOW_SIZE, STRIDE):  # 400ms
        data_slide[k, :, :] = dataset_cp[i:i + WINDOW_SIZE, :]
        time_t[k] = dataset[i, 0]
        k = k + 1

    z = 0
    X_test0 = data_slide[z:]

    # Load Trained Model

    tmpfile = tempfile.NamedTemporaryFile(delete=True)
    tmpfile.write(get_resource_contents(TYPING_MODEL_FILENAME))
    model = load_model(os.path.realpath(tmpfile.name))
    tmpfile.close()

    # network_type = 'ConvLSTM'
    network_type = 'CNN'
    _, win_len, dim = X_test0.shape

    # data has to be reshaped before being fed into the model
    X_test = _data_reshaping(X_test0, network_type)

    # y_pred = 1 indicates typing
    # y_pred = 0 indicates no_typing
    y_pred = np.argmax(model.predict(X_test), axis=1)

    # Smoothing - to reduce noisy predictions
    indices_type = np.where(y_pred == 1)[0]
    time_type = time_t[indices_type]  # contains timestamps of when user is typing
    data = []
    typing_time = timedelta(0)

    # smooth_labels_3: final output prediction
    # start_time: start time of the typing seesion
    # end_time of the typing session

    if len(indices_type) > 0:
        pred_l = len(y_pred)
        ind_l = len(indices_type)
        smooth_labels_3 = np.zeros((pred_l, 1))
        s = 0
        start_time = []
        end_time = []

        for i in range(0, ind_l - 1):
            if s == 0:
                start_time.append(time_type[i])
                s = 1

            if (time_type[i + 1] - time_type[i]) < 10000:  # 10000 = 10 seconds
                smooth_labels_3[indices_type[i]:indices_type[i + 1]] = 1
            else:
                end_time.append(time_type[i] + 200)  # 200 = 200 milliseconds
                s = 0
        end_time.append(time_type[-1] + 200)  # 200 = 200 milliseconds

        for i in range(0, len(start_time)):
            st = datetime.fromtimestamp(int(float(start_time[i])))
            et = datetime.fromtimestamp(int(float(end_time[i])))
            if st.day != et.day:
                et = datetime(st.year, st.month, st.day) + timedelta(hours=23, minutes=59, seconds=59)
            typing_time = et - st

            # data.append(DataPoint(start_time=st, end_time=et, offset=offset,sample=1))
            # data.append(DataPoint(st,et,offset,[1,float(format(typing_time.seconds/60,'.3f'))]))
            data.append(DataPoint(start_time=st, end_time=et, offset=offset,
                                  sample=[1, float(
                                      format(typing_time.seconds / 60,
                                             '.3f'))]))

    return data


def _data_reshaping(x_va: np.ndarray, network_type: str) -> np.ndarray:
    """
    This function is used to reshape the data into a particular form
    to make use of the keras tensorflow api.

    :param np.ndarray x_va: dataset
    :param str network_type: model type
    :return: reshaped dataset
    :rtype: np.ndarray
    """
    _, win_len, dim = x_va.shape

    if network_type == 'CNN' or network_type == 'ConvLSTM':
        # make it into (frame_number, dimension, window_size, channel=1) for convNet
        x_va = np.swapaxes(x_va, 1, 2)
        x_va = np.reshape(x_va, (-1, dim, win_len, 1))

    return x_va



def sync_left_right_accel(dl: pd.DataFrame, dr: pd.DataFrame) -> pd.DataFrame:
    """
    This function is used to sync and combine the left,right accl and gyro dataframes.

    :param pd.DataFrame dl: combined dataframe of left accelerometer and gyroscope data
    :param pd.DataFrame dr: combined dataframe of right accelerometer and gyroscope data
    :return: a synced and combined dataframe of left and right accelerometer and gyroscope
            dataframes
    :rtype:pd.DataFrame
    """

    dl_new = dl
    dr_new = dr

    time_l = np.array(dl[dl.columns[0]])  # making a numpy array
    time_r = np.array(dr[dr.columns[0]])  # making a numpy array

    # taking the max of two time values left and right arrays
    max_val = np.amax((time_r[0], time_l[0]))

    # to ensure that both left and right dataframes start with the same time
    dl_new = dl_new[dl_new['time'] >= max_val].drop(['time'], axis=1)
    dr_new = dr_new[dr_new['time'] >= max_val].drop(['time'], axis=1)

    time_l = time_l[time_l >= max_val]
    time_r = time_r[time_r >= max_val]

    # zeros are padded at the end to make the dataframes similar in size
    # so that they can be merged together
    n_values = np.abs(time_l.shape[0] - time_r.shape[0])
    d = dr_new.shape[1]

    if time_l.shape[0] > time_r.shape[0]:
        time_r = np.append(time_r, np.zeros((n_values,)))
        dr_new = np.append(dr_new, np.zeros((n_values, d)), axis=0)
        time_r = time_r.reshape((-1, 1))
        time_l = time_l.reshape((-1, 1))
        dataset = pd.DataFrame(np.concatenate((time_l, dl_new, dr_new), axis=1))

    elif time_l.shape[0] < time_r.shape[0]:
        time_l = np.append(time_l, np.zeros((n_values,)))
        dl_new = np.append(dl_new, np.zeros((n_values, d)), axis=0)
        time_r = time_r.reshape((-1, 1))
        time_l = time_l.reshape((-1, 1))
        dataset = pd.DataFrame(np.concatenate((time_r, dl_new, dr_new), axis=1))

    dataset.columns = ['time', 'arx', 'ary', 'arz', 'grx', 'gry', 'grz',
                       'alx', 'aly', 'alz', 'glx', 'gly', 'glz']

    return (dataset)


def unique_days_of_one_stream(input_dict: dict) -> set:
    """
    This function takes a dictionary of stream ids with dates of each stream
    and makes a unique set of dates for all the stream ids

    :param dict input_dict: a dictionary of stream ids as keys with dates as values
    :return: a set of dates for all the stream ids of one stream for a day
    :rtype: set
    """
    merged_dates = []

    for stream_id in input_dict:
        merged_dates = list(set(merged_dates + input_dict[stream_id]))

    merged_dates_set = set(merged_dates)
    return merged_dates_set


def get_dataframe(data: List[DataPoint], var_name: list) -> pd.DataFrame:
    """
    This function takes a list of DataPoints of each stream
    and makes a dataframe with unique set of column names

    :param List[DataPoint] data: a list of DataPoints
    :param list var_name: a list of X,Y,Z column names for left and right accelerometer
                          and gyroscope data
    :return: a dataframe of one stream (like left accelerometer)
    :rtype: pd.DataFrame
    """

    # this function takes a list of DataPoints and make them into a dataframe
    if len(data) == 0:
        return None
    d = []

    for v in data:
        if type(v.sample) != list or len(v.sample) != 3:
            continue

        for index in range(len(v.sample)):
            if type(v.sample[index]) == str:
                #    print("string Data:",v.sample)
                v.sample[index] = v.sample[index].replace('\x00', '0')
                try:
                    v.sample[index] = float(v.sample[index])
                except:
                    v.sample[index] = 0
            #   print("converted data:",v.sample)

        d.append([v.start_time.timestamp(), v.sample[0], v.sample[1], v.sample[2]])

    data_frame = pd.DataFrame(d, columns=var_name)

    return data_frame

# Copyright (c) 2018, MD2K Center of Excellence
# - Nazir Saleheen <nazir.saleheen@gmail.com>
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

import json
import os
import uuid
from datetime import timedelta
from typing import List

import numpy as np
from numpy.linalg import norm

import core.signalprocessing.vector as vector
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datapoint import DataPoint

study_name = 'mperf'

# Sampling frequency
SAMPLING_FREQ_RIP = 21.33
SAMPLING_FREQ_MOTIONSENSE_ACCEL = 25
SAMPLING_FREQ_MOTIONSENSE_GYRO = 25

# MACD (moving average convergence divergence) related threshold
FAST_MOVING_AVG_SIZE = 20
SLOW_MOVING_AVG_SIZE = 205

# Hand orientation related threshold
MIN_ROLL = -20
MAX_ROLL = 65
MIN_PITCH = -125
MAX_PITCH = -40

# MotionSense sample range
MIN_MSHRV_ACCEL = -2.5
MAX_MSHRV_ACCEL = 2.5

MIN_MSHRV_GYRO = -250
MAX_MSHRV_GYRO = 250

# Puff label
PUFF_LABEL_RIGHT = 2
PUFF_LABEL_LEFT = 1
NON_PUFF_LABEL = 0

# Input stream names required for puffmarker
MOTIONSENSE_HRV_ACCEL_LEFT_STREAMNAME = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
MOTIONSENSE_HRV_GYRO_LEFT_STREAMNAME = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
MOTIONSENSE_HRV_ACCEL_RIGHT_STREAMNAME = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
MOTIONSENSE_HRV_GYRO_RIGHT_STREAMNAME = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"

PUFFMARKER_MODEL_FILENAME = 'core/resources/models/puffmarker/puffmarker_wrist_randomforest.model'

# Outputs stream names
PUFFMARKER_WRIST_SMOKING_EPISODE = "org.md2k.data_analysis.feature.puffmarker.smoking_episode"
PUFFMARKER_WRIST_SMOKING_PUFF = "org.md2k.data_analysis.feature.puffmarker.smoking_puff"

# smoking episode
MINIMUM_TIME_DIFFERENCE_FIRST_AND_LAST_PUFFS = 7 * 60  # seconds
MINIMUM_INTER_PUFF_DURATION = 5  # seconds
MINIMUM_PUFFS_IN_EPISODE = 4

# change orientation of the device
CONV_R = [[0, 1, 0],
          [-1, 0, 0],
          [0, 0, 1]]
CONV_L = [[0, 1, 0],
          [1, 0, 0],
          [0, 0, 1]]
# sample = [x, y, z]
# new_sample = list(np.dot(CONV,sample))

def getInterpoletedValue(g0, g1, t0, t1, t):
    g = g0 + (g1 - g0) * (t - t0) / (t1 - t0)
    return g

def merge_two_datastream(accel: List[DataPoint], gyro: List[DataPoint]):
    A = np.array(
        [[dp.start_time.timestamp(), dp.sample[0], dp.sample[1], dp.sample[2]]
         for dp in accel])
    G = np.array(
        [[dp.start_time.timestamp(), dp.sample[0], dp.sample[1], dp.sample[2]]
         for dp in gyro])
    At = A[:, 0]

    Gt = G[:, 0]
    Gx = G[:, 1]
    Gy = G[:, 2]
    Gz = G[:, 3]
    i = 0
    j = 0
    _Gx = [0] * len(At)
    _Gy = [0] * len(At)
    _Gz = [0] * len(At)
    while (i < len(At)) and (j < len(Gt)):
        while Gt[j] < At[i]:
            j = j + 1
            if j >= len(Gt):
                break
        if j < len(Gt):
            if (At[i] == Gt[j]) | (j == 0):
                _Gx[i] = Gx[j]
                _Gy[i] = Gy[j]
                _Gz[i] = Gz[j]
            else:
                _Gx[i] = getInterpoletedValue(Gx[j - 1], Gx[j], Gt[j - 1],
                                              Gt[j], At[i])
                _Gy[i] = getInterpoletedValue(Gy[j - 1], Gy[j], Gt[j - 1],
                                              Gt[j], At[i])
                _Gz[i] = getInterpoletedValue(Gz[j - 1], Gz[j], Gt[j - 1],
                                              Gt[j], At[i])
        i = i + 1

    gyro = [DataPoint(start_time=dp.start_time, end_time=dp.end_time,
                      offset=dp.offset, sample=[_Gx[i], _Gy[i], _Gz[i]])
            for i, dp in enumerate(accel)]
    return gyro


def magnitude(data: List[DataPoint]) -> List[DataPoint]:
    """

    :param list[DataPoint] data:
    :return: magnitude of the data
    """
    if data is None or len(data) == 0:
        result = []
        return result

    result_data = [DataPoint(start_time=value.start_time, offset=value.offset,
                             sample=norm(value.sample)) for value in data]

    return result_data


def smooth(data: List[DataPoint],
           span: int = 5) -> List[DataPoint]:
    if span % 2 == 0:
        span = span + 1

    data_smooth = vector.smooth(data, span)

    return data_smooth


def moving_average_convergence_divergence(
        slow_moving_average_data: List[DataPoint]
        , fast_moving_average_data: List[DataPoint]
        , THRESHOLD: float, near: int):
    '''
    Generates intersection points of two moving average signals
    :param slow_moving_average_data:
    :param fast_moving_average_data:
    :param THRESHOLD: Cut-off value
    :param near: # of nearest points to ignore; i.e. gap between two segment should be greater than near
    :return:
    '''
    slow_moving_average = np.array(
        [data.sample for data in slow_moving_average_data])
    fast_moving_average = np.array(
        [data.sample for data in fast_moving_average_data])

    index_list = [0] * len(slow_moving_average)
    cur_index = 0

    for index in range(len(slow_moving_average)):
        diff = slow_moving_average[index] - fast_moving_average[index]
        if diff > THRESHOLD:
            if cur_index == 0:
                index_list[cur_index] = index
                cur_index = cur_index + 1
                index_list[cur_index] = index
            else:
                if index <= index_list[cur_index] + near:
                    index_list[cur_index] = index
                else:
                    cur_index = cur_index + 1
                    index_list[cur_index] = index
                    cur_index = cur_index + 1
                    index_list[cur_index] = index

    intersection_points = []
    if cur_index > 0:
        for index in range(0, cur_index, 2):
            start_index = index_list[index]
            end_index = index_list[index + 1]
            start_time = slow_moving_average_data[start_index].start_time
            end_time = slow_moving_average_data[end_index].start_time
            intersection_points.append(
                DataPoint(start_time=start_time, end_time=end_time,
                          sample=[index_list[index], index_list[index + 1]]))

    return intersection_points

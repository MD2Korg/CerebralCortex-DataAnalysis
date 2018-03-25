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
NO_PUFF = 0

# Input stream names required for puffmarker
MOTIONSENSE_HRV_ACCEL_LEFT_STREAMNAME = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
MOTIONSENSE_HRV_GYRO_LEFT_STREAMNAME = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
MOTIONSENSE_HRV_ACCEL_RIGHT_STREAMNAME = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
MOTIONSENSE_HRV_GYRO_RIGHT_STREAMNAME = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"

PUFFMARKER_MODEL_FILENAME = 'core/resources/models/puffmarker/model_file/puffmarker_wrist_randomforest.model'

# Outputs stream names
PUFFMARKER_WRIST_SMOKING_EPISODE = "org.md2k.data_analysis.feature.puffmarker.smoking_episode"
PUFFMARKER_WRIST_SMOKING_PUFF = "org.md2k.data_analysis.feature.puffmarker.smoking_puff"

# smoking episode
MINIMUM_TIME_DIFFERENCE_FIRST_AND_LAST_PUFFS = 7 * 60  # seconds
MINIMUM_INTER_PUFF_DURATION = 5  # seconds
MINIMUM_PUFFS_IN_EPISODE = 4


CONV_R = [[0, 1, 0],
          [-1, 0, 0],
          [0, 0, 1]]
CONV_L = [[0, 1, 0],
          [1, 0, 0],
          [0, 0, 1]]
# sample = [x, y, z]
# new_sample = [np.dot(CONV[0],sample), np.dot(CONV[1],sample), np.dot(CONV[2],sample)]



def magnitude(data: List[DataPoint]) -> List[DataPoint]:
    """

    :param datastream:
    :return:
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
    :param near: # of nearest point to ignore
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


def get_stream_days(stream_id: uuid, CC: CerebralCortex) -> List:
    """
    Returns a list of days (string format: YearMonthDay (e.g., 20171206)
    :param stream_id:
    """
    stream_dicts = CC.get_stream_duration(stream_id)
    stream_days = []
    days = stream_dicts["end_time"] - stream_dicts["start_time"]
    for day in range(days.days + 1):
        stream_days.append(
            (stream_dicts["start_time"] + timedelta(days=day)).strftime(
                '%Y%m%d'))
    return stream_days


def store_data(filepath, input_streams, user_id, data, instance):
    output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(
        filepath + user_id + "SMOKING EPISODE")))

    cur_dir = os.path.dirname(os.path.abspath(__file__))
    new_file_path = os.path.join(cur_dir, filepath)
    with open(new_file_path, "r") as f:
        metadata = f.read()
        metadata = metadata.replace("CC_INPUT_STREAM_ID_CC",
                                    input_streams[0].identifier)
        metadata = metadata.replace("CC_INPUT_STREAM_NAME_CC",
                                    input_streams[0].name)
        metadata = metadata.replace("CC_OUTPUT_STREAM_IDENTIFIER_CC",
                                    output_stream_id)
        metadata = metadata.replace("CC_OWNER_CC", user_id)
        metadata = json.loads(metadata)

        instance.store(identifier=output_stream_id, owner=user_id,
                       name=metadata["name"],
                       data_descriptor=metadata["data_descriptor"],
                       execution_context=metadata["execution_context"],
                       annotations=metadata["annotations"],
                       stream_type="datastream", data=data)

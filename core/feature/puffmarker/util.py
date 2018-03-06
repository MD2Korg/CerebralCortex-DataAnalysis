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

import core.signalprocessing.vector as vector
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream


def smooth(datastream: DataStream,
           span: int = 5) -> DataStream:
    if span % 2 == 0:
        span = span + 1

    data = datastream.data
    data_smooth = vector.smooth(data, span)

    data_smooth_stream = DataStream.from_datastream([datastream])
    data_smooth_stream.data = data_smooth
    return data_smooth_stream


def moving_average_convergence_divergence(slow_moving_average_data: DataStream
                                          , fast_moving_average_data: DataStream
                                          , THRESHOLD: float, near: int):
    slow_moving_average = np.array([data.sample for data in slow_moving_average_data.data])
    fast_moving_average = np.array([data.sample for data in fast_moving_average_data.data])

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
            start_time = slow_moving_average_data.data[start_index].start_time
            end_time = slow_moving_average_data.data[end_index].start_time
            intersection_points.append(
                DataPoint(start_time=start_time, end_time=end_time, sample=[index_list[index], index_list[index + 1]]))

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
        stream_days.append((stream_dicts["start_time"] + timedelta(days=day)).strftime('%Y%m%d'))
    return stream_days


def store_data(filepath, input_streams, user_id, data, instance):
    output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(filepath + user_id + "SMOKING EPISODE")))

    cur_dir = os.path.dirname(os.path.abspath(__file__))
    new_file_path = os.path.join(cur_dir, filepath)
    with open(new_file_path, "r") as f:
        metadata = f.read()
        metadata = metadata.replace("CC_INPUT_STREAM_ID_CC", input_streams[0].identifier)
        metadata = metadata.replace("CC_INPUT_STREAM_NAME_CC", input_streams[0].name)
        metadata = metadata.replace("CC_OUTPUT_STREAM_IDENTIFIER_CC", output_stream_id)
        metadata = metadata.replace("CC_OWNER_CC", user_id)
        metadata = json.loads(metadata)

        instance.store(identifier=output_stream_id, owner=user_id, name=metadata["name"],
                       data_descriptor=metadata["data_descriptor"],
                       execution_context=metadata["execution_context"], annotations=metadata["annotations"],
                       stream_type="datastream", data=data)

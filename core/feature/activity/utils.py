# Copyright (c) 2018, MD2K Center of Excellence
# - Sayma Akther <sakther@memphis.edu>
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

import os
import json
import uuid
from datetime import timedelta
from typing import List
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datapoint import DataPoint

###### --------------- start constants -------------------------------------
study_name = 'mperf'

# Sampling frequency
SAMPLING_FREQ_MOTIONSENSE_ACCEL = 25.0
SAMPLING_FREQ_MOTIONSENSE_GYRO = 25.0

IS_MOTIONSENSE_HRV_GYRO_IN_DEGREE = True

# input filenames
MOTIONSENSE_HRV_ACCEL_RIGHT = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
MOTIONSENSE_HRV_ACCEL_LEFT = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
MOTIONSENSE_HRV_GYRO_RIGHT = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
MOTIONSENSE_HRV_GYRO_LEFT = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"

# MotionSense sample range
MIN_MSHRV_ACCEL = -2.5
MAX_MSHRV_ACCEL = 2.5

MIN_MSHRV_GYRO = -250
MAX_MSHRV_GYRO = 250

LEFT_WRIST = 'left_wrist'
RIGHT_WRIST = 'right_wrist'

# Window size
TEN_SECONDS = 10

POSTURE_MODEL_FILENAME = 'core/feature/activity/models/posture_randomforest.model'
ACTIVITY_MODEL_FILENAME = 'core/feature/activity/models/activity_level_randomforest.model'

# Output labels
ACTIVITY_LABELS = ["NO", "LOW", "WALKING", "MOD", "HIGH"]
POSTURE_LABELS = ["lying", "sitting", "standing"]

ACTIVITY_LABELS_INDEX_MAP = {"NO": 0, "LOW": 1, "WALKING": 2, "MOD": 3, "HIGH": 4}
POSTURE_LABELS_INDEX_MAP = {"lying": 0, "sitting": 1, "standing": 2}


###### --------------- END constants -------------------------------------


def get_max_label(label1, label2):
    if label1 in ACTIVITY_LABELS and label2 in ACTIVITY_LABELS:
        if ACTIVITY_LABELS_INDEX_MAP[label1] > ACTIVITY_LABELS_INDEX_MAP[label2]:
            return label1
        else:
            return label2
    elif label1 in POSTURE_LABELS and label2 in POSTURE_LABELS:
        if POSTURE_LABELS_INDEX_MAP[label1] > POSTURE_LABELS_INDEX_MAP[label2]:
            return label1
        else:
            return label2
    return "UNDEFINED"


def merge_left_right(left_data: List[DataPoint],
                     right_data: List[DataPoint],
                     window_size=10.0):
    data = left_data + right_data
    data.sort(key=lambda x: x.start_time)

    merged_data = []
    win_size = timedelta(seconds=window_size)

    index = 0
    while index < len(data) - 1:
        if data[index].start_time + win_size > data[index + 1].start_time:
            updated_label = get_max_label(data[index].sample, data[index + 1].sample)
            merged_data.append(DataPoint(start_time=data[index].start_time,
                                         end_time=data[index].end_time,
                                         offset=data[index].offset,
                                         sample=updated_label))
            index = index + 2

        else:
            merged_data.append(DataPoint(start_time=data[index].start_time,
                                         end_time=data[index].end_time,
                                         offset=data[index].offset,
                                         sample=data[index].sample))
            index = index + 1
    return merged_data


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


def store_data(filepath, input_streams, user_id, data, str_sufix, instance):
    output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(filepath + user_id + str_sufix)))
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    newfilepath = os.path.join(cur_dir, filepath)
    with open(newfilepath, "r") as f:
        metadata = f.read()
        metadata = metadata.replace("CC_INPUT_STREAM_ID_CC", input_streams[0]["identifier"])
        metadata = metadata.replace("CC_INPUT_STREAM_NAME_CC", input_streams[0]["name"])
        metadata = metadata.replace("CC_OUTPUT_STREAM_IDENTIFIER_CC", output_stream_id)
        metadata = metadata.replace("CC_OWNER_CC", user_id)
        metadata = json.loads(metadata)

        instance.store(identifier=output_stream_id, owner=user_id, name=metadata["name"],
                       data_descriptor=metadata["data_descriptor"],
                       execution_context=metadata["execution_context"], annotations=metadata["annotations"],
                       stream_type="datastream", data=data)

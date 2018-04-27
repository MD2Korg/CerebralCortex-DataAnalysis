# Copyright (c) 2018, MD2K Center of Excellence
# All rights reserved.
# author: Md Azim Ullah
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

from typing import List
from cerebralcortex.core.datatypes.datapoint import DataPoint
import numpy as np
from copy import deepcopy
from scipy import signal
from collections import Counter
from cerebralcortex.cerebralcortex import CerebralCortex

motionsense_hrv_left = "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
motionsense_hrv_right = "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
motionsense_hrv_left_cat = "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
motionsense_hrv_right_cat = "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"

Fs = 25
window_size_60sec = 60
window_size_10sec = 10

def get_datastream(CC:CerebralCortex,
                   identifier:str,
                   day:str,
                   user_id:str,
                   localtime:bool)->List[DataPoint]:
    stream_ids = CC.get_stream_id(user_id,identifier)
    data = []
    for stream_id in stream_ids:
        temp_data = CC.get_stream(stream_id=stream_id['identifier'],user_id=user_id,day=day,localtime=localtime)
        if len(temp_data.data)>0:
            data.extend(temp_data.data)
    return data

def admission_control(data: List[DataPoint]) -> List[DataPoint]:
    """

    :rtype: List[DataPoint]
    :param List[DataPoint] data:
    :return:
    """
    final_data = []
    for dp in data:
        if isinstance(dp.sample, str) and len(dp.sample.split(',')) == 20:
            final_data.append(dp)
        if isinstance(dp.sample, list) and len(dp.sample) == 20:
            final_data.append(dp)
    return final_data


def decode_only(data: object) -> object:
    """

    :rtype: object
    :param data:
    :return:
    """
    final_data = []
    for dp in data:
        if isinstance(dp.sample, str):
            str_sample = str(dp.sample)
            str_sample_list = str_sample.split(',')
            if len(str_sample_list) != 20:
                continue
            Vals = [np.int8(np.float(val)) for val in str_sample_list]
        elif isinstance(dp.sample, list):
            Vals = [np.int8(val) for val in dp.sample]
        else:
            continue
        sample = np.array([0] * 3)
        sample[0] = (np.uint8(Vals[12]) << 10) | (np.uint8(Vals[13]) << 2) | \
                    ((np.uint8(Vals[14]) & int('11000000', 2)) >> 6)
        sample[1] = ((np.uint8(Vals[14]) & int('00111111', 2)) << 12) | \
                    (np.uint8(Vals[15]) << 4) | \
                    ((np.uint8(Vals[16]) & int('11110000', 2)) >> 4)
        sample[2] = ((np.uint8(Vals[16]) & int('00001111', 2)) << 14) | \
                    (np.uint8(Vals[17]) << 6) | \
                    ((np.uint8(Vals[18]) & int('11111100', 2)) >> 2)
        final_data.append(deepcopy(dp))
        final_data[-1].sample = sample
    return final_data


def bandpassfilter(x: object, fs: object) -> object:
    """
    
    :rtype: object
    :param x: a list of samples
    :param fs: sampling frequency
    :return: filtered list
    """
    x = signal.detrend(x)
    b = signal.firls(129, [0, 0.6 * 2 / fs, 0.7 * 2 / fs, 3 * 2 / fs, 3.5 * 2 / fs, 1], [0, 0, 1, 1, 0, 0],
                     [100 * 0.02, 0.02, 0.02])
    return signal.convolve(x, b, 'valid')


def isDatapointsWithinRange(red: object, infrared: object, green: object) -> bool:
    """

    :rtype: bool
    :param red:
    :param infrared:
    :param green:
    :return:
    """
    a = len(np.where((red >= 14000) & (red <= 170000))[0]) < .64 * len(red)
    b = len(np.where((infrared >= 100000) & (infrared <= 245000))[0]) < .64 * len(infrared)
    c = len(np.where((green >= 800) & (green <= 20000))[0]) < .64 * len(green)
    if a and b and c:
        return False
    return True


def compute_quality(red: object, infrared: object, green: object, fs: object) -> bool:
    """
    
    :param red:
    :param infrared:
    :param green:
    :param fs:
    :return: True/False where 0 = attached, 1 = not attached TODO: Confirm 0/1 values
    """

    if not isDatapointsWithinRange(red, infrared, green):
        return False

    if np.mean(red) < 5000 and np.mean(infrared) < 5000 and np.mean(green) < 5000:
        return False

    if not (np.mean(red) > np.mean(green) and np.mean(infrared) > np.mean(red)):
        return False

    diff = 30000
    if np.mean(red) > 140000 or np.mean(red) <= 30000:
        diff = 11000

    if not (np.mean(red) - np.mean(green) > diff and np.mean(infrared) - np.mean(red) > diff):
        return False

    if np.std(bandpassfilter(red, fs)) <= 5 and np.std(bandpassfilter(infrared, fs)) <= 5 and np.std(
            bandpassfilter(green, fs)) <= 5:
        return False

    return True


def get_quality(windowed_data: object, Fs: object) -> object:
    """

    :rtype: object
    :param windowed_data:
    :param Fs:
    :return:
    """
    quality = []
    for key in windowed_data.keys():
        data = windowed_data[key]
        if len(data) < .64 * Fs * 10:
            quality.append(False)
            continue
        red = np.array([i.sample[0] for i in data])
        infrared = np.array([i.sample[1] for i in data])
        green = np.array([i.sample[2] for i in data])
        quality.append(compute_quality(red, infrared, green, Fs))
    if not quality:
        return False
    value, count = Counter(quality).most_common()[0]
    return value

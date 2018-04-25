# Copyright (c) 2018, MD2K Center of Excellence
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

import numbers
from typing import Tuple

from core.feature.activity.utils import *


def is_valid_motionsense_hrv_accelerometer(dp: DataPoint) -> bool:
    """Check whether a valid accelerometer data point

    It checks whether it is a array of three floating point number and the range is valid

    Args:
        dp: DataPoint

    Returns:
        bool: True if this point is a MotionSenseHRV accelerometer sample
    """

    if not isinstance(dp.sample, List):
        return False

    if len(dp.sample) != 3:
        return False

    for v in dp.sample:
        if not isinstance(v, numbers.Real):
            return False
        if (v < MIN_MSHRV_ACCEL) or (v > MAX_MSHRV_ACCEL):
            return False

    return True


def is_valid_motionsense_hrv_gyroscope(dp: DataPoint) -> bool:
    """Check whether a valid gyroscope data point

    It checks whether it is a array of three floating point number and the range is valid

    Args:
        dp: DataPoint

    Returns:
        bool: True if this point is a MotionSenseHRV gyroscope sample
    """

    if not isinstance(dp.sample, List):
        return False

    if len(dp.sample) != 3:
        return False

    for v in dp.sample:
        if not isinstance(v, numbers.Real):
            return False
        if (v < MIN_MSHRV_GYRO) or (v > MAX_MSHRV_GYRO):
            return False

    return True


def clean_motionsense_hrv_accelerometer(accelerometer_data: List[DataPoint]) -> List[DataPoint]:
    """ Cleans an input DataPoint stream of invalid objects

    Args:
        accelerometer_data: List of DataPoints to check

    Returns:
        List[DataPoint]: Cleaned list of DataPoints
    """

    valid_accelerometer_data = []
    for dp in accelerometer_data:
        if is_valid_motionsense_hrv_accelerometer(dp):
            valid_accelerometer_data.append(dp)

    return valid_accelerometer_data


def clean_motionsense_hrv_gyroscope(gyroscope_data: List[DataPoint]) -> List[DataPoint]:
    """ Cleans an input DataPoint stream of invalid objects

    Args:
        gyroscope_data: List of DataPoints to check

    Returns:
        List[DataPoint]: Cleaned list of DataPoints
    """
    valid_gyro_data = []
    for dp in gyroscope_data:
        if is_valid_motionsense_hrv_gyroscope(dp):
            valid_gyro_data.append(dp)

    return valid_gyro_data


def check_motionsense_hrv_accel_gyroscope(accelerometer_data: List[DataPoint], gyroscope_data: List[DataPoint]) \
        -> Tuple[List[DataPoint], List[DataPoint]]:
    """ Cleans a paired set of input streams of invalid objects

    Args:
        accelerometer_data: List of DataPoints to check
        gyroscope_data: List of DataPoints to check

    Returns:
        Tuple[List[DataPoint],List[DataPoint]]: Cleaned matched set of accelerometer and gyroscope DataPoint streams
    """
    valid_accel_data = []
    valid_gyro_data = []
    for index, dp in enumerate(gyroscope_data):
        dp_g = gyroscope_data[index]
        dp_a = accelerometer_data[index]
        if is_valid_motionsense_hrv_accelerometer(dp_a) and is_valid_motionsense_hrv_gyroscope(dp_g):
            valid_accel_data.append(dp_a)
            valid_gyro_data.append(dp_g)

    return valid_accel_data, valid_gyro_data

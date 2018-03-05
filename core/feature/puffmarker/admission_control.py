import numbers
from typing import List

from cerebralcortex.core.datatypes.datapoint import DataPoint
from core.feature.puffmarker.CONSTANTS import *


def is_number(sample):
    return isinstance(sample, numbers.Real)


def is_valid_motionsense_hrv_accelerometer(dp: DataPoint):
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


def is_valid_motionsense_hrv_gyroscope(dp: DataPoint):
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


def check_motionsense_hrv_accelerometer(accel_data):
    valid_accel_data = []
    for dp in accel_data:
        if is_valid_motionsense_hrv_accelerometer(dp):
            valid_accel_data.append(dp)
    return valid_accel_data


def check_motionsense_hrv_gyroscope(accel_data):
    valid_accel_data = []
    for dp in accel_data:
        if is_valid_motionsense_hrv_gyroscope(dp):
            valid_accel_data.append(dp)
    return valid_accel_data

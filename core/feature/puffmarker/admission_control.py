import numbers
from typing import List

from cerebralcortex.core.datatypes.datapoint import DataPoint
from core.feature.puffmarker.PUFFMARKER_CONSTANTS import *


def is_valid_motionsense_hrv_accelerometer(dp: DataPoint):
    '''
    Check whether a valid accelerometer data point
    It checks whether it is a array of three real number and range is valid
    :param dp: accelerometer data point
    :return: True if valid data point
    '''
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
    '''
    Check whether a valid gyroscope data point
    It checks whether it is a array of three real number and range is valid
    :param dp: gyroscope data point
    :return: True if valid data point
    '''
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
    '''
    Check valid accelerometer data stream
    :param accel_data:
    :return: valid_accel_data
    '''
    valid_accel_data = []
    for dp in accel_data:
        if is_valid_motionsense_hrv_accelerometer(dp):
            valid_accel_data.append(dp)
    return valid_accel_data


def check_motionsense_hrv_gyroscope(gyro_data):
    '''
    Check valid gyroscope data stream
    :param gyro_data:
    :return: valid_accel_data
    '''
    valid_accel_data = []
    for dp in gyro_data:
        if is_valid_motionsense_hrv_gyroscope(dp):
            valid_accel_data.append(dp)
    return valid_accel_data

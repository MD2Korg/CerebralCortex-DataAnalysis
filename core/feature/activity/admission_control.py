import numbers
from typing import List

from cerebralcortex.core.datatypes.datapoint import DataPoint
from core.feature.activity.ACTIVITY_CONSTANTS import *


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
        else:
            print('ACCL='+str(dp.sample))

    return valid_accel_data


def check_motionsense_hrv_gyroscope(gyro_data):
    '''
    Check valid gyroscope data stream
    :param gyro_data:
    :return: valid_gyro_data
    '''
    valid_gyro_data = []
    for dp in gyro_data:
        if is_valid_motionsense_hrv_gyroscope(dp):
            valid_gyro_data.append(dp)
        else:
            print('gyro='+str(dp.sample))

    return valid_gyro_data

def check_motionsense_hrv_accel_gyroscope(accel_data, gyro_data):
    '''
    Check valid gyroscope data stream
    :param gyro_data:
    :return: valid_accel_data
    '''
    valid_accel_data = []
    valid_gyro_data = []
    for index, dp in enumerate(gyro_data):
        dp_g = gyro_data[index]
        dp_a = accel_data[index]
        if is_valid_motionsense_hrv_accelerometer(dp_a) and is_valid_motionsense_hrv_gyroscope(dp_g):
            valid_accel_data.append(dp_a)
            valid_gyro_data.append(dp_g)

    return valid_accel_data, valid_gyro_data

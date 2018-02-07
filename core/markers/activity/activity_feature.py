from core.signalprocessing.window import window_sliding
import numpy as np
import math
from typing import List
from scipy.stats import skew
from scipy.stats import kurtosis
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream

def get_rate_of_change(timestamp, value):
    roc = 0
    cnt = 0
    for i in range(len(value) - 1):
        if (timestamp[i + 1] - timestamp[i]).total_seconds() > 0:
            roc = roc + (((value[i + 1] - value[i]) / (timestamp[i + 1] - timestamp[i]).total_seconds()))
            cnt = cnt + 1
    if cnt > 0:
        roc = roc / cnt

    return roc


def get_magnitude(ax, ay, az):
    return math.sqrt(ax * ax + ay * ay + az * az)


def compute_basic_features(ts, data_array):
    mean = np.mean(data_array)
    median = np.median(data_array)
    std = np.std(data_array)
    skewness = skew(data_array)
    kurt = kurtosis(data_array)
    rateOfChanges = get_rate_of_change(ts, data_array)
    power = np.mean([v * v for v in data_array])

    return mean, median, std, skewness, kurt, rateOfChanges, power


def compute_window_features(start_time, end_time, data: List[DataPoint]) -> DataPoint:
    timestamps = [v.start_time for v in data]
    accel_x = [v.sample[0] for v in data]
    accel_y = [v.sample[1] for v in data]
    accel_z = [v.sample[2] for v in data]
    accel_magnitude = [get_magnitude(i.sample[0], i.sample[1], i.sample[2]) for i in data]

    mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges, mag_power = compute_basic_features(
        timestamps, accel_magnitude)
    x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power = compute_basic_features(timestamps, accel_x)
    y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power = compute_basic_features(timestamps, accel_y)
    z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power = compute_basic_features(timestamps, accel_z)

    # f = [timestamps[0], timestamps[-1], mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges,
    f = [mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges,
         mag_power]
    f.extend([x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power])
    f.extend([y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power])
    f.extend([z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power])

    return DataPoint(start_time=start_time, end_time=end_time, sample=f)


def compute_accelerometer_features(accel_stream: DataStream,
                           window_size: float = 10.0) -> DataStream:

    # perform windowing of datastream
    window_data = window_sliding(accel_stream.data, window_size, window_size)

    all_features = []

    for key, value in window_data.items():
        if len(value) > 200:
            start_time, end_time = key
            feature_list = compute_window_features(start_time, end_time, value)
            all_features.append(feature_list)

    all_feature_stream = DataStream.from_datastream([accel_stream])
    all_feature_stream.data = all_features

    return all_feature_stream

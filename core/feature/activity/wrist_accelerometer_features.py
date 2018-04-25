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
import datetime
from typing import Tuple

import numpy

import numpy as np
import math
from scipy.stats import skew
from scipy.stats import kurtosis
from core.feature.activity.utils import *


def get_rate_of_change(timestamps: List[datetime], values: List) -> float:  # TODO: What is the type of value?
    """# TODO: What is this supposed to do?

    Args:
        timestamps:
        values:

    Returns:
        float: Rate of change of ???
    """
    roc = 0
    cnt = 0
    for i in range(len(values) - 1):
        if (timestamps[i + 1] - timestamps[i]).total_seconds() > 0:
            roc = roc + ((values[i + 1] - values[i]) / (timestamps[i + 1] - timestamps[i]).total_seconds())
            cnt = cnt + 1
    if cnt > 0:
        roc = roc / cnt

    return roc


def get_magnitude(data_point: DataPoint) -> float:
    """Return the 3-axis magnitude calculation

    Args:
        data_point: 3-dimensional vector
    Returns:
        float: magnitude of three values
    """
    return math.sqrt(data_point.sample[0] * data_point.sample[0] +
                     data_point.sample[1] * data_point.sample[1] +
                     data_point.sample[2] * data_point.sample[2])


def spectral_entropy(data: numpy.ndarray, sampling_freq: float, bands: List[float] = None) -> numpy.ndarray:
    """Compute spectral entropy of a  signal with respect to frequency bands.

    The power spectrum is computed through fft. Then, it is normalised and
    assimilated to a probability density function.
    The entropy of the signal :math:`x` can be expressed by:
    .. math::
        H(x) =  -\sum_{f=0}^{f = f_s/2} PSD(f) log_2[PSD(f)]
    Where:
    :math:`PSD` is the normalised power spectrum (Power Spectrum Density), and
    :math:`f_s` is the sampling frequency

    Args:
        data: a one dimensional floating-point array representing a time series.
        sampling_freq: sampling frequency
        bands: a list of numbers delimiting the bins of the frequency bands.
            If None the entropy is computed over the whole range of the DFT
            (from 0 to :math:`f_s/2`)

    Returns:
        numpy.ndarray: the spectral entropy
    """

    psd = np.abs(np.fft.rfft(data)) ** 2
    psd /= np.sum(psd)  # psd as a pdf (normalised to one)

    if bands is None:
        power_per_band = psd[psd > 0]
    else:
        freqs = np.fft.rfftfreq(data.size, 1 / float(sampling_freq))
        bands = np.asarray(bands)

        freq_limits_low = np.concatenate([[0.0], bands])
        freq_limits_up = np.concatenate([bands, [np.Inf]])

        power_per_band = [np.sum(psd[np.bitwise_and(freqs >= low, freqs < up)])
                          for low, up in zip(freq_limits_low, freq_limits_up)]

        power_per_band = power_per_band[power_per_band > 0]

    return - np.sum(power_per_band * np.log2(power_per_band))


def peak_frequency(data: object) -> numpy.ndarray:
    """# TODO: Fix this

    Args:
        data:

    Returns:

    """
    w = np.fft.fft(data)
    frequencies = np.fft.fftfreq(len(w))
    return frequencies.max()


def compute_basic_features(timestamp: object, data: object) \
        -> Tuple[numpy.ndarray, numpy.ndarray, numpy.ndarray, numpy.ndarray,
                 numpy.ndarray, float, numpy.ndarray, numpy.ndarray, numpy.ndarray]:
    """# TODO: Fix this

    Args:
        timestamp:
        data:

    Returns:
        Tuple[numpy.ndarray,numpy.ndarray,numpy.ndarray,numpy.ndarray,numpy.ndarray,float,numpy.ndarray,numpy.ndarray,numpy.ndarray]
    """

    mean = np.mean(data)
    median = np.median(data)
    std = np.std(data)
    skewness = skew(data)
    kurt = kurtosis(data)
    rate_of_changes = get_rate_of_change(timestamp, data)
    power = np.mean([v * v for v in data])
    sp_entropy = spectral_entropy(data, SAMPLING_FREQ_MOTIONSENSE_ACCEL)
    peak_freq = peak_frequency(data)

    return mean, median, std, skewness, kurt, rate_of_changes, power, sp_entropy, peak_freq


# def computeFeatures(start_time: object, end_time: object, time: object, x: object, y: object, z: object,
#                     pid: object) -> object:
#     """
#
#     :rtype: object
#     :param start_time:
#     :param end_time:
#     :param time:
#     :param x:
#     :param y:
#     :param z:
#     :param pid:
#     :return:
#     """
#     mag = [0] * len(x)
#     for i, value in enumerate(x):
#         mag[i] = math.sqrt(x[i] * x[i] + y[i] * y[i] + z[i] * z[i])
#
#     mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges, mag_power, mag_sp_entropy, mag_peak_freq = compute_basic_features(time, mag)
#     x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power, x_sp_entropy, x_peak_freq = compute_basic_features(time, x)
#     y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power, y_sp_entropy, y_peak_freq = compute_basic_features(time, y)
#     z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power, z_sp_entropy, z_peak_freq = compute_basic_features(time, z)
#
#     f = [pid, start_time, end_time, mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges, mag_power, mag_sp_entropy, mag_peak_freq]
#
#     f.extend([x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power,x_sp_entropy, x_peak_freq])
#     f.extend([y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power, y_sp_entropy, y_peak_freq])
#     f.extend([z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power,z_sp_entropy, z_peak_freq])
#
#     return f


def compute_window_features(start_time: datetime, end_time: datetime, data: List[DataPoint]) -> DataPoint:
    """Computes the feature vector for a single window

    Args:
        start_time: Starting point
        end_time: Ending point
        data: Data to utilize in the computation

    Returns:
        DataPoint: DataPoint containing the computed feature vector
    """

    offset = 0
    if len(data) > 0:
        offset = data[0].offset

    timestamps = [v.start_time for v in data]

    accelerometer_x = [v.sample[0] for v in data]
    accelerometer_y = [v.sample[1] for v in data]
    accelerometer_z = [v.sample[2] for v in data]

    accel_magnitude = [get_magnitude(value) for value in data]

    mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rate_of_change, mag_power, mag_spectral_entropy, mag_peak_freq = compute_basic_features(timestamps, accel_magnitude)
    x_mean, x_median, x_std, x_skewness, x_kurt, x_rate_of_change, x_power, x_spectral_entropy, x_peak_freq = compute_basic_features(timestamps, accelerometer_x)
    y_mean, y_median, y_std, y_skewness, y_kurt, y_rate_of_change, y_power, y_spectral_entropy, y_peak_freq = compute_basic_features(timestamps, accelerometer_y)
    z_mean, z_median, z_std, z_skewness, z_kurt, z_rate_of_change, z_power, z_spectral_entropy, z_peak_freq = compute_basic_features(timestamps, accelerometer_z)

    feature_vector = [mag_mean,
                      mag_median,
                      mag_std,
                      mag_skewness,
                      mag_kurt,
                      mag_rate_of_change,
                      mag_power,
                      mag_spectral_entropy,
                      mag_peak_freq]

    feature_vector.extend([x_mean,
                           x_median,
                           x_std,
                           x_skewness,
                           x_kurt,
                           x_rate_of_change,
                           x_power,
                           x_spectral_entropy,
                           x_peak_freq])
    feature_vector.extend([y_mean,
                           y_median,
                           y_std,
                           y_skewness,
                           y_kurt,
                           y_rate_of_change,
                           y_power,
                           y_spectral_entropy,
                           y_peak_freq])
    feature_vector.extend([z_mean,
                           z_median,
                           z_std,
                           z_skewness,
                           z_kurt,
                           z_rate_of_change,
                           z_power,
                           z_spectral_entropy,
                           z_peak_freq])

    return DataPoint(start_time=start_time, end_time=end_time, offset=offset, sample=feature_vector)


def compute_accelerometer_features(accelerometer_data: List[DataPoint],
                                   window_size: float = 10.0) -> List[DataPoint]:
    """Segment data and computes feature vector for each window

    Args:
        accelerometer_data: Input data
        window_size: Length of window to operate over in seconds

    Returns:
        List[DataPoint]: A list of DataPoints containing feature vectors
    """

    all_features = []

    current_index = 0

    while current_index < len(accelerometer_data):
        start_index = current_index
        end_index = current_index

        accelerometer_window = []
        win_size = timedelta(seconds=window_size)

        while (accelerometer_data[end_index].start_time - accelerometer_data[start_index].start_time) < win_size:
            accelerometer_window.append(accelerometer_data[end_index])
            end_index = end_index + 1
            if end_index >= len(accelerometer_data):
                break

        feature_vector = compute_window_features(accelerometer_window[0].start_time,
                                                 accelerometer_window[-1].start_time,
                                                 accelerometer_window)
        all_features.append(feature_vector)

        current_index = end_index

    return all_features

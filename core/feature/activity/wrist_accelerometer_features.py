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

import numpy as np
import math
from scipy.stats import skew
from scipy.stats import kurtosis
from cerebralcortex.core.datatypes.datastream import DataStream
from core.feature.activity.utils import *


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


def spectral_entropy(data, sampling_freq, bands=None):
    """
    Compute spectral entropy of a  signal with respect to frequency bands.
    The power spectrum is computed through fft. Then, it is normalised and assimilated to a probability density function.
    The entropy of the signal :math:`x` can be expressed by:
    .. math::
        H(x) =  -\sum_{f=0}^{f = f_s/2} PSD(f) log_2[PSD(f)]
    Where:
    :math:`PSD` is the normalised power spectrum (Power Spectrum Density), and
    :math:`f_s` is the sampling frequency
    :param data: a one dimensional floating-point array representing a time series.
    :type data: :class:`~numpy.ndarray` or :class:`~pyrem.time_series.Signal`
    :param sampling_freq: the sampling frequency
    :type sampling_freq:  float
    :param bands: a list of numbers delimiting the bins of the frequency bands. If None the entropy is computed over the whole range of the DFT (from 0 to :math:`f_s/2`)
    :return: the spectral entropy; a scalar
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


def peak_frequency(data):
    w = np.fft.fft(data)
    freqs = np.fft.fftfreq(len(w))
    return freqs.max()


def compute_basic_features(timestamp, data):
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


def computeFeatures(start_time, end_time, time, x, y, z, pid):
    mag = [0] * len(x)  # np.empty([len(x), 1])
    for i, value in enumerate(x):
        mag[i] = math.sqrt(x[i] * x[i] + y[i] * y[i] + z[i] * z[i])

    mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges, mag_power, mag_sp_entropy, mag_peak_freq = compute_basic_features(
        time, mag)
    x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power, x_sp_entropy, x_peak_freq = compute_basic_features(
        time, x)
    y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power, y_sp_entropy, y_peak_freq = compute_basic_features(
        time, y)
    z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power, z_sp_entropy, z_peak_freq = compute_basic_features(
        time, z)

    f = [pid, start_time, end_time, mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges, mag_power,
         mag_sp_entropy, mag_peak_freq]

    f.extend([x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power, x_sp_entropy, x_peak_freq])
    f.extend([y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power, y_sp_entropy, y_peak_freq])
    f.extend([z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power, z_sp_entropy, z_peak_freq])

    return f


def compute_window_features(start_time, end_time, data: List[DataPoint]) -> DataPoint:
    """ Computes feature vector for single window
    :param start_time:
    :param end_time:
    :param data:
    :return: feature vector as DataPoint
    """

    offset = 0
    if len(data)>0:
        offset = data[0].offset
    timestamps = [v.start_time for v in data]
    accel_x = [v.sample[0] for v in data]
    accel_y = [v.sample[1] for v in data]
    accel_z = [v.sample[2] for v in data]
    accel_magnitude = [get_magnitude(value.sample[0], value.sample[1], value.sample[2]) for value in data]

    mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges, mag_power, mag_sp_entropy, mag_peak_freq = \
        compute_basic_features(timestamps, accel_magnitude)
    x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power, x_sp_entropy, x_peak_freq = \
        compute_basic_features(timestamps, accel_x)
    y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power, y_sp_entropy, y_peak_freq = \
        compute_basic_features(timestamps, accel_y)
    z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power, z_sp_entropy, z_peak_freq = \
        compute_basic_features(timestamps, accel_z)

    feature_vector = [mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges, mag_power,
                      mag_sp_entropy,
                      mag_peak_freq]

    feature_vector.extend(
        [x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power, x_sp_entropy, x_peak_freq])
    feature_vector.extend(
        [y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power, y_sp_entropy, y_peak_freq])
    feature_vector.extend(
        [z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power, z_sp_entropy, z_peak_freq])

    return DataPoint(start_time=start_time, end_time=end_time, offset=offset, sample=feature_vector)


def compute_accelerometer_features(accel_stream: DataStream,
                                   window_size: float = 10.0):
    """ Segment data and computes feature vector for each window
    :param accel_stream:
    :param window_size:
    :return: list of feature vectors
    """
    all_features = []

    cur_index = 0
    accel_data = accel_stream.data

    while cur_index < len(accel_data):
        start_index = cur_index
        end_index = cur_index

        accel_window = []
        win_size = timedelta(seconds=window_size)

        while (accel_data[end_index].start_time - accel_data[start_index].start_time) < win_size:
            accel_window.append(accel_data[end_index])
            end_index = end_index + 1
            if end_index >= len(accel_stream.data):
                break

        feature_vector = compute_window_features(accel_window[0].start_time, accel_window[-1].start_time, accel_window)
        all_features.append(feature_vector)

        cur_index = end_index

    # TODO: I will remove this when windowing function works
    # # perform windowing of datastream
    # window_data = window_sliding(accel_stream.data, window_size, window_size)
    # for key, value in window_data.items():
    #     if len(value) > 200:
    #         start_time, end_time = key
    #         feature_list = compute_window_features(start_time, end_time, value)
    #         all_features.append(feature_list)

    return all_features

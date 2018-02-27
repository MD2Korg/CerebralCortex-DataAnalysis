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
from typing import List
from scipy.stats import skew
from scipy.stats import kurtosis
from datetime import timedelta
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

def spectral_entropy(a, sampling_freq, bands=None):

    r"""
    Compute spectral entropy of a  signal with respect to frequency bands.
    The power spectrum is computed through fft. Then, it is normalised and assimilated to a probability density function.
    The entropy of the signal :math:`x` can be expressed by:
    .. math::
        H(x) =  -\sum_{f=0}^{f = f_s/2} PSD(f) log_2[PSD(f)]
    Where:
    :math:`PSD` is the normalised power spectrum (Power Spectrum Density), and
    :math:`f_s` is the sampling frequency
    :param a: a one dimensional floating-point array representing a time series.
    :type a: :class:`~numpy.ndarray` or :class:`~pyrem.time_series.Signal`
    :param sampling_freq: the sampling frequency
    :type sampling_freq:  float
    :param bands: a list of numbers delimiting the bins of the frequency bands. If None the entropy is computed over the whole range of the DFT (from 0 to :math:`f_s/2`)
    :return: the spectral entropy; a scalar
    """
    psd = np.abs(np.fft.rfft(a))**2
    psd /= np.sum(psd) # psd as a pdf (normalised to one)

    if bands is None:
        power_per_band= psd[psd>0]
    else:
        freqs = np.fft.rfftfreq(a.size, 1/float(sampling_freq))
        bands = np.asarray(bands)

        freq_limits_low = np.concatenate([[0.0],bands])
        freq_limits_up = np.concatenate([bands, [np.Inf]])

        power_per_band = [np.sum(psd[np.bitwise_and(freqs >= low, freqs<up)])
                          for low,up in zip(freq_limits_low, freq_limits_up)]

        power_per_band= power_per_band[ power_per_band > 0]

    return - np.sum(power_per_band * np.log2(power_per_band))

def peak_frequency(data):
    w = np.fft.fft(data)
    freqs = np.fft.fftfreq(len(w))
    print(freqs.min(), freqs.max())
    return freqs.max()

def compute_basic_features(timestamp, data):
    mean =  np.mean(data)
    median = np.median(data)
    std = np.std(data)
    skewness = skew(data)
    kurt = kurtosis(data)
    rateOfChanges=get_rate_of_change(timestamp, data)
    power = np.mean([v*v for v in data] )
    sp_entropy = spectral_entropy(data, 25)
    peak_freq = peak_frequency(data)

    return mean, median, std, skewness, kurt, rateOfChanges, power, sp_entropy, peak_freq

def computeFeatures(start_time, end_time, time, x, y, z, pid):

    mag =[0]*len(x)# np.empty([len(x), 1])
    for i in range(len(x)):
        mag[i] = math.sqrt(x[i]*x[i] + y[i]*y[i]+z[i]*z[i])

    mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges, mag_power, mag_sp_entropy, mag_peak_freq = compute_basic_features(time, mag)
    x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power, x_sp_entropy, x_peak_freq = compute_basic_features(time, x)
    y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power, y_sp_entropy, y_peak_freq = compute_basic_features(time, y)
    z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power, z_sp_entropy, z_peak_freq = compute_basic_features(time, z)

    f = [pid, start_time, end_time, mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges, mag_power, mag_sp_entropy, mag_peak_freq]

    f.extend([x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power, x_sp_entropy, x_peak_freq])
    f.extend([y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power, y_sp_entropy, y_peak_freq])
    f.extend([z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power, z_sp_entropy, z_peak_freq])

    return f

def compute_window_features(start_time, end_time, data: List[DataPoint]) -> DataPoint:
    timestamps = [v.start_time for v in data]
    accel_x = [v.sample[0] for v in data]
    accel_y = [v.sample[1] for v in data]
    accel_z = [v.sample[2] for v in data]
    accel_magnitude = [get_magnitude(i.sample[0], i.sample[1], i.sample[2]) for i in data]

    mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges, mag_power, mag_sp_entropy, mag_peak_freq = compute_basic_features(
        timestamps, accel_magnitude)
    x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power, x_sp_entropy, x_peak_freq = compute_basic_features(timestamps, accel_x)
    y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power, y_sp_entropy, y_peak_freq = compute_basic_features(timestamps, accel_y)
    z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power, z_sp_entropy, z_peak_freq = compute_basic_features(timestamps, accel_z)

    f = [mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges, mag_power, mag_sp_entropy, mag_peak_freq]

    f.extend([x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power, x_sp_entropy, x_peak_freq])
    f.extend([y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power, y_sp_entropy, y_peak_freq])
    f.extend([z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power, z_sp_entropy, z_peak_freq])

    return DataPoint(start_time=start_time, end_time=end_time, sample=f)

def compute_accelerometer_features(accel_stream: DataStream,
                           window_size: float = 10.0) -> DataStream:


    all_features = []

    indx = 0
    accl = accel_stream.data

    while(indx < len(accel_stream.data)):
        start_index = indx
        end_index = indx

        accl_window = []
        win_size = timedelta(seconds=window_size)

        while(accl[end_index].start_time - accl[start_index].start_time < win_size):
            accl_window.append(accl[end_index])
            end_index = end_index + 1
            if end_index >= len(accel_stream.data):
                break

        feature_list = compute_window_features(accl_window[0].start_time, accl_window[-1].start_time, accl_window)
        all_features.append(feature_list)

        indx = end_index


    # perform windowing of datastream
    # window_data = window_sliding(accel_stream.data, window_size, window_size)
    # for key, value in window_data.items():
    #     if len(value) > 200:
    #         start_time, end_time = key
    #         feature_list = compute_window_features(start_time, end_time, value)
    #         all_features.append(feature_list)

    all_feature_stream = DataStream.from_datastream([accel_stream])
    all_feature_stream.data = all_features

    return all_feature_stream

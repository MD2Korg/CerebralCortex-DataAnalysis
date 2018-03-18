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

from core.feature.rr_interval.utils.util import *
from core.feature.rr_interval.utils.get_store import *
from cerebralcortex.cerebralcortex import CerebralCortex
from core.signalprocessing.window import window_sliding
from cerebralcortex.core.datatypes.datapoint import DataPoint
import numpy as np
from datetime import datetime
from typing import List,Dict

CC = CerebralCortex()
users = CC.get_all_users("mperf-alabsi")


def find_min_max_ts_of_day(left:List[DataPoint],
                           right:List[DataPoint])->(datetime,datetime):
    """
    Finds the minimum and maximum timestamps of two given array of datapoints

    :param left:
    :param right:
    :return:
    """
    return min(left[0].start_time,right[0].start_time),\
           max(left[-1].start_time,right[-1].start_time)

def find_window_st_et(left:List[DataPoint],
                      right:List[DataPoint],
                      window_size:float=60,
                      window_offset:float=31)->Dict:
    """
    Given two list of datapoints it returns an windowed list of datapoints
    containing only the start and end time

    :param left:
    :param right:
    :return:
    """

    ts_min,ts_max = find_min_max_ts_of_day(left,
                                           right)
    ts_array = np.linspace(ts_min.timestamp(),ts_max.timestamp(),
                           np.round((ts_max.timestamp()-
                                     ts_min.timestamp())*Fs))
    list_of_dp_for_windowing = []

    for ts in ts_array:
        list_of_dp_for_windowing.append(DataPoint.from_tuple(
            start_time=datetime.fromtimestamp(ts),sample=ts))

    windowed_ts_list_of_dp = window_sliding(list_of_dp_for_windowing,
                                            window_size=window_size,
                                            window_offset=window_offset)
    return windowed_ts_list_of_dp

def find_combination(data_left:List[DataPoint],
                 data_right:List[DataPoint],
                 data_left_sample:np.ndarray,
                 data_right_sample:np.ndarray)->np.ndarray:
    """
    For each window of 1 minute combine left and right hand PPG signals.
    If not possible then return the one which has less accelerometer magnitude

    :param data_left:
    :param data_right:
    :param data_left_sample:
    :param data_right_sample:
    :return: (*,3) or (*,6) shaped matrix depending on the result
    """
    ts_array_left = np.array([i.start_time.timestamp() for i in
                              data_left])
    ts_array_right = np.array([i.start_time.timestamp() for i in
                               data_right])
    min_ts = max(data_left[0].start_time.timestamp(),
                 data_right[0].start_time.timestamp())
    max_ts = min(data_left[-1].start_time.timestamp(),
                 data_right[-1].start_time.timestamp())
    index_left = np.where((ts_array_left>=min_ts)
                          & (ts_array_left<=max_ts))[0]
    index_right = np.where((ts_array_right>=min_ts)
                           & (ts_array_right<=max_ts))[0]
    if len(index_left)==len(index_right):
        data_final = np.zeros((len(index_left),6))
        data_final[:,:3] = data_left_sample[index_left,6:]
        data_final[:,3:] = data_right_sample[index_right,6:]
    else:
        accl_mag_left = np.sum(data_left_sample[0,:]**2 +
                               data_left_sample[1,:]**2 +
                               data_left_sample[0,:]**2)

        accl_mag_right = np.sum(data_right_sample[0,:]**2 +
                                data_right_sample[1,:]**2 +
                                data_right_sample[0,:]**2)

        if accl_mag_left < accl_mag_right:
            data_final = data_left_sample[:,6:]
        else:
            data_final = data_right_sample[:,6:]
    return data_final

def find_sample_from_combination_of_left_right_or_one(left:List[DataPoint],
                                                  right:List[DataPoint])-> \
                                                  List[DataPoint]:
    """
    When both left and right PPG are available for the day this function
    windows the whole day into one minute window and decides for each window
    how to combine the left and right wrist ppg signals

    :param left:
    :param right:
    :return:
    """
    windowed_list_of_dp = find_window_st_et(
        left,right,window_size=60,window_offset=31)

    ts_array_left = np.array([i.start_time.timestamp() for i in
                              left])
    ts_array_right = np.array([i.start_time.timestamp() for i in
                               right])
    final_window_list = []
    for key in windowed_list_of_dp.keys():
        index_left = np.where((ts_array_left>=key[0].timestamp())
                              & (ts_array_left<key[1].timestamp()))[0]
        data_left = np.array(decoded_left_raw.data)[index_left]
        data_left_sample = np.array([i.sample for i in data_left])
        index_right = np.where((ts_array_right>=key[0].timestamp())
                               & (ts_array_right<key[1].timestamp()))[0]
        data_right = np.array(decoded_right_raw.data)[index_right]
        data_right_sample = np.array([i.sample for i in data_right])
        if not data_right.all and not data_left.all:
            continue
        elif not data_left.all and len(data_right)>.64*Fs*60:
            data_final = data_right_sample
        elif not data_right.all and len(data_left)>.64*Fs*60:
            data_final = data_left_sample
        elif not data_left.all and len(data_right)<=.64*Fs*60:
            continue
        elif not data_right.all and len(data_left)<=.64*Fs*60:
            continue
        elif len(data_right)<=.64*Fs*60 and len(
                data_left)<=.64*Fs*60:
            continue
        else:
            difference = np.abs(len(data_left)-len(data_right))
            if difference == 0:
                data_final = np.zeros((len(data_left),6))
                data_final[:,:3] = data_left_sample[:,6:]
                data_final[:,3:] = data_right_sample[:,6:]
            elif difference < 5:
                data_final = find_combination(data_left,
                                              data_right,
                                              data_left_sample,
                                              data_right_sample)
            else:
                accl_mag_left = np.sum(data_left_sample[0,:]**2 +
                                       data_left_sample[1,:]**2 +
                                       data_left_sample[0,:]**2)

                accl_mag_right = np.sum(data_right_sample[0,:]**2 +
                                        data_right_sample[1,:]**2 +
                                        data_right_sample[0,:]**2)

                if accl_mag_left/np.shape(data_left_sample)[0] < \
                        accl_mag_right/np.shape(data_right_sample)[0]:
                    data_final = data_left_sample[:,6:]
                else:
                    data_final = data_right_sample[:,6:]
        final_window_list.append(DataPoint.from_tuple(
            start_time=key[0],end_time=key[1],sample=data_final))

    return final_window_list


for user in users[1:2]:
    streams = CC.get_user_streams(user['identifier'])
    user_id = user["identifier"]
    user_data_collection = {}
    if led_decode_left_wrist in streams:
        stream_days_left = get_stream_days(streams[led_decode_left_wrist]["identifier"],
                                           CC)
        stream_days_right = get_stream_days(streams[led_decode_left_wrist][
                                            "identifier"],CC)
        common_days = list(set(stream_days_left) & set(stream_days_right))
        left_only_days = list(set(stream_days_left) - set(stream_days_right))
        right_only_days = list(set(stream_days_right) - set(stream_days_left))
        union_of_days_list = list(set(stream_days_left) | set(
            stream_days_right))

        for day in union_of_days_list:
            if day in common_days:
                decoded_left_raw = CC.get_stream(streams[led_decode_left_wrist][
                                                         "identifier"],
                                                     day=day, user_id=user_id)
                decoded_right_raw = CC.get_stream(streams[
                                                      led_decode_right_wrist][
                                                     "identifier"],
                                                 day=day, user_id=user_id)

                final_windowed_data = find_sample_from_combination_of_left_right_or_one(
                    decoded_left_raw.data,decoded_right_raw.data)
            elif day in left_only_days:
                decoded_left_raw = CC.get_stream(streams[led_decode_left_wrist][
                                                     "identifier"],
                                                 day=day, user_id=user_id)

                windowed_data = window_sliding(decoded_left_raw.data,
                                                     window_size=60,
                                                     window_offset=31)
                final_windowed_data = []
                for key in windowed_data.keys():
                    final_windowed_data.append(DataPoint.from_tuple(
                        start_time=key[0],
                        end_time=key[1],
                        sample = np.array([i.sample[6:] for i in windowed_data[
                        key]])))
            else:
                decoded_right_raw = CC.get_stream(streams[
                                                      led_decode_right_wrist][
                                                      "identifier"],
                                                  day=day, user_id=user_id)

                windowed_data = window_sliding(decoded_right_raw.data,
                                               window_size=60,
                                               window_offset=31)
                final_windowed_data = []
                for key in windowed_data.keys():
                    final_windowed_data.append(DataPoint.from_tuple(
                        start_time=key[0],
                        end_time=key[1],
                        sample = np.array([i.sample[6:] for i in windowed_data[
                            key]])))
            print(final_windowed_data[0])
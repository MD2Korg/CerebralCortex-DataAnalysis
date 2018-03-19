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


from core.signalprocessing.window import window_sliding
from cerebralcortex.core.datatypes.datapoint import DataPoint
import numpy as np
from datetime import datetime
from typing import List,Dict


def find_min_max_ts_of_day(left:List[DataPoint],
                           right:List[DataPoint])->(datetime,datetime):
    """
    Finds the minimum and maximum timestamps of two given array of datapoints

    :param left:
    :param right:
    :return:
    """
    return min(left[0].start_time,right[0].start_time), \
           max(left[-1].start_time,right[-1].start_time)

def find_window_st_et(left:List[DataPoint],
                      right:List[DataPoint],
                      window_size:float=60,
                      window_offset:float=31,
                      Fs:float=25)->Dict:
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

def find_sample_from_combination_of_left_right_or_one(left:List[DataPoint],
                                                      right:List[DataPoint],
                                                      window_size:float=60,
                                                      window_offset:float=31,
                                                      acceptable:float=.64,
                                                      Fs:float=25)-> \
        List[DataPoint]:
    """
    When both left and right PPG are available for the day this function
    windows the whole day into one minute window and decides for each window
    how to combine the left and right wrist ppg signals

    :param left:
    :param right:
    :return:
    """
    # windowed_list_of_dp = find_window_st_et(
    #     left,right,window_size=window_size,window_offset=window_offset,Fs=Fs)

    ts_array_left = np.array([i.start_time.timestamp() for i in
                              left])
    ts_array_right = np.array([i.start_time.timestamp() for i in
                               right])
    final_window_list = []
    initial = min(ts_array_left[0],ts_array_right[0])
    while initial<=max(ts_array_left[-1],ts_array_right[-1]):
        index_left = np.where((ts_array_left>=initial)
                              & (ts_array_left<initial+60))[0]
        data_left = np.array(left)[index_left]
        data_left_sample = np.array([i.sample for i in data_left])
        index_right = np.where((ts_array_right>=initial)
                               & (ts_array_right<initial+60))[0]
        data_right = np.array(right)[index_right]
        data_right_sample = np.array([i.sample for i in data_right])
        initial += 60
        if len(data_right)<=acceptable*Fs*window_size and len(
                data_left)<=acceptable*Fs*window_size:
            continue
        elif len(data_left)<=acceptable*Fs*window_size and len(
                data_right)>acceptable*Fs*window_size:
            data_final = {'right':data_right_sample[:,6:],'left':[]}
        elif len(data_right)<=acceptable*Fs*window_size and len(
                data_left)>acceptable*Fs*window_size:
            data_final = {'right':[],'left':data_left_sample[:,6:]}
        else:
            data_final = {'right':data_right_sample[:,6:],
                          'left':data_left_sample[:,6:]}
            # accl_mag_left = np.sum(data_left_sample[:,0]**2 +
            #                        data_left_sample[:,1]**2 +
            #                        data_left_sample[:,2]**2)
            #
            # accl_mag_right = np.sum(data_right_sample[:,0]**2 +
            #                         data_right_sample[:,1]**2 +
            #                         data_right_sample[:,2]**2)
            #
            # if accl_mag_left/np.shape(data_left_sample)[0] < \
            #         accl_mag_right/np.shape(data_right_sample)[0]:
            #     data_final = data_left_sample[:,6:]
            # else:
            #     data_final = data_right_sample[:,6:]
        final_window_list.append(DataPoint.from_tuple(
            start_time=datetime.fromtimestamp(initial-60),
            end_time=datetime.fromtimestamp(initial),
            sample=data_final))

    return final_window_list

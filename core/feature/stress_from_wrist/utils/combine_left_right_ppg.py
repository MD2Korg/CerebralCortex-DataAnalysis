# Copyright (c) 2018, MD2K Center of Excellence
# All rights reserved.
# author: Md Azim Ullah
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


from cerebralcortex.core.datatypes.datapoint import DataPoint
import numpy as np
from datetime import datetime
from typing import List


def find_sample_from_combination_of_left_right_or_one(left:List[DataPoint],
                                                      right:List[DataPoint],
                                                      window_size:float=60,
                                                      window_offset:float=60,
                                                      acceptable:float=.5,
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
    if not list(left) and not list(right):
        return []
    elif not list(left):
        offset = right[0].offset
    else:
        offset = left[0].offset

    ts_array_left = np.array([i.start_time.timestamp() for i in
                              left])
    ts_array_right = np.array([i.start_time.timestamp() for i in
                               right])
    final_window_list = []
    initial = min(ts_array_left[0],ts_array_right[0])
    while initial<=max(ts_array_left[-1],ts_array_right[-1]):
        index_left = np.where((ts_array_left>=initial)
                              & (ts_array_left<initial+window_size))[0]
        data_left = np.array(left)[index_left]
        data_left_sample = np.array([i.sample for i in data_left])
        index_right = np.where((ts_array_right>=initial)
                               & (ts_array_right<initial+window_size))[0]
        data_right = np.array(right)[index_right]
        data_right_sample = np.array([i.sample for i in data_right])
        initial += window_offset
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

        final_window_list.append(DataPoint.from_tuple(
            start_time=datetime.fromtimestamp(initial-window_offset),
            end_time=datetime.fromtimestamp(initial-window_offset+window_size),
            sample=data_final,offset=offset))

    return final_window_list

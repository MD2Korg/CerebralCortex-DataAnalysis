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

from core.feature.rr_interval.utils.util import Fs,acceptable,window_size,\
    window_offset,led_decode_left_wrist,led_decode_right_wrist
from core.feature.rr_interval.utils.get_store import *
from cerebralcortex.cerebralcortex import CerebralCortex
from core.feature.rr_interval.utils.combine_left_right_ppg import *
from scipy.io import loadmat
from scipy import signal
from pylab import *


def get_constants():
    data = loadmat('./utils/int_RR_dist_obj_kernel_Fs25_11clusters.mat')
    int_RR_dist_obj = data['int_RR_dist_obj']
    data = loadmat('./utils/H_alignedByECGpks(DelayedBy6)_win15_center8.mat')
    H = data['H']
    w_l = np.squeeze(data['w_l'])
    w_r = np.squeeze(data['w_r'])
    fil_type = 'ppg'
    return int_RR_dist_obj,H,w_l,w_r,fil_type

def preProcessing(X0,Fs,fil_type):
    X1 = signal.detrend(X0,axis=0,type='constant')
    if fil_type in ['ppg']:
        b = signal.firls(65,[0,0.3*2/Fs, 0.4*2/Fs, 5*2/Fs ,5.5*2/Fs ,1],[0, 0 ,1 ,1 ,0, 0],
                         [100*0.02,0.02,0.02])
    else:
        b = signal.firls(129,[0,0.3*2/Fs,0.4*2/Fs,1],[0,0,1,1],[100*0.02,0.02])
    for i in range(np.shape(X1)[1]):
        X1[:,i] = np.convolve(X1[:,i],b,'same')
    return X1

CC = CerebralCortex()
users = CC.get_all_users("mperf-alabsi")

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
                    decoded_left_raw.data,decoded_right_raw.data,
                    window_size=window_size,window_offset=window_offset,
                    Fs=Fs,acceptable=acceptable)
            elif day in left_only_days:
                decoded_left_raw = CC.get_stream(streams[led_decode_left_wrist][
                                                     "identifier"],
                                                 day=day, user_id=user_id)

                windowed_data = window_sliding(decoded_left_raw.data,
                                                     window_size=window_size,
                                                     window_offset=window_offset)
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
                                               window_size=window_size,
                                               window_offset=window_offset)
                final_windowed_data = []
                for key in windowed_data.keys():
                    final_windowed_data.append(DataPoint.from_tuple(
                        start_time=key[0],
                        end_time=key[1],
                        sample = np.array([i.sample[6:] for i in windowed_data[
                            key]])))
            int_RR_dist_obj,H,w_l,w_r,fil_type = get_constants()
            for dp in final_windowed_data:
                X_ppg = dp.sample
                t_start = dp.start_time.timestamp()
                t_end = dp.end_time.timestamp()
                Fs_ppg = (np.shape(X_ppg)[0]/(t_end-t_start))
                X_ppg_fil = preProcessing(X0=X_ppg,Fs=Fs_ppg,fil_type=fil_type)

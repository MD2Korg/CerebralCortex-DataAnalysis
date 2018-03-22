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
from core.feature.stress_from_wrist.utils.util import acceptable,window_size, \
    window_offset,led_decode_left_wrist,led_decode_right_wrist,get_constants,\
    get_stream_days,Fs
from cerebralcortex.cerebralcortex import CerebralCortex
from core.feature.stress_from_wrist.utils.combine_left_right_ppg import *
import warnings
warnings.filterwarnings("ignore")
from copy import deepcopy
from core.feature.stress_from_wrist.utils.preprocessing_LED import *
from core.signalprocessing.window import window_sliding
import math
import time

def get_sample_from_comparison(
                            RR_interval_all_realization_r,
                            score_r,HR_r,
                            RR_interval_all_realization_l,
                            score_l,HR_l):
    if math.isnan(score_r):
        sample =  [RR_interval_all_realization_l,
                score_l,HR_l]
    elif math.isnan(score_l):
        sample =  [RR_interval_all_realization_r,
                score_r,HR_r]
    else:
        if score_r<score_l:
            sample =  [RR_interval_all_realization_r,
                    score_r,HR_r]
        else:
            sample =  [RR_interval_all_realization_l,
                    score_l,HR_l]
    return sample


def collect_final_windowed_data(windowed_data,offset):
    final_windowed_data = []
    for key in windowed_data.keys():
        final_windowed_data.append(DataPoint.from_tuple(
            start_time=key[0],
            end_time=key[1],
            sample = np.array([i.sample[6:] for i in windowed_data[
                key]]),offset=offset))
        if np.shape(final_windowed_data[-1].sample) >= 1500:
            final_windowed_data[-1].sample = final_windowed_data[-1].sample[
                                             :1500,:]
    return final_windowed_data

def get_final_windowed_data(left,right,window_size=60,
                            window_offset=60,acceptable=.5,Fs=25):
    if not left:
        offset = right[0].offset
        windowed_data = window_sliding(right,
                                       window_size=window_size,
                                       window_offset=window_offset)
        final_windowed_data  = collect_final_windowed_data(windowed_data,offset)
    elif not right:
        offset = left[0].offset
        windowed_data = window_sliding(left,
                                       window_size=window_size,
                                       window_offset=window_offset)
        final_windowed_data  = collect_final_windowed_data(windowed_data,offset)
    else:
        final_windowed_data = find_sample_from_combination_of_left_right_or_one(
            decoded_left_raw.data,decoded_right_raw.data,
            window_size=window_size,window_offset=window_offset,
            Fs=Fs,acceptable=acceptable)

    return final_windowed_data

def get_RR_interval_score_HR(sample,st,et,int_RR_dist_obj,H,w_l,w_r,fil_type):
    X_ppg = sample
    t_start = st
    t_end = et
    Fs_ppg = (np.shape(X_ppg)[0]/(t_end-t_start))
    X_ppg_fil = preProcessing(X0=X_ppg,Fs=Fs_ppg,fil_type=fil_type)
    RR_interval_all_realization,score,HR = GLRT_bayesianIP_HMM(
                    X_ppg_fil,H,w_r,w_l,[],int_RR_dist_obj)
    return RR_interval_all_realization,score,HR

def get_RR_interval_score_HR_for_all(final_windowed_data):
    int_RR_dist_obj,H,w_l,w_r,fil_type = get_constants()
    final_rr_interval_list = []
    for dp in final_windowed_data[:30]:
        st = dp.start_time.timestamp()
        et = dp.end_time.timestamp()
        if not list(dp.sample['left']) and \
                not list(dp.sample['right']):
            continue
        elif not list(dp.sample['left']):
            try:
                RR_interval_all_realization,score,HR = \
                    get_RR_interval_score_HR(dp.sample['right'],
                                             st,et,int_RR_dist_obj,
                                             H,w_l,w_r,fil_type)
                if not math.isnan(score):
                    final_rr_interval_list.append(deepcopy(dp))
                    final_rr_interval_list[-1].sample = \
                        [RR_interval_all_realization,score,HR]
            except Exception:
                pass
        elif not list(dp.sample['right']):
            try:
                RR_interval_all_realization,score,HR = \
                    get_RR_interval_score_HR(dp.sample['right'],
                                             st,et,int_RR_dist_obj,
                                             H,w_l,w_r,fil_type)
                if not math.isnan(score):
                    final_rr_interval_list.append(deepcopy(dp))
                    final_rr_interval_list[-1].sample = \
                        [RR_interval_all_realization,score,HR]
            except Exception:
                pass
        else:
            try:
                RR_interval_all_realization_r,score_r,HR_r = \
                    get_RR_interval_score_HR(dp.sample['right'],
                                             st,et,int_RR_dist_obj,
                                             H,w_l,w_r,fil_type)
                RR_interval_all_realization_l,score_l,HR_l = \
                    get_RR_interval_score_HR(dp.sample['left'],
                                             st,et,int_RR_dist_obj,
                                             H,w_l,w_r,fil_type)
                if math.isnan(score_r) and math.isnan(score_l):
                    continue
                else:
                    final_sample = get_sample_from_comparison(
                                              RR_interval_all_realization_r,
                                              score_r,HR_r,
                                              RR_interval_all_realization_l,
                                              score_l,HR_l)
                    final_rr_interval_list.append(deepcopy(dp))
                    final_rr_interval_list[-1].sample = final_sample

            except Exception:
                pass
    return final_rr_interval_list

CC = CerebralCortex()
users = CC.get_all_users("mperf-alabsi")
for user in users[1:2]:
    streams = CC.get_user_streams(user['identifier'])
    user_id = user["identifier"]
    if led_decode_left_wrist in streams or led_decode_right_wrist in streams:

        stream_days_left = get_stream_days(streams[led_decode_left_wrist][
                                               "identifier"],
                                           CC)

        stream_days_right = get_stream_days(streams[led_decode_right_wrist][
                                                "identifier"],CC)
        common_days = list(set(stream_days_left) & set(stream_days_right))
        left_only_days = list(set(stream_days_left) - set(stream_days_right))
        right_only_days = list(set(stream_days_right) - set(stream_days_left))
        union_of_days_list = list(set(stream_days_left) | set(
            stream_days_right))
        st = time.time()
        for day in union_of_days_list:
            decoded_left_raw = CC.get_stream(streams[
                                                 led_decode_left_wrist][
                                                 "identifier"],
                                             day=day,
                                             user_id=user_id)
            decoded_right_raw = CC.get_stream(streams[
                                                  led_decode_right_wrist][
                                                  "identifier"],
                                              day=day, user_id=user_id)

            if not decoded_left_raw.data and not decoded_right_raw.data:
                continue
            elif not decoded_left_raw.data:
                final_windowed_data = get_final_windowed_data([],
                                                            decoded_right_raw.data,
                                                            window_size=window_size,
                                                            window_offset=window_offset)
            elif not decoded_right_raw.data:
                final_windowed_data = get_final_windowed_data(
                                        decoded_left_raw.data,[],
                                        window_size=window_size,
                                        window_offset=window_offset)
            else:
                final_windowed_data = get_final_windowed_data(
                                        decoded_left_raw.data,
                                        decoded_right_raw.data,
                                        window_size=window_size,
                                        window_offset=window_offset,
                                        acceptable=acceptable,
                                        Fs=Fs)

            final_rr_interval_list = get_RR_interval_score_HR_for_all(
                                                                final_windowed_data)
            print(final_rr_interval_list,time.time()-st)


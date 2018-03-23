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
import uuid
from datetime import timedelta
import pickle
import core.computefeature
from copy import deepcopy
from core.feature.stress_from_wrist.utils.preprocessing_LED import *
from core.signalprocessing.window import window_sliding
import math
from cerebralcortex.cerebralcortex import CerebralCortex
from core.feature.stress_from_wrist.utils.combine_left_right_ppg import *

Fs = 25
led_decode_left_wrist1 = "org.md2k.signalprocessing.decodedLED.leftwrist"
led_decode_right_wrist1 = "org.md2k.signalprocessing.decodedLED.rightwrist"

window_size = 60
window_offset = 60
acceptable = .5
label_lab = 'LABEL--org.md2k.studymperflab'
motionsense_hrv_left_raw = \
    "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"

led_decode_left_wrist = "org.md2k.feature.decodedLED.leftwrist"
led_decode_right_wrist = "org.md2k.feature.decodedLED.rightwrist"
motionsense_hrv_right_raw = \
    "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
path_to_stress_files = 'core/resources/stress_files/'
path_to_model_files = 'core/resources/models/stress_wrist/'
qualtrics_identifier = \
    "org.md2k.data_qualtrics.feature.stress_MITRE.omnibus_stress_question.daily"


def get_stream_days(stream_id: uuid, CC: CerebralCortex) -> List:
    """
    Returns a list of days (string format: YearMonthDay (e.g., 20171206)
    :param stream_id:
    """
    stream_dicts = CC.get_stream_duration(stream_id)
    stream_days = []
    days = stream_dicts["end_time"]-stream_dicts["start_time"]
    for day in range(days.days+1):
        stream_days.append(
            (stream_dicts["start_time"]+timedelta(days=day)).strftime('%Y%m%d'))
    return stream_days
def get_model():
    model = pickle.loads(core.computefeature.get_resource_contents(
        path_to_model_files+'stress_model.model'))
    scaler = pickle.loads(core.computefeature.get_resource_contents(
        path_to_model_files+'stress_scaler.model'))
    return model,scaler


def get_constants():
    int_RR_dist_obj = pickle.loads(
        core.computefeature.get_resource_contents(path_to_stress_files+'int_RR_dist_obj.p'))
    H = pickle.loads(
        core.computefeature.get_resource_contents(path_to_stress_files+'H.p'))
    w_l = pickle.loads(
        core.computefeature.get_resource_contents(path_to_stress_files+'w_l.p'))
    w_r = pickle.loads(
        core.computefeature.get_resource_contents(path_to_stress_files+'w_r.p'))
    fil_type = 'ppg'
    return int_RR_dist_obj,H,w_l,w_r,fil_type


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


def collect_final_windowed_data(windowed_data,offset,hand1,hand2):
    final_windowed_data = []
    for key in windowed_data.keys():
        if len(windowed_data[key])>.5*25*60:
            final_windowed_data.append(DataPoint.from_tuple(
                start_time=key[0],
                end_time=key[1],
                sample = {hand1:np.array([i.sample[6:] for i in windowed_data[key]]),hand2:[]},
                offset=offset))
            if np.shape(final_windowed_data[-1].sample[hand1])[0] >= 1500:
                final_windowed_data[-1].sample[hand1] = final_windowed_data[-1].sample[hand1][
                                                        :1500,:]
    return final_windowed_data

def get_final_windowed_data(left,right,window_size=60,
                            window_offset=60,acceptable=.5,Fs=25):
    if not left and not right:
        return []

    if not left:
        offset = right[0].offset
        windowed_data = window_sliding(right,
                                       window_size=window_size,
                                       window_offset=window_offset)
        final_windowed_data  = collect_final_windowed_data(windowed_data,offset,'right','left')
    elif not right:
        offset = left[0].offset
        windowed_data = window_sliding(left,
                                       window_size=window_size,
                                       window_offset=window_offset)
        final_windowed_data  = collect_final_windowed_data(windowed_data,offset,'left','right')
    else:
        final_windowed_data = find_sample_from_combination_of_left_right_or_one(
            left,right,
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
    for dp in final_windowed_data:
        st = dp.start_time.timestamp()
        et = dp.end_time.timestamp()
        if len(dp.sample['left'])==0 and \
                len(dp.sample['right'])==0:
            continue
        if len(dp.sample['left'])==0:
            try:

                print(dp.sample['right'])
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
        elif len(dp.sample['right'])==0:
            try:
                RR_interval_all_realization,score,HR = \
                    get_RR_interval_score_HR(dp.sample['left'],
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

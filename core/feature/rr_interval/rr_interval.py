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
from core.feature.motionsenseHRVdecode.motionsenseHRVdecode import DecodeHRV
from core.feature.rr_interval.utils.util import Fs,acceptable,window_size,\
    window_offset,led_decode_left_wrist,led_decode_right_wrist,\
    led_decode_left_wrist1,led_decode_right_wrist1
from core.feature.rr_interval.utils.get_store import *
from cerebralcortex.cerebralcortex import CerebralCortex
from core.feature.rr_interval.utils.combine_left_right_ppg import *
from scipy.io import loadmat
from scipy import signal
from sklearn.preprocessing import normalize
from core.feature.rr_interval.utils.JU_code import Bayesian_IP_memphis
import warnings
warnings.filterwarnings("ignore")
import pickle
import pandas as pd
from copy import deepcopy
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

def get_GLRT(X_ppg,H0,w_r,w_l):
    H_mean = H0[:,0]
    eig_num_H = 1
    H = H0[:,:eig_num_H]
    I = np.eye(np.shape(H)[0],np.shape(H)[0])
    Proj_H = np.matmul(H/np.matmul(H.T,H),H.T)
    GLRT_prod = []
    for k in range(w_l):
        GLRT_prod.append(0)
    for i in range(w_l,np.shape(X_ppg)[0]-w_r,1):
        y = normalize(signal.detrend(X_ppg[i-w_l:i+w_r+1,:],axis = 0,
                                 type='constant'),axis=0,norm='max')
        GLRT = np.diag(np.matmul(y.T,y))/np.diag(np.matmul(np.matmul(y.T,(I-Proj_H)),y))
        GLRT[np.where(np.matmul(y.T,H_mean)<0)[0]] = 1
        if np.shape(X_ppg)[1]==6:
            GLRT_prod.append(np.prod(GLRT)**(3**(-1)))
        else:
            GLRT_prod.append(np.prod(GLRT))
    return np.array(GLRT_prod).reshape((len(GLRT_prod),1))
def get_candidatePeaks(G_static):
    Candidates_position = []
    Candidates_LR = []
    for i in range(2,len(G_static)-2,1):
        if G_static[i-1,0] < G_static[i,0] and G_static[i,0] > G_static[i+1,0]:
            Candidates_position.append(i)
            Candidates_LR.append(G_static[i,0])
    return Candidates_position,Candidates_LR

def get_RRinter_cell(Peak_mat, Candidates_position,Z_output,int_RR_dist_obj):
    RR_interval_perrealization = []
    Delta_max = np.shape(int_RR_dist_obj[0,0])[1]
    for i in range(np.shape(Peak_mat)[0]):
        windowed_peak = np.array(Candidates_position)[np.where(Peak_mat[i,:]==1)[0]]
        if not list(windowed_peak):
            continue
        RR_Row = np.diff(windowed_peak)
        RR_Row = RR_Row[np.where(RR_Row<=Delta_max)[0]]
        RR_row_prob = int_RR_dist_obj[np.int8(Z_output[i,0]),2][0,RR_Row]
        RR_Row[np.where(RR_row_prob == 2)[0]] = 0.5*RR_Row[np.where(RR_row_prob == 2)[0]]
        RR_interval_perrealization.append(RR_Row[1:])
    return RR_interval_perrealization

def GLRT_bayesianIP_HMM(X_ppg_input,H,w_r,w_l,pks_ecg,int_RR_dist_obj):
    Fs = 25
    G_statistics = get_GLRT(X_ppg_input,H,w_r,w_l)
    Candidates_position,Candidates_LR = get_candidatePeaks(G_statistics)
    start_position = 0
    end_position = len(G_statistics)-1
    window_length = end_position-start_position+1
    Peak_mat,output_Z = Bayesian_IP_memphis(Candidates_position,Candidates_LR,int_RR_dist_obj,start_position,end_position)
    RR_interval_all_realization = get_RRinter_cell(Peak_mat, Candidates_position,output_Z,int_RR_dist_obj)
    HR_perRealization = [[0]]*len(RR_interval_all_realization)
    for i in range(len(RR_interval_all_realization)):
        HR_perRealization[i] = Fs*60/np.mean(RR_interval_all_realization[i])
    HR_bayesian = np.mean(HR_perRealization)
    HR_step = np.floor(2*Fs)
    HR_window = np.floor(8*Fs)
    HR = np.zeros((1,np.int64(np.floor((window_length-HR_window)/HR_step))))
    score_8sec = np.zeros((1,np.int64(np.floor((window_length-HR_window)/HR_step))))
    Delta_max = np.shape(int_RR_dist_obj[0,0])[1]

    for HR_window_idx in range(np.shape(HR)[1]):
        RR_row_mean = []
        for i in range(np.shape(Peak_mat)[0]):
            windowed_peak = np.array(Candidates_position)[np.where(Peak_mat[i,:]==1)[0]]
            if not list(windowed_peak):
                continue
            windowed_peak = windowed_peak[np.where((windowed_peak >= HR_step*(HR_window_idx)) & (windowed_peak <= (HR_step*(HR_window_idx) + HR_window)))[0]]
            if not list(windowed_peak):
                continue
            RR_Row = np.diff(windowed_peak)
            RR_Row = RR_Row[np.where(RR_Row<=Delta_max)[0]]
            RR_row_prob = int_RR_dist_obj[np.int(output_Z[i,0]),2][0,RR_Row]
            RR_row_non_zero_prob_1x = RR_Row[np.where(RR_row_prob == 1)[0]]
            RR_row_mean.append(np.mean(RR_row_non_zero_prob_1x))
        if not list(RR_row_mean):
            HR[0,HR_window_idx] = np.nan
            score_8sec[0,HR_window_idx] = np.nan
            continue
        RR_col_mean = np.nanmean(Fs*60/np.array(RR_row_mean))
        HR[0,HR_window_idx] = RR_col_mean
        score_8sec[0,HR_window_idx] = np.nanstd(Fs*60/np.array(RR_row_mean))
    score = np.nanmean(score_8sec)
    return RR_interval_all_realization,score,HR

def get_stress_marks_window(data,user,day,st,et):
    for l in data:
        if l[0]==user and str(l[1])==day:
            # print()
            # l[3] = datetime.utcfromtimestamp(l[3])
            if st>=datetime.utcfromtimestamp(l[2]) and \
                    et<=datetime.utcfromtimestamp(l[3]):
                return l[4]
            # if st<l[2] and et>l[2] and l[2]-st>25 and et<=l[3]:
            #     return l[4]
            # if st>=l[2] and et>l[3] and l[3]-st>25 and st<l[3]:
            #     return l[4]
    return 2
count = 494
CC = CerebralCortex()
users = CC.get_all_users("mperf-alabsi")
user_data_collection = pickle.load(open('data2.p','rb'))
lab_stress_marks = pd.read_csv('./utils/stress.csv',header=None,
                               sep=',').as_matrix()
x = DecodeHRV()
for user in users:
    streams = CC.get_user_streams(user['identifier'])
    user_id = user["identifier"]
    if user_id not in list(lab_stress_marks[:,0]):
        continue
    # user_data_collection[user_id] = {}
    if led_decode_left_wrist1 in streams:

        stream_days_left = get_stream_days(streams[led_decode_left_wrist1][
                                               "identifier"],
                                           CC)

        stream_days_right = get_stream_days(streams[led_decode_right_wrist1][
                                            "identifier"],CC)
        common_days = list(set(stream_days_left) & set(stream_days_right))
        left_only_days = list(set(stream_days_left) - set(stream_days_right))
        right_only_days = list(set(stream_days_right) - set(stream_days_left))
        union_of_days_list = list(set(stream_days_left) | set(
            stream_days_right))

        for day in union_of_days_list:
            # if day in common_days:
            #     decoded_left_raw = CC.get_stream(streams[
            #                                          led_decode_left_wrist1][
            #                                              "identifier"],
            #                                          day=day, user_id=user_id)
            #     decoded_right_raw = CC.get_stream(streams[
            #                                           led_decode_right_wrist1][
            #                                          "identifier"],
            #                                      day=day, user_id=user_id)
            #
            #     final_windowed_data = find_sample_from_combination_of_left_right_or_one(
            #         decoded_left_raw.data,decoded_right_raw.data,
            #         window_size=window_size,window_offset=window_offset,
            #         Fs=Fs,acceptable=acceptable)
            #     print(final_windowed_data[-1].start_time,decoded_left_raw.data[
            #         -1].start_time,decoded_right_raw.data[-1].start_time)
            # elif day in left_only_days:
            #     decoded_left_raw = CC.get_stream(streams[led_decode_left_wrist][
            #                                          "identifier"],
            #                                      day=day, user_id=user_id)
            #
            #     windowed_data = window_sliding(decoded_left_raw.data,
            #                                          window_size=window_size,
            #                                          window_offset=window_offset)
            #     final_windowed_data = []
            #     for key in windowed_data.keys():
            #         final_windowed_data.append(DataPoint.from_tuple(
            #             start_time=key[0],
            #             end_time=key[1],
            #             sample = np.array([i.sample[6:] for i in windowed_data[
            #             key]])))
            # else:
            #     decoded_right_raw = CC.get_stream(streams[
            #                                           led_decode_right_wrist][
            #                                           "identifier"],
            #                                       day=day, user_id=user_id)
            #
            #     windowed_data = window_sliding(decoded_right_raw.data,
            #                                    window_size=window_size,
            #                                    window_offset=window_offset)
            #     final_windowed_data = []
            #     for key in windowed_data.keys():
            #         final_windowed_data.append(DataPoint.from_tuple(
            #             start_time=key[0],
            #             end_time=key[1],
            #             sample = np.array([i.sample[6:] for i in windowed_data[
            #                 key]])))

            final_windowed_data = user_data_collection[user_id][
                day]
            int_RR_dist_obj,H,w_l,w_r,fil_type = get_constants()
            for dp in final_windowed_data:
                try:
                    st = dp.start_time
                    et = dp.end_time
                    label = get_stress_marks_window(lab_stress_marks,user_id,day,st,et)
                    if label ==2:
                        continue
                    print(label,np.shape(dp.sample['left']),np.shape(
                        dp.sample['right']),st,et,user_id)
                    if not list(dp.sample['left']):
                        print('0000000000000000000000000000000')
                        X_ppg = dp.sample['right']
                        t_start = dp.start_time.timestamp()
                        t_end = dp.end_time.timestamp()
                        Fs_ppg = (np.shape(X_ppg)[0]/(t_end-t_start))
                        X_ppg_fil = preProcessing(X0=X_ppg,Fs=Fs_ppg,fil_type=fil_type)
                        RR_interval_all_realization,score,HR = GLRT_bayesianIP_HMM(
                            X_ppg_fil,H,w_r,w_l,[],int_RR_dist_obj)
                        if not RR_interval_all_realization:
                            continue
                        print('saving only')
                        data = np.array([RR_interval_all_realization,score,HR,label,
                                         user_id,day])
                        print(score)
                        np.savez('./windows1/'+str(count),data=data)
                        count+=1
                    elif not list(dp.sample['right']):
                        print('11111111111111111111111111111')
                        X_ppg = dp.sample['left']
                        t_start = dp.start_time.timestamp()
                        t_end = dp.end_time.timestamp()
                        Fs_ppg = (np.shape(X_ppg)[0]/(t_end-t_start))
                        X_ppg_fil = preProcessing(X0=X_ppg,Fs=Fs_ppg,fil_type=fil_type)
                        RR_interval_all_realization,score,HR = GLRT_bayesianIP_HMM(
                            X_ppg_fil,H,w_r,w_l,[],int_RR_dist_obj)
                        if not RR_interval_all_realization:
                            continue
                        print('saving only')
                        data = np.array([RR_interval_all_realization,score,HR,label,
                                         user_id,day])
                        print(score)
                        np.savez('./windows1/'+str(count),data=data)
                        count+=1
                    elif np.shape(dp.sample['left'])[0]>0 and np.shape(
                            dp.sample['right'])[0]>0:

                        X_ppg = deepcopy(dp.sample['left'])
                        print(X_ppg)
                        np.savetxt('./windows1/left.csv',X_ppg,delimiter=',')
                        t_start = dp.start_time.timestamp()
                        t_end = dp.end_time.timestamp()
                        Fs_ppg = (np.shape(X_ppg)[0]/(t_end-t_start))
                        print(Fs_ppg)
                        X_ppg_fil = preProcessing(X0=X_ppg,Fs=Fs_ppg,fil_type=fil_type)
                        RR_interval_all_realization_l,score_l,\
                        HR_l = GLRT_bayesianIP_HMM(
                            X_ppg_fil,H,w_r,w_l,[],int_RR_dist_obj)

                        X_ppg = deepcopy(dp.sample['right'])
                        t_start = dp.start_time.timestamp()
                        t_end = dp.end_time.timestamp()
                        Fs_ppg = (np.shape(X_ppg)[0]/(t_end-t_start))
                        print(Fs_ppg)
                        X_ppg_fil = preProcessing(X0=X_ppg,Fs=Fs_ppg,fil_type=fil_type)


                        RR_interval_all_realization_r,score_r, \
                        HR_r = GLRT_bayesianIP_HMM(
                            X_ppg_fil,H,w_r,w_l,[],int_RR_dist_obj)
                        print(score_l,score_r)
                        if not RR_interval_all_realization_l and not \
                                RR_interval_all_realization_r:
                            continue
                        elif not RR_interval_all_realization_l:
                            data = np.array([RR_interval_all_realization_r,
                                             score_r,HR_r,label,
                                             user_id,day])
                            np.savez('./windows1/'+str(count),data=data)
                            count+=1
                            print(score_r)
                            print('saving right')
                            np.savetxt('./windows1/left.csv',dp.sample['right'])
                            print('stopppppppppppppppppppppp')
                            break
                        elif not RR_interval_all_realization_r:
                            data = np.array([RR_interval_all_realization_l,
                                             score_l,HR_l,label,
                                             user_id,day])
                            np.savez('./windows1/'+str(count),data=data)
                            count+=1
                            print(score_l)
                            print('saving left')
                        else:
                            print('saving_comparison')

                            data = np.array([RR_interval_all_realization_l,
                                             score_l,HR_l,label,
                                             user_id,day])
                            np.savez('./windows1/'+str(count),data=data)
                            print(score_l)
                            count+=1
                            data = np.array([RR_interval_all_realization_r,
                                             score_r,HR_r,label,
                                             user_id,day])
                            np.savez('./windows1/'+str(count),data=data)
                            print(score_r)
                            count+=1

                except Exception:
                    pass
print(count)
# pickle.dump(user_data_collection,open('data2.p','wb'))
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
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from cerebralcortex.core.datatypes.datapoint import DataPoint
from typing import List
import pickle
import core.computefeature
import numpy as np
from scipy import signal,interpolate
from datetime import datetime
from core.feature.rr_interval.utils.JU_code import Bayesian_IP_memphis
from sklearn.preprocessing import normalize
from copy import deepcopy
import pytz

motionsense_hrv_left_raw = \
    "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
motionsense_hrv_right_raw = \
    "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
qualtrics_identifier = \
    "org.md2k.data_qualtrics.feature.v6.stress_MITRE.omnibus_stress_question.daily"
motionsense_hrv_left_raw_cat = \
    "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
motionsense_hrv_right_raw_cat = \
    "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"

path_to_stress_files = 'core/resources/stress_files/'



def admission_control(data:List[DataPoint])->List[DataPoint]:
    final_data = []
    for dp in data:
        if isinstance(dp.sample,str) and len(dp.sample.split(','))==20:
            final_data.append(dp)
        if isinstance(dp.sample,list) and len(dp.sample)==20:
            final_data.append(dp)
    return final_data


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


def bandpassfilter(x,fs):
    """
    
    :param x: a list of samples 
    :param fs: sampling frequency
    :return: filtered list
    """
    x = signal.detrend(x)
    b = signal.firls(129,[0,0.6*2/fs,0.7*2/fs,3*2/fs,3.5*2/fs,1],[0,0,1,1,0,0],[100*0.02,0.02,0.02])
    return signal.convolve(x,b,'valid')

def isDatapointsWithinRange(red,infrared,green):
    a =  len(np.where((red >= 14000)& (red<=170000))[0]) < .64*len(red)
    b = len(np.where((infrared >= 100000)& (infrared<=255000))[0]) < .64*len(infrared)
    c = len(np.where((green >= 800)& (green<=20000))[0]) < .64*len(green)
    if a and b and c:
        return False
    return True

def compute_quality(red,infrared,green,fs):
    """
    
    :param window: a window containing list of datapoints 
    :return: an integer reptresenting the status of the window 0= attached, 1 = not attached
    """

    if not isDatapointsWithinRange(red,infrared,green):
        return False

    if np.mean(red) < 5000 and np.mean(infrared) < 5000 and np.mean(green)<5000:
        return False

    if not (np.mean(red)>np.mean(green) and np.mean(infrared)>np.mean(red)):
        return False

    diff = 30000
    if np.mean(red)>140000 or np.mean(red)<=30000:
        diff = 11000

    if not (np.mean(red) - np.mean(green) > diff and np.mean(infrared) - np.mean(red) >diff):
        return False

    if np.std(bandpassfilter(red,fs)) <= 5 and np.std(bandpassfilter(infrared,fs)) <= 5 and np.std(bandpassfilter(green,fs)) <= 5:
        return False

    return True


def decode_only(data):
    final_data = []
    for dp in data:
        if isinstance(dp.sample,str):
            str_sample = str(dp.sample)
            str_sample_list = str_sample.split(',')
            if len(str_sample_list) != 20:
                continue
            Vals = [np.int8(np.float(val)) for val in str_sample_list]
        elif isinstance(dp.sample,list):
            Vals = [np.int8(val) for val in dp.sample]
        else:
            continue
        sample = np.array([0]*5)
        sample[0] = dp.start_time.timestamp()*1000
        sample[1] = ((np.uint8(Vals[18]) & int('00000011',2))<<8) | \
                    (np.uint8(Vals[19]))
        sample[2] = (np.uint8(Vals[12])<<10) | (np.uint8(Vals[13])<<2) | \
                    ((np.uint8(Vals[14]) & int('11000000',2))>>6)
        sample[3] =((np.uint8(Vals[14]) & int('00111111',2))<<12) | \
                   (np.uint8(Vals[15])<<4) | \
                   ((np.uint8(Vals[16]) & int('11110000',2))>>4)
        sample[4] = ((np.uint8(Vals[16]) & int('00001111',2))<<14) | \
                    (np.uint8(Vals[17])<<6) | \
                    ((np.uint8(Vals[18]) & int('11111100',2))>>2)
        final_data.append(deepcopy(dp))
        final_data[-1].sample = sample
    return final_data


def preProcessing(X0,Fs,fil_type):
    X1 = signal.detrend(X0,axis=0,type='constant')
    if fil_type in ['ppg']:
        b = signal.firls(65,[0,0.3*2/Fs, 0.4*2/Fs, 5*2/Fs ,5.5*2/Fs ,1],[0, 0 ,1 ,1 ,0, 0],
                         [100*0.02,0.02,0.02])
    else:
        b = signal.firls(129,[0,0.3*2/Fs,0.4*2/Fs,1],[0,0,1,1],[100*0.02,0.02])
    X2 = np.zeros((np.shape(X1)[0]-len(b)+1,np.shape(X1)[1]))
    for i in range(np.shape(X1)[1]):
        X2[:,i] = signal.convolve(X1[:,i],b,mode='valid')
    return X2


def get_interp_PPG(PPG,tStamp_start,Fs_ppg):
    tStamp = PPG[:,0]
    counter = PPG[:,1]
    LED = signal.detrend(PPG[:,2:],axis = 0,type='constant')
    c_n = 0
    index_seq = np.ones((len(counter),1))
    index_seq[0,0] = counter[0]+1
    for i in range(1,len(counter),1):
        if counter[i]-counter[i-1]<0:
            c_n += 1
        index_seq[i,0] = 1024*c_n+counter[i]+1
    if np.max(np.diff(index_seq[:,0]))>=5*Fs_ppg:
        LED_interp = []
    else:
        index_seq[:,0] = index_seq[:,0]-index_seq[0,0]
        tStamp_new = index_seq[:,0]*1000/Fs_ppg+ tStamp[0]
        tStamp_final = np.linspace(index_seq[0,0],index_seq[-1,0],np.int64(index_seq[-1,0]+1))*1000/Fs_ppg+ tStamp_start
        f = interpolate.interp1d(tStamp_new, LED, axis = 0,kind ='linear' ,fill_value='extrapolate')
        LED_interp = f(tStamp_final)
    return LED_interp





def get_inputData_pksECG(PPG_L,PPG_R,ECG_raw,counter_ecg,tStamp_ecg,tStamp_start,tStamp_end,Fs_ppg,Fs_ecg):
    idx_tStamp_L = np.where((PPG_L[:,0]>=tStamp_start) & (PPG_L[:,0]<=tStamp_end))[0]
    idx_tStamp_R = np.where((PPG_R[:,0]>=tStamp_start) & (PPG_R[:,0]<=tStamp_end))[0]
    LED_interp_L = get_interp_PPG(PPG_L[idx_tStamp_L,:],tStamp_start,Fs_ppg)
    LED_interp_R = get_interp_PPG(PPG_R[idx_tStamp_R,:],tStamp_start,Fs_ppg)
    if not list(LED_interp_L) and not list(LED_interp_R):
        return []
    if not list(LED_interp_L):
        LED_interp_L = LED_interp_R
    elif not list(LED_interp_R):
        LED_interp_R = LED_interp_L
    LED_interp_L_fil = preProcessing(LED_interp_L,Fs_ppg,'ppg')
    LED_interp_R_fil = preProcessing(LED_interp_R,Fs_ppg,'ppg')
    length_LED = min(np.shape(LED_interp_L_fil)[0],np.shape(LED_interp_R_fil)[0])
    LED_input = np.zeros((length_LED,6))
    LED_input[:,:3] = LED_interp_L_fil[:length_LED,:]
    LED_input[:,3:] = LED_interp_R_fil[:length_LED,:]
    return LED_input


def find_sample_from_combination_of_left_right(left:List[DataPoint],
                                               right:List[DataPoint],
                                               window_size:float=60000,
                                               window_offset:float=60000,
                                               acceptable:float=.5,
                                               Fs:float=25)-> List[DataPoint]:
    """
    When both left and right PPG are available for the day this function
    windows the whole day into one minute window and decides for each window
    how to combine the left and right wrist ppg signals. Then it preprocesses the combined 
    values to get the required input to BayesianIP based rr interval extraction method

    :param left:
    :param right:
    :return:
    """
    if not list(left) and not list(right):
        return []

    elif not list(left):
        left = right
        offset = right[0].offset
    elif not list(right):
        right = left
        offset = left[0].offset
    else:
        offset = left[0].offset

    ts_array_left = np.array([i.start_time.timestamp()*1000 for i in
                              left])
    ts_array_right = np.array([i.start_time.timestamp()*1000 for i in
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
        if np.shape(data_left_sample)[0] < acceptable*window_size*Fs/1000 \
                and np.shape(data_left_sample)[0] < acceptable*window_size*Fs/1000:
            continue

        if np.shape(data_left_sample)[0] < acceptable*window_size*Fs/1000:
            data_left_sample = data_right_sample
        elif np.shape(data_right_sample)[0] < acceptable*window_size*Fs/1000:
            data_right_sample = data_left_sample

        left_quality = compute_quality(data_left_sample[:,2],data_left_sample[:,3],data_left_sample[:,4],Fs)
        right_quality = compute_quality(data_right_sample[:,2],data_right_sample[:,3],data_right_sample[:,4],Fs)

        if not left_quality and not right_quality:
            continue
        if not left_quality:
            data_left_sample = data_right_sample
        elif not right_quality:
            data_right_sample = data_left_sample
        t_start = max(data_left_sample[0,0],data_right_sample[0,0])
        t_end = max(data_left_sample[-1,0],data_right_sample[-1,0])
        LED_input = get_inputData_pksECG(data_left_sample,data_right_sample,0,0,0,t_start,t_end,Fs,100)
        if not list(LED_input):
            continue
        start = (initial-window_offset)/1000
        end = (initial-window_offset+window_size)/1000
        final_window_list.append(DataPoint.from_tuple(start_time = datetime.utcfromtimestamp(start).replace(tzinfo=pytz.UTC),
                                                      offset=offset,
                                                      end_time = datetime.utcfromtimestamp(end).replace(tzinfo=pytz.UTC),
                                                      sample = LED_input))

    return final_window_list

def get_GLRT(X_ppg,H0,w_r,w_l):
    H_mean = H0[:,0]
    eig_num_H = 1
    H = H0[:,:eig_num_H]
    I = np.eye(np.shape(H)[0],np.shape(H)[0])
    temp = H/np.matmul(H.T,H)
    Proj_H = np.matmul(temp,H.T)
    GLRT_prod = []
    for k in range(w_l):
        GLRT_prod.append(0)
    for i in range(w_l,np.shape(X_ppg)[0]-w_r,1):
        y = normalize(signal.detrend(X_ppg[i-w_l:i+w_r+1,:],axis = 0,type='constant'),axis=0,norm='l2')
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
        RR_Row = np.diff(windowed_peak)
        RR_Row = RR_Row[np.where(RR_Row<=Delta_max)[0]]
        RR_row_prob = int_RR_dist_obj[Z_output[i,0],2][0,RR_Row]
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
            windowed_peak = windowed_peak[np.where((windowed_peak >= HR_step*(HR_window_idx)) & (windowed_peak <= (HR_step*(HR_window_idx) + HR_window)))[0]]
            RR_Row = np.diff(windowed_peak)
            RR_Row = RR_Row[np.where(RR_Row<=Delta_max)[0]]
            RR_row_prob = int_RR_dist_obj[output_Z[i,0],2][0,RR_Row]
            RR_row_non_zero_prob_1x = RR_Row[np.where(RR_row_prob == 1)[0]]
            RR_row_mean.append(np.mean(RR_row_non_zero_prob_1x))
        RR_col_mean = np.nanmean(Fs*60/np.array(RR_row_mean))
        HR[0,HR_window_idx] = RR_col_mean
        score_8sec[0,HR_window_idx] = np.nanstd(Fs*60/np.array(RR_row_mean))
    score = np.nanmean(score_8sec)
    return RR_interval_all_realization,score,HR





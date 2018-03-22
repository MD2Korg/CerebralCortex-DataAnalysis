# Copyright (c) 2018, MD2K Center of Excellence
# All rights reserved.
# author: Md Azim Ullah (a translation of matlab code from Ju Gao
# and Teng Diyan(Ohio State))
#  Redistribution and use in source and binary forms,
#  with or without
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

from scipy import signal
from sklearn.preprocessing import normalize
from core.feature.stress_from_wrist.utils.JU_code import Bayesian_IP_memphis
import numpy as np

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

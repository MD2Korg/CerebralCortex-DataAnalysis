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

from scipy.stats import mode
import numpy as np


def HMM_forward_smoothie(Candidates_LR,Candidates_position,int_RR_dist_obj,start_position):
    Candidates_position = np.array(Candidates_position) - start_position + 1
    C_N = len(Candidates_LR)
    Delta_max = np.shape(int_RR_dist_obj[0,0])[1]
    Z_size = np.shape(int_RR_dist_obj)[0]
    log_Gamma_R = np.zeros((Z_size,C_N))
    for z in range(Z_size):
        pi_R_z = np.squeeze(1/np.dot(int_RR_dist_obj[z,0],int_RR_dist_obj[z,1].T))
        for i in range(C_N):
            if Candidates_position[i] <= Delta_max:
                j_range = np.linspace(0,C_N-1,C_N) < i
                if i == 0:
                    log_Gamma_R[z,i] = np.log(Candidates_LR[i])+np.log(pi_R_z*np.sum(int_RR_dist_obj[z,0][0,(Candidates_position[i]-1):]))
                else:
                    log_Gamma_R[z,i] = np.log(Candidates_LR[i]*np.sum(np.exp(log_Gamma_R[z,j_range])*(int_RR_dist_obj[z,0][0,Candidates_position[i] - Candidates_position[j_range]-1]))+pi_R_z*np.sum(int_RR_dist_obj[z,0][0,(Candidates_position[i]-1):]))
            else:
                j_range = np.logical_and((Candidates_position[i] - Candidates_position) <= Delta_max,(Candidates_position[i] - Candidates_position) > 0)
                if np.sum(int_RR_dist_obj[z,0][0,Candidates_position[i] - Candidates_position[j_range]-1]) == 0:
                    log_Gamma_R[z,i] = -np.inf
                else:
                    log_Gamma_R[z,i] = np.log(Candidates_LR[i])+np.log(np.sum(np.exp(log_Gamma_R[z,j_range])*
                                                                              (int_RR_dist_obj[z,0][0,Candidates_position[i] - Candidates_position[j_range]-1])))

    return log_Gamma_R

def HMM_stochastic_backward_smoothie(log_Gamma_R,Candidates_position,int_RR_dist_obj,Z_L,start_position,end_position):
    C_N = len(Candidates_position)
    Delta_max = np.shape(int_RR_dist_obj[0,0])[1]
    Candidates_position = np.array(Candidates_position) - start_position
    end_position = end_position - start_position
    start_position = 0
    P_FSB_Z = np.zeros((1,C_N))
    L = len(Z_L)
    score = np.zeros((L,2))
    Tau_R = np.zeros((L,C_N))
    log_Omega_N = np.zeros((L,C_N))
    peak_mat = np.zeros((L,C_N))
    continue_flag = 0
    l = -1
    while l < L-1:
        l = l + 1
        if continue_flag == 1:
            continue_flag = 0
        pointer_position = 'ab'
        pointer_i = []
        for i in range(C_N-1,-1,-1):
            if pointer_position == 'ab':
                if end_position - Candidates_position[i] > Delta_max-1:
                    continue_flag = 1
                    continue
                Tau_R[l,i] = np.sum(int_RR_dist_obj[Z_L[l]-1,0][0,(end_position - Candidates_position[i]):])
                j_range = np.logical_and((end_position - Candidates_position) < Delta_max,
                                         np.logical_and((end_position - Candidates_position) > 0,
                                                        Candidates_position < Candidates_position[i]))
                if np.sum(j_range)==0:
                    log_Omega_N[l,i] = -np.inf
                else:
                    Candidates_j_interval = end_position - Candidates_position[j_range]
                    p_j_range_temp = np.zeros((1,np.sum(j_range)))
                    for j_range_idx in range(np.int64(np.sum(j_range))):
                        p_j_range_temp[0,j_range_idx] = np.sum(int_RR_dist_obj[Z_L[l]-1,0][0,(Candidates_j_interval[j_range_idx]):])
                    log_Omega_N[l,i] = np.log(np.sum(np.exp(log_Gamma_R[Z_L[l]-1,j_range])*p_j_range_temp))
            else:
                if Candidates_position[i] > Delta_max - 1:
                    if pointer_position - Candidates_position[i] > Delta_max-1:
                        continue_flag = 1
                        continue
                    Tau_R[l,i]= (Tau_R[l,pointer_i])*(int_RR_dist_obj[Z_L[l]-1,0][0,pointer_position - Candidates_position[i]-1])
                    j_range = np.logical_and((pointer_position - Candidates_position + 1) <= Delta_max,
                                             np.logical_and((pointer_position - Candidates_position + 1) > 0,
                                                            Candidates_position < Candidates_position[i]))
                    if np.sum(j_range)==0:
                        log_Omega_N[l,i] = -np.inf
                    else:
                        Candidates_j_interval = pointer_position - Candidates_position[j_range]
                        p_j_range_temp = np.zeros((1,np.sum(j_range)))
                        for j_range_idx in range(np.int64(np.sum(j_range))):
                            p_j_range_temp[0,j_range_idx] = int_RR_dist_obj[Z_L[l]-1,0][0,Candidates_j_interval[j_range_idx]-1]
                        log_Omega_N[l,i] = np.log(Tau_R[l,pointer_i])+np.log(np.sum(np.exp(log_Gamma_R[Z_L[l]-1,j_range])*p_j_range_temp))
                else:
                    if pointer_position - Candidates_position[i] > Delta_max -1:
                        continue_flag = 1
                        continue
                    Tau_R[l,i] = Tau_R[l,pointer_i]*(int_RR_dist_obj[Z_L[l]-1,0][0,pointer_position - Candidates_position[i]-1])
                    if i == 0:
                        log_Omega_N[l,i] = np.log(Tau_R[l,pointer_i])+np.log(np.sum(int_RR_dist_obj[Z_L[l]-1,0][0,pointer_position:]))
                    else:
                        j_range = np.logical_and((pointer_position - Candidates_position + 1) <= Delta_max,
                                                 np.logical_and((pointer_position - Candidates_position + 1 ) > 0,
                                                                Candidates_position < Candidates_position[i]))
                        if np.sum(j_range)==0:
                            log_Omega_N[l,i] = -np.inf
                        else:
                            Candidates_j_interval = pointer_position - Candidates_position[j_range]
                            p_j_range_temp = np.zeros((1,np.sum(j_range)))
                            for j_range_idx in range(np.int64(np.sum(j_range))):
                                p_j_range_temp[0,j_range_idx] = int_RR_dist_obj[Z_L[l]-1,0][0,Candidates_j_interval[j_range_idx]-1]
                            log_Omega_N[l,i] = np.log(Tau_R[l,pointer_i]*np.sum(int_RR_dist_obj[Z_L[l]-1,1][0,pointer_position:])+np.sum(np.exp(log_Gamma_R[Z_L[l]-1,j_range])*p_j_range_temp))
            if Tau_R[l,i] == 0:
                P_FSB_Z[0,i] = 0
            else:
                temp = (1 + np.exp(log_Omega_N[l,i]-log_Gamma_R[Z_L[l]-1,i])/Tau_R[l,i])
                if temp==0:
                    P_FSB_Z[0,i] = 0
                else:
                    P_FSB_Z[0,i] = 1/temp

            peak_mat[l,i] = np.random.choice([0,1],1,p=[1-P_FSB_Z[0,i],P_FSB_Z[0,i]])[0]
            if peak_mat[l,i] == 1:
                pointer_position = Candidates_position[i]
                pointer_i = i



    return peak_mat

def eval_int_RR_prob(R_sample_position,int_RR_dist_obj,z_idx,start_position,end_position):
    pi_1z = 1/np.squeeze(np.dot(int_RR_dist_obj[z_idx,0],(int_RR_dist_obj[z_idx,1]).T))
    #    print(1)
    window_length = end_position-start_position
    if len(R_sample_position)==0:
        sequence_prob = np.squeeze(np.dot(int_RR_dist_obj[z_idx,0][0,window_length+1:],
                                          (int_RR_dist_obj[z_idx,1][0,window_length+1:]-window_length).T))/np.squeeze(np.dot(int_RR_dist_obj[z_idx,0],(int_RR_dist_obj[z_idx,1]).T))
    elif R_sample_position[0] == start_position and R_sample_position[-1] == end_position:
        int_RR_time = np.diff(R_sample_position)
        sequence_prob = pi_1z
        for int_RR_time_idx in range(len(int_RR_time)):
            sequence_prob = sequence_prob*np.squeeze(int_RR_dist_obj[z_idx,0][int_RR_dist_obj[z_idx,1]==int_RR_time[int_RR_time_idx]])
    elif R_sample_position[0] != start_position and R_sample_position[-1] == end_position:
        int_RR_time = np.diff(R_sample_position)
        sequence_prob = pi_1z*np.sum(int_RR_dist_obj[z_idx,0][int_RR_dist_obj[z_idx,1]>(R_sample_position[0]-start_position)])
        for int_RR_time_idx in range(len(int_RR_time)):
            sequence_prob = sequence_prob*np.squeeze(int_RR_dist_obj[z_idx,0][int_RR_dist_obj[z_idx,1]==int_RR_time[int_RR_time_idx]])
    elif R_sample_position[0] == start_position and R_sample_position[-1] != end_position:
        int_RR_time = np.diff(R_sample_position)
        sequence_prob = pi_1z*np.sum(int_RR_dist_obj[z_idx,0][int_RR_dist_obj[z_idx,1]>(end_position-R_sample_position[-1])])
        for int_RR_time_idx in range(len(int_RR_time)):
            sequence_prob = sequence_prob*np.squeeze(int_RR_dist_obj[z_idx,0][int_RR_dist_obj[z_idx,1]==int_RR_time[int_RR_time_idx]])
    elif R_sample_position[0] != start_position and R_sample_position[-1] != end_position:
        int_RR_time = np.diff(R_sample_position)
        sequence_prob = pi_1z*np.sum(int_RR_dist_obj[z_idx,0][int_RR_dist_obj[z_idx,1]>(end_position-R_sample_position[-1])])*np.sum(int_RR_dist_obj[z_idx,0][int_RR_dist_obj[z_idx,1]>(R_sample_position[0]-start_position)])
        for int_RR_time_idx in range(len(int_RR_time)):
            if True not in np.squeeze(int_RR_dist_obj[z_idx,1]==int_RR_time[int_RR_time_idx]):
                sequence_prob = 0
            else:
                sequence_prob = sequence_prob*np.squeeze(int_RR_dist_obj[z_idx,0][int_RR_dist_obj[z_idx,1]==int_RR_time[int_RR_time_idx]])
    if 'sequence_prob' not in locals():
        sequence_prob = 0
    return sequence_prob, pi_1z














def z_sample_generation(Peak_mat,Candidates_position,pi_Z,int_RR_dist_obj,start_position,end_position):
    z_index = np.where(pi_Z[0,:] != 0)[0]
    sequence_prob = np.zeros((1,len(z_index)))
    posterior_z = np.zeros((np.shape(Peak_mat)[0],np.shape(pi_Z)[1]))
    for l in range(np.shape(Peak_mat)[0]):
        for i in range(len(z_index)):
            sequence_prob[0,i],temp = eval_int_RR_prob(np.array(Candidates_position)[np.where(Peak_mat[l,:]==1)[0]],int_RR_dist_obj,z_index[i],start_position,end_position)
        if np.sum(sequence_prob) == 0:
            sequence_prob = np.ones((1,np.shape(sequence_prob)[1]))/(np.shape(sequence_prob)[1])
        Z_y_prob = np.zeros((1,len(pi_Z[0,:])))
        Z_y_prob[0,z_index] = sequence_prob[0,:]
        posterior_z[l,:] = Z_y_prob*pi_Z/np.sum(Z_y_prob*pi_Z)
    posterior_z_marg = np.mean(posterior_z,axis=0)
    Z_L = np.random.choice(np.linspace(0,np.shape(pi_Z)[1]-1,np.shape(pi_Z)[1]),np.shape(Peak_mat)[0],p=posterior_z_marg)
    return Z_L+1


def Bayesian_IP_memphis(Candidates_position,Candidates_LR,int_RR_dist_obj,start_position,end_position):
    log_Gamma_R = HMM_forward_smoothie(Candidates_LR,Candidates_position,int_RR_dist_obj,start_position)
    iter_num_1 = 1
    num_cluster = np.shape(int_RR_dist_obj)[0]
    L = 20
    Z_L = np.array([[i+1 for k in range(L)] for i in range(num_cluster)]).reshape((L*num_cluster,1))
    #    print(Z_L)
    Peak_mat = np.zeros((np.shape(Z_L)[0],np.shape(log_Gamma_R)[1]))
    pi_Z = np.ones((1,num_cluster))/num_cluster
    Z_L_modePerZ = np.array([0]*num_cluster)
    while iter_num_1 <=8:
        for t in range(num_cluster):
            if iter_num_1 == 1:
                Peak_mat[t*L:(t+1)*L,:] = HMM_stochastic_backward_smoothie(log_Gamma_R,Candidates_position,int_RR_dist_obj,
                                                                           Z_L[t*L:(t+1)*L,0],start_position,end_position)
            Z_L[t*L:(t+1)*L,0] = z_sample_generation(Peak_mat[t*L:(t+1)*L,:],Candidates_position,pi_Z,int_RR_dist_obj,start_position,end_position)
            Peak_mat[t*L:(t+1)*L,:] = HMM_stochastic_backward_smoothie(log_Gamma_R,Candidates_position,int_RR_dist_obj,
                                                                       Z_L[t*L:(t+1)*L,0],start_position,end_position)
            Z_L_modePerZ[t] = mode(Z_L[t*L:(t+1)*L,0])[0]
        iter_num_1 += 1
    N,temp = np.histogram(np.squeeze(Z_L), bins=np.array(range(0,num_cluster+1,1))+.5)
    Z_mode = np.where(N==max(N))[0]+1
    Z_output = Z_L[np.where(Z_L[:,0]==Z_mode)[0],:]
    Peak_mat_output = Peak_mat[Z_L[:,0]==Z_mode,:]
    return Peak_mat_output,Z_output
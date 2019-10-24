# Copyright (c) 2018, MD2K Center of Excellence
# - main author: Tushar(Ohio State)
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
import numpy as np
import pandas as pd
from numpy import int16,uint8

def Preprc(raw_data: object, flag: object = 0) -> object:
    """
    Function to compute the decoded values in motionsense HRV sensors and
    interploate the timestamps given the decoded sequence numbers

    :param raw_data:
    :param flag:
    :return:
    """
    #     print(raw_data.shape,'input')
    # process recieved arrays (data_arr1=data, data_arr2=time,seq)
    if not list(raw_data):
        return []
    #     print(raw_data.shape)
    data_arr1, data_arr2, err_pkts = process_raw_PPG(raw_data)
    seq = np.copy(data_arr2[:, 1])
    # make Sq no. ordered
    d = np.diff(seq)
    idx1 = np.where(d < -(1023 - 50))[0]
    idx1 = np.append(idx1, len(seq) - 1)
    for i in range(len(idx1) - 1):
        seq[idx1[i] + 1:idx1[i + 1] + 1] = seq[idx1[i] + 1:idx1[i + 1] + 1] - (i + 1) * d[idx1[i]]
    seq = (seq - seq[0]).astype(int).reshape((len(seq)))
    # print(seq)
    seq_max = max(seq)  # just some heuristic to make ECG  seq value 4 times

    arr1 = np.concatenate([seq.reshape((len(seq), 1)), data_arr1], axis=1)

    if raw_data.all != None:
        df1 = pd.DataFrame(arr1, columns=['Seq', 'AccX', 'AccY', 'AccZ', 'GyroX',
                                          'GyroY', 'GyroZ', 'LED1', 'LED2', 'LED3'])
    else:
        return []

    df1.drop_duplicates(subset=['Seq'], inplace=True)

    df2 = pd.DataFrame(np.array(range(seq_max + 1)), columns=['Seq'])

    itime = data_arr2[0, 0];
    ftime = data_arr2[-1, 0]
    df3 = df2.merge(df1, how='left', on=['Seq'])
    df3['time'] = pd.to_datetime(np.linspace(itime, ftime, len(df2)), unit='ms')
    df3.set_index('time', inplace=True)
    df3.interpolate(method='time', axis=0, inplace=True)  # filling missing data
    df3.dropna(inplace=True)
    df3['time_stamps'] = np.linspace(itime, ftime, len(df2))
    #     print(df3.values.shape,'output')
    return df3

def process_raw_PPG(raw_data):
    data = raw_data
    Vals = data[:,2:]
    num_samples = Vals.shape[0]
    ts = data[:,0]
    Accx=np.zeros((num_samples));Accy=np.zeros((num_samples))
    Accz=np.zeros((num_samples));Gyrox=np.zeros((num_samples))
    Gyroy=np.zeros((num_samples));Gyroz=np.zeros((num_samples))
    led1=np.zeros((num_samples));led2=np.zeros((num_samples))
    led3=np.zeros((num_samples));seq=np.zeros((num_samples))
    time_stamps=np.zeros((num_samples))
    i=0;s=0
    while (i+s)<(num_samples):
        time_stamps[i]=ts[i+s]
        Accx[i] = int16((uint8(Vals[i+s,0])<<8) | (uint8(Vals[i+s,1])))
        Accy[i] = int16((uint8(Vals[i+s,2])<<8) | (uint8(Vals[i+s,3])))
        Accz[i] = int16((uint8(Vals[i+s,4])<<8) | (uint8(Vals[i+s,5])))
        Gyrox[i] = int16((uint8(Vals[i+s,6])<<8) | (uint8(Vals[i+s,7])))
        Gyroy[i] = int16((uint8(Vals[i+s,8])<<8) | (uint8(Vals[i+s,9])))
        Gyroz[i] = int16((uint8(Vals[i+s,10])<<8) | (uint8(Vals[i+s,11])))
        led1[i]=(uint8(Vals[i+s,12])<<10) | (uint8(Vals[i+s,13])<<2) | ((uint8(Vals[i+s,14]) & int('11000000',2))>>6)
        led2[i]=((uint8(Vals[i+s,14]) & int('00111111',2))<<12) | (uint8(Vals[i+s,15])<<4) | ((uint8(Vals[i+s,16]) & int('11110000',2))>>4)
        led3[i]=((uint8(Vals[i+s,16]) & int('00001111',2))<<14) | (uint8(Vals[i+s,17])<<6) | ((uint8(Vals[i+s,18]) & int('11111100',2))>>2)
        seq[i]=((uint8(Vals[i+s,18]) & int('00000011',2))<<8) | (uint8(Vals[i+s,19]))
        if i>0:
            difer=int((seq[i]-seq[i-1])%1024)
            if difer>20:
                s=s+1 # keep a record of how many such errors occured
                continue
        i=i+1
    # removing any trailing zeros
    seq=seq[:i];time_stamps=time_stamps[:i]
    Accx=Accx[:i]; Accy=Accy[:i]; Accz=Accz[:i]
    Gyrox=Gyrox[:i]; Gyroy=Gyroy[:i]; Gyroz=Gyroz[:i]
    led1=led1[:i]; led2=led2[:i]; led3=led3[:i]
    #     print('no. of unknown seq errors in PPG= ',s)
    data_arr1=np.stack((Accx,Accy,Accz,Gyrox,Gyroy,Gyroz,led1,led2,led3),axis=1)
    data_arr2=np.concatenate((time_stamps.reshape(1,-1),seq.reshape(1,-1))).T
    return data_arr1,data_arr2,0

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

def Preprc(raw_data,flag=0):
    """
    Function to compute the decoded values in motionsense HRV sensors and
    interploate the timestamps given the decoded sequence numbers

    :param raw_data:
    :param flag:
    :return:
    """
    # process recieved arrays (data_arr1=data, data_arr2=time,seq)
    if not raw_data:
        return []
    if raw_data.all is None:
        return []
    data_arr1,data_arr2,err_pkts=process_raw_PPG(raw_data)
    seq=np.copy(data_arr2[:,1])
    # make Sq no. ordered
    d = np.diff(seq)
    idx1 = np.where(d<-(1023-50))[0]
    idx1=np.append(idx1,len(seq)-1)
    for i in range(len(idx1)-1):
        seq[idx1[i]+1:idx1[i+1]+1]=seq[idx1[i]+1:idx1[i+1]+1]-(i+1)*d[idx1[i]]
    seq=(seq-seq[0]).astype(int).reshape((len(seq)))
    # print(seq)
    seq_max=max(seq) #just some heuristic to make ECG  seq value 4 times

    arr1=np.concatenate([seq.reshape((len(seq),1)),data_arr1],axis=1)


    if raw_data.all!=None:
        df1=pd.DataFrame(arr1,columns=['Seq','AccX','AccY','AccZ','GyroX',
                                       'GyroY','GyroZ','LED1','LED2','LED3'])
    else:
        return []

    df1.drop_duplicates(subset=['Seq'],inplace=True)

    df2=pd.DataFrame(np.array(range(seq_max+1)),columns=['Seq'])

    itime=data_arr2[0,0];ftime=data_arr2[-1,0]
    df3=df2.merge(df1,how='left',on=['Seq'])
    df3['time']=pd.to_datetime(np.linspace(itime,ftime,len(df2)),unit='ms')
    df3.set_index('time',inplace=True)
    df3.interpolate(method='time',axis=0,inplace=True) #filling missing data
    df3.dropna(inplace=True)
    df3['time_stamps']=np.linspace(itime,ftime,len(df2))
    return df3

def process_raw_PPG(raw_data):
    """
    function to decode the values from raw byte arrays

    :param raw_data:
    :return:
    """
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
    n=0;i=0;s=0;mis_pkts=0
    while (n)<(num_samples):
        time_stamps[i]=ts[n]
        Accx[i] = np.int16((np.uint8(Vals[n,0])<<8) | (np.uint8(Vals[n,1])))
        Accy[i] = np.int16((np.uint8(Vals[n,2])<<8) | (np.uint8(Vals[n,3])))
        Accz[i] = np.int16((np.uint8(Vals[n,4])<<8) | (np.uint8(Vals[n,5])))
        Gyrox[i] = np.int16((np.uint8(Vals[n,6])<<8) | (np.uint8(Vals[n,7])))
        Gyroy[i] = np.int16((np.uint8(Vals[n,8])<<8) | (np.uint8(Vals[n,9])))
        Gyroz[i] = np.int16((np.uint8(Vals[n,10])<<8) | (np.uint8(Vals[n,11])))
        led1[i]=(np.uint8(Vals[n,12])<<10) | (np.uint8(Vals[n,13])<<2) | \
                ((np.uint8(Vals[n,14]) & int('11000000',2))>>6)
        led2[i]=((np.uint8(Vals[n,14]) & int('00111111',2))<<12) |\
                (np.uint8(Vals[n,15])<<4) | \
                ((np.uint8(Vals[n,16]) & int('11110000',2))>>4)
        led3[i]=((np.uint8(Vals[n,16]) & int('00001111',2))<<14) | \
                (np.uint8(Vals[n,17])<<6) |\
                ((np.uint8(Vals[n,18]) & int('11111100',2))>>2)
        seq[i]=((np.uint8(Vals[n,18]) & int('00000011',2))<<8) | \
               (np.uint8(Vals[n,19]))
        if i>0:
            difer=int((seq[i]-seq[i-1])%1024)
            if difer>50:
                s=s+1 # keep a record of how many such errors occured
                n=n+1
                continue
            mis_pkts=mis_pkts+(difer-1)
        n=n+1;i=i+1
    # removing any trailing zeros
    seq=seq[:i];time_stamps=time_stamps[:i]
    Accx=Accx[:i]; Accy=Accy[:i]; Accz=Accz[:i]
    Gyrox=Gyrox[:i]; Gyroy=Gyroy[:i]; Gyroz=Gyroz[:i]
    led1=led1[:i]; led2=led2[:i]; led3=led3[:i]
    # print('no. of unknown seq errors in PPG= ',s)
    # print('no. of missed packets= {}'.format(mis_pkts))
    data_arr1=np.stack((Accx,Accy,Accz,Gyrox,Gyroy,Gyroz,led1,led2,led3),axis=1)
    # print(np.shape(data_arr1))
    data_arr2=np.concatenate((time_stamps.reshape(1,-1),seq.reshape(1,-1))).T
    return data_arr1,data_arr2,(mis_pkts+s)

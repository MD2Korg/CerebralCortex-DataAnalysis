# Copyright (c) 2019, MD2K Center of Excellence
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


from core.computefeature import ComputeFeatureBase
from core.feature.DataQualityPPG.utils import get_features,get_model
import numpy as np
from datetime import datetime
from cerebralcortex.core.datatypes.datapoint import DataPoint
from typing import List
from scipy import signal
from scipy.stats import skew,kurtosis,iqr
from sklearn.preprocessing import MinMaxScaler, RobustScaler
from copy import deepcopy
from collections import Counter

feature_class_name = 'DataQualityPPG'


class DataQualityPPG(ComputeFeatureBase):
    """
    This class takes as input raw datastreams from motionsenseHRV and decodes them to get the Accelerometer, Gyroscope
    PPG, Sequence number timeseries. Last of all it does timestamp correction on all the timeseries and saves them.
    """

    def return_numpy_array_from_datastream(self,data):
        if len(data)==0:
            return np.array([])
        final_data = np.zeros((len(data),len(data[0].sample)+2))
        for i,dp in enumerate(data):
            final_data[i,:2] = [dp.start_time.timestamp()*1000,dp.offset]
            final_data[i,2:] = dp.sample
        return final_data

    def preProcessing(self,data,Fs=25,fil_type='ppg'):
        '''
        Inputs
        data: a numpy array of shape n*10 .. the columns are timestamp,ppg red, ppg infrared,
        ppg green, acl x,y,z, gyro x,y,z
        Fs: sampling rate
        fil_type: ppg or ecg
        Output X2: preprocessed signal data
        preprocessing the data by filtering

        '''

        X0 = data[:,1:]
        X1 = signal.detrend(X0,axis=0,type='constant')
        b = signal.firls(65,np.array([0,0.2, 0.3, 3 ,3.5,Fs/2]),np.array([0, 0 ,1 ,1 ,0, 0]),
                         np.array([100*0.02,0.02,0.02]),fs=Fs)
        X2 = np.zeros((np.shape(X1)[0]-len(b)+1,data.shape[1]))
        for i in range(X2.shape[1]):
            if i in [0,4,5,6,7,8,9,10]:
                X2[:,i] = data[64:,i]
            else:
                X2[:,i] = signal.convolve(X1[:,i-1],b,mode='valid')
        return X2

    def get_datastream_raw(self,
                           identifier:str,
                           day:str,
                           user_id:str,
                           localtime:bool)->List[DataPoint]:
        stream_ids = self.CC.get_stream_id(user_id,identifier)
        data = []
        for stream_id in stream_ids:
            temp_data = self.CC.get_stream(stream_id=stream_id['identifier'],user_id=user_id,day=day,localtime=localtime)
            if len(temp_data.data)>0:
                data.extend(temp_data.data)
        return data
    def get_predict_prob(self,window):
        window[:,1:4] = signal.detrend(RobustScaler().fit_transform(window[:,1:4]),axis=0)
        window[:,1:4] = MinMaxScaler().fit_transform(window[:,1:4])
        f,pxx = signal.welch(window[:,1:4],fs=25,nperseg=len(window),nfft=1000,axis=0)
        pxx = np.abs(pxx)
        pxx = MinMaxScaler().fit_transform(pxx)
        skews = skew(window[:,1:4],axis=0).reshape(3,1)
        kurs = kurtosis(window[:,1:4],axis=0).reshape(3,1)
        iqrs = np.std(window[:,1:4],axis=0).reshape(3,1)
        rps = np.divide(np.trapz(pxx[np.where((f>=.8)&(f<=2.5))[0]],axis=0),np.trapz(pxx,axis=0)).reshape(3,1)
        features = np.concatenate([skews,kurs,rps,iqrs],axis=1)
        return features

    def save_data(self,decoded_data,offset,tzinfo,json_path,all_streams,user_id,localtime=False):
        final_data = []
        for i in range(len(decoded_data[:, 0])):
            final_data.append(DataPoint.from_tuple(
                start_time=datetime.utcfromtimestamp((decoded_data[i, 0])/1000).replace(tzinfo=tzinfo),
                offset=offset, sample=decoded_data[i, 1:]))
        print(final_data[0],all_streams)
        self.store_stream(json_path, [all_streams],
                          user_id, final_data, localtime=localtime)


    def get_and_save_data(self,
                          all_streams: dict,
                          all_days: list,
                          stream_identifiers: list,
                          user_id: str,
                          json_paths: str,
                          localtime=False):
        """
        all computation and storing of data

        :param all_streams: all streams of a person
        :param all_days: daylist to compute
        :param stream_identifier: left/right wrist HRV raw stream identifier
        :param user_id: user id
        :param json_path: name of json file containing metadata

        """

        for day in all_days:
            motionsense_raw = []
            for s in stream_identifiers:
                motionsense_raw.extend(self.get_datastream_raw(s,day,user_id,localtime=localtime))
            if len(motionsense_raw)<100:
                continue
            print(motionsense_raw[0],'first datapoint')
            print(len(motionsense_raw)," Size of extracted raw byte data")
            tzinfo = motionsense_raw[0].start_time.tzinfo
            print(len(motionsense_raw)," Size after admission control")
            if len(motionsense_raw)<100:
                continue
            motionsense_raw_data = self.return_numpy_array_from_datastream(motionsense_raw)
            if len(motionsense_raw_data)<100:
                continue
            offset = motionsense_raw_data[0,1]
            ppg_data = motionsense_raw_data[motionsense_raw_data[:,0].argsort()]
            ppg_data = ppg_data[:,np.array([0,2,3,4,5,6,7,8,9,10,11])]
            ppg_data = self.preProcessing(ppg_data)
            print(len(ppg_data),"length of preprocessed data")
            if len(ppg_data)<1000:
                continue
            window_col = []
            acl_features = []
            attachment_all = []
            ts_col = []
            for i in range(32,len(ppg_data[:,0])-32,25):
                ppgs = deepcopy(ppg_data[i-32:i+32,:])
                if ppgs[-1,0]-ppgs[0,0]>5000:
                    continue
                ts_col.append(ppg_data[i,0])
                likelihood_features = np.nan_to_num(self.get_predict_prob(ppgs))
                acl_features.append(likelihood_features.reshape(-1,3,4))
                features_acl = get_features(ppgs[:,np.array([0,4,5,6])])
                # features_gyro = get_features(ppgs[:,np.array([0,7,8,9])])
                features_all = np.array(features_acl)
                value, numbervalue = Counter(ppgs[:,10].reshape(-1)).most_common()[0]
                attachment_all.append(value)
                window_col.append(features_all)
            if len(attachment_all)<60:
                continue
            ts_col = np.array(ts_col).reshape(-1,1)
            acl_features = np.concatenate(acl_features)
            window_col = np.array(window_col)
            # print(window_col.shape,acl_features.shape)
            attachment_all = np.array(attachment_all).reshape(-1,1)
            # print(window_col.shape,attachment_all.shape)
            #     hr_col = np.concatenate(hr_col)
            likelihood = []
            clf = get_model()
            for k in range(acl_features.shape[1]):
                tmp = acl_features[:,k,:].reshape(-1,4)
                likelihood.append(clf.predict_proba(tmp)[:,1].reshape(-1,1))
            likelihood = np.concatenate(likelihood,axis=1)
            likelihood = np.max(likelihood,axis=1).reshape(-1,1)
            ppg_data_final = np.concatenate((ts_col,likelihood,attachment_all,window_col),axis=1)
            self.save_data(ppg_data_final,offset,tzinfo,json_paths,all_streams,user_id,localtime)
        return 0

    def process(self, user, all_days: list):
        """

        Takes the user identifier and the list of days and does the required processing

        :param user: user id string
        :param all_days: list of days to compute


        """
        if not all_days:
            return
        if self.CC is not None:
            if user:
                streams = self.CC.get_user_streams(user_id=user)
                if streams is None:
                    return None
                user_id = user
                all_streams_left = ['org.md2k.feature.motionsensehrv.decoded.leftwrist.v2']
                for s in all_streams_left:
                    if s in streams:
                        json_path = 'data_quality_motion_left.json'
                        temp = self.get_and_save_data(streams[s], all_days,
                                               all_streams_left,
                                               user_id, json_path)
                        break
                all_streams_right = ['org.md2k.feature.motionsensehrv.decoded.rightwrist.v2']
                for s in all_streams_right:
                    if s in streams:
                        json_path = 'data_quality_motion_right.json'
                        temp = self.get_and_save_data(streams[s], all_days,
                                               all_streams_right,
                                               user_id, json_path)
                        break
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
import numpy as np
from datetime import datetime
from cerebralcortex.core.datatypes.datapoint import DataPoint
from typing import List
from scipy.stats import skew,kurtosis
from core.feature.PPGHourYield.utils import get_model


feature_class_name = 'PPGHourYield'


class PPGHourYield(ComputeFeatureBase):
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

    def save_data(self,decoded_data,offset,tzinfo,json_path,all_streams,user_id,localtime=False):
        final_data = []
        for i in range(len(decoded_data[:, 0])):
            final_data.append(DataPoint.from_tuple(
                start_time=datetime.utcfromtimestamp((decoded_data[i, 0])/1000).replace(tzinfo=tzinfo),
                offset=offset, sample=decoded_data[i, 1:]))
        print(final_data[0],all_streams)
        self.store_stream(json_path, [all_streams],
                          user_id, final_data, localtime=localtime)
    def get_features(self,X):
        m = X[:,0]
        s = X[:,1]
        feature = [np.mean(m),np.std(m),skew(m),kurtosis(m)] + [np.mean(s),np.std(s),skew(s),kurtosis(s)]
        return np.array(feature)

    def get_yield(self,clf,data):
        count_all = 0
        count_best = 0
        count_good = 0
        count_bad = 0
        count_motion = 0
        count_medium = 0
        i = 0
        index_col = []
        while i<len(data[:,0]):
            j = i
            index = []
            while j<len(data[:,0]) and (data[j,0]-data[i,0])<=60000:
                index.append(j)
                j+=1
            index_col.append(np.array(index))
            j = i
            while j<len(data[:,0]) and (data[j,0]-data[i,0])<=20000:
                j+=1
            i=j
        acl_features = []
        for index in index_col:
            tmp = data[index]
            index_window = np.array([4+1,4+2])
            acl_window = tmp[:,index_window]
            feature_acl = self.get_features(acl_window)
            acl_features.append(feature_acl)
        motion_indicator = clf.predict(np.array(acl_features)).reshape(-1)
        for i,index in enumerate(index_col):
            tmp = data[index]
            window_likelihood = tmp[:,2]
            window_likelihood = window_likelihood[window_likelihood>=.1]
            window_attachment = tmp[:,3]
            count_all+=1
            if len(window_attachment[window_attachment==-1])/len(window_attachment)>=.5:
                continue
            if motion_indicator[i]==1:
                count_motion+=1
                continue
            if len(window_likelihood)<30:
                count_bad+=1
            elif np.mean(window_likelihood)>=.7 and len(window_likelihood)>=30:
                count_best+=1
                count_medium+=1
                count_good+=1
            elif np.mean(window_likelihood)>=.5 and len(window_likelihood)>=30:
                count_medium+=1
                count_good+=1
            elif np.mean(window_likelihood)>=.3 and len(window_likelihood)>=30:
                count_good+=1
            else:
                count_bad+=1
        tmp = [count_best/180,count_medium/180,count_good/180,count_motion/180,count_bad/180]
        #     print(tmp)
        return tmp


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
        :param stream_identifiers: left/right wrist HRV raw stream identifier
        :param user_id: user id
        :param json_paths: name of json file containing metadata

        """
        clf = get_model()
        for day in all_days:
            try:
                final_data = []
                data = []
                for s in stream_identifiers:
                    data.extend(self.get_datastream_raw(s,day,user_id,localtime=localtime))
                if len(data)<100:
                    continue
                tzinfo = data[0].start_time.tzinfo
                offset = data[0].offset
                i=0
                while i<len(data):
                    j = i
                    hour_now = data[j].start_time.hour
                    tmp = [data[j]]
                    while j<len(data) and data[j].start_time.hour== hour_now:
                        tmp.append(data[j])
                        j+=1
                    if len(tmp)>60:
                        tmp_data = self.return_numpy_array_from_datastream(tmp)
                        offset = tmp_data[0,1]
                        feat = self.get_yield(clf,tmp_data)
                        feat = [tmp_data[30,0]]+feat
                        final_data.append(np.array(feat))
                    i=j
                ppg_data_final = np.array(final_data)
                self.save_data(ppg_data_final,offset,tzinfo,json_paths,all_streams,user_id,localtime)
            except Exception as e:
                continue
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
                output_streams = ["org.md2k.feature.motionsensehrv.ppg.yield.leftwrist.v3"]
                data_out = []
                for s1 in output_streams:
                    data_out.extend(self.get_datastream_raw(s1,all_days[0],user_id,False))
                if len(data_out)==0:
                    all_streams_left = ['org.md2k.feature.motionsensehrv.ppg.quality.leftwrist',
                                        'org.md2k.feature.motionsensehrv.ppg.quality.leftwrist.v2']
                    for s in all_streams_left:
                        if s in streams:
                            json_path = 'data_yield_left.json'
                            temp = self.get_and_save_data(streams[s], all_days,
                                                          all_streams_left,
                                                          user_id, json_path)
                            break
                output_streams = ["org.md2k.feature.motionsensehrv.ppg.yield.rightwrist.v3"]
                data_out = []
                for s1 in output_streams:
                    data_out.extend(self.get_datastream_raw(s1,all_days[0],user_id,False))
                if len(data_out)==0:
                    all_streams_right = ['org.md2k.feature.motionsensehrv.ppg.quality.rightwrist',
                                         'org.md2k.feature.motionsensehrv.ppg.quality.rightwrist.v2']
                    for s in all_streams_right:
                        if s in streams:
                            json_path = 'data_yield_right.json'
                            temp = self.get_and_save_data(streams[s], all_days,
                                                          all_streams_right,
                                                          user_id, json_path)
                            break
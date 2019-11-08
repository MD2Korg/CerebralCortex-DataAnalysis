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
feature_class_name = 'PresenceDecodeHRV'


class PresenceDecodeHRV(ComputeFeatureBase):
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


    def get_and_save_data(self,
                          all_streams: dict,
                          all_days: list,
                          stream_identifiers: list,
                          user_id: str,
                          json_paths: str,
                          data,
                          localtime=False):
        """
        all computation and storing of data

        :param all_streams: all streams of a person
        :param all_days: daylist to compute
        :param stream_identifier: left/right wrist HRV raw stream identifier
        :param user_id: user id
        :param json_path: name of json file containing metadata
        
        """
        tzinfo = data[0].start_time.tzinfo
        offset = data[0].offset
        motionsense_raw_data = self.return_numpy_array_from_datastream(data[:1])
        marked_data = np.zeros((1,3))
        marked_data[0,0] = motionsense_raw_data[0,0]
        marked_data[0,1] = motionsense_raw_data[0,1]
        marked_data[0,2] = 1
        self.save_data(marked_data,offset,tzinfo,json_paths,all_streams,user_id,localtime)


    def process(self, user, all_days: list):
        """

        Takes the user identifier and the list of days and does the required processing
        
        :param user: user id string
        :param all_days: list of days to compute

        
        """
        if not all_days:
            return
        streams = self.CC.get_user_streams(user_id=user)
        if streams is None:
            return None
        if self.CC is not None:
            if user:
                left_identifier = "org.md2k.feature.motionsensehrv.decoded.leftwrist.v2"
                right_identifier = "org.md2k.feature.motionsensehrv.decoded.rightwrist.v2"
                tmp = self.get_datastream_raw(left_identifier,
                                              all_days[0],
                                              user,False)
                if len(tmp)>0:
                    user_id = user
                    json_path = 'decoded_hrv_presence.json'
                    self.get_and_save_data(streams[left_identifier], all_days,
                                           [left_identifier],
                                           user_id, json_path,tmp)
                else:
                    tmp = self.get_datastream_raw(right_identifier,
                                              all_days[0],
                                              user,False)
                    if len(tmp)>0:
                        user_id = user
                        json_path = 'decoded_hrv_presence.json'
                        self.get_and_save_data(streams[right_identifier], all_days,
                                               [right_identifier],
                                               user_id, json_path,tmp)

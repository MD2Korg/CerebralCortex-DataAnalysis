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


from core.computefeature import ComputeFeatureBase
from core.feature.motionsenseHRVdecode.util_helper_functions \
    import *
import numpy as np
import pytz
from datetime import datetime,timedelta

feature_class_name = 'DecodeHRV'

class DecodeHRV(ComputeFeatureBase):

    """
    This class takes as input raw datastreams from motionsenseHRV and decodes them to get the Accelerometer, Gyroscope 
    PPG, Sequence number timeseries. Last of all it does timestamp correction on all the timeseries and saves them. 
    """
    def get_data_around_stress_survey(self,
                                      all_streams:dict,
                                      day:str,
                                      user_id:str,
                                      raw_byte_array:list)->list:
        """
        This function checks for qualtrics stress survey data present on the day 
        specified and finds those DataPoints which are only 60 minutes behind the
        time of taking the survey. The motivation is to predict the stress value 
        we would be more concerned with the 60 minutes of data beforehand 
        
        :param all_streams: a dictionery of all the streams of the partiipant
        :param day: a string in 'YYYYMMDD' format 
        :param user_id: uuid string representing the user identifier
        :param raw_byte_array: A list of all the DataPoints for that user on that day
        
        :return: A list of only those DataPoints those are 60 minutes behind the timing of stress survey
        """
        if qualtrics_identifier in all_streams:
            data = self.CC.get_stream(all_streams[qualtrics_identifier][
                                          'identifier'], user_id=user_id, day=day,localtime=False)
            if len(data.data) > 0:
                data = data.data
                final_data = []
                s1 = data[0].end_time
                for dp in raw_byte_array:
                    s2 = dp.start_time
                    if s1 > s2 and s2 + timedelta(minutes=60) > s1 :
                        final_data.append(dp)
                return final_data
        return []

    def get_and_save_data(self,
                          all_streams:dict,
                          all_days:list,
                          stream_identifier:str,
                          user_id:str,
                          json_path:str):
        """
        all computation and storing of data

        :param all_streams: all streams of a person
        :param all_days: daylist to compute
        :param stream_identifier: left/right wrist HRV raw stream identifier
        :param user_id: user id
        :param json_path: name of json file containing metadata
        
        """
        for day in all_days:
            motionsense_raw = self.CC.get_stream(
                all_streams[stream_identifier]["identifier"],day=day,
                user_id=user_id,localtime=False)
            motionsense_raw_data = admission_control(motionsense_raw.data)
            if len(motionsense_raw_data) > 0:
                offset = motionsense_raw_data[0].offset
                data = self.get_data_around_stress_survey(all_streams,
                                                     day,
                                                     user_id,
                                                     motionsense_raw_data)
                if len(data) > 0:
                    decoded_sample = get_decoded_matrix(np.array(data))
                    if not list(decoded_sample):
                        continue
                    final_data = []
                    for i in range(len(decoded_sample[:, 0])):
                        final_data.append(DataPoint.from_tuple(
                            start_time=datetime.fromtimestamp(decoded_sample[i, -1]).replace(tzinfo=pytz.UTC),
                            offset=offset, sample=decoded_sample[i, 1:-1]))
                    self.store_stream(json_path,[all_streams[stream_identifier]],
                           user_id, final_data,localtime=False)
        return


    def process(self, user, all_days:list):
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
                    return
                user_id = user

                if motionsense_hrv_left_raw in streams:
                    json_path = 'decoded_hrv_left_wrist.json'
                    self.get_and_save_data(streams,all_days,
                                           motionsense_hrv_left_raw,
                                           user_id,json_path)


                if motionsense_hrv_right_raw in streams:
                    json_path = 'decoded_hrv_right_wrist.json'
                    self.get_and_save_data(streams,all_days,
                                           motionsense_hrv_right_raw,
                                           user_id,json_path)



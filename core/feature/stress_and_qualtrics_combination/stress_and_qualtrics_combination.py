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


from core.computefeature import ComputeFeatureBase
import math
from datetime import timedelta
from core.feature.stress_and_qualtrics_combination.util import *
from cerebralcortex.core.datatypes.datapoint import DataPoint
feature_class_name = 'stress_and_qualtrics_combination'



class stress_and_qualtrics_combination(ComputeFeatureBase):

    def get_data_around_stress_survey(self,
                                      all_streams:dict,
                                      day:str,
                                      user_id:str,
                                      raw_byte_array:list,
                                      mins:int)->list:
        """
        This function checks for qualtrics stress survey data present on the day 
        specified and finds those DataPoints which are only 60 minutes behind the
        time of taking the survey. The motivation is to predict the stress value 
        we would be more concerned with the 60 minutes of data beforehand 
        
        :rtype: list
        :param dict all_streams: a dictionery of all the streams of the partiipant
        :param str day: a string in 'YYYYMMDD' format
        :param str user_id: uuid string representing the user identifier
        :param list raw_byte_array: A list of all the DataPoints for that user on that day
        
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
                    if s2 <= s1 <= s2 + timedelta(minutes=mins):
                        final_data.append(dp)
                return final_data
        return []

    def process(self, user:str, all_days:list):
        """
        Takes the user identifier and the list of days and does the required processing

        :param user: user id string
        :param all_days: list of days to compute
        """
        if not all_days:
            return
        if self.CC is None:
            return
        if not user:
            return

        all_streams = self.CC.get_user_streams(user_id=user)

        if all_streams is None:
            return

        if qualtrics_identifier not in all_streams or stress_identifier not in all_streams:
            return

        user_id = user

        for day in all_days:
            if stress_identifier not in all_streams:
                continue

            data = get_datastream(self.CC,stress_identifier,day,user_id,False)

            if not list(data):
                continue

            print('-'*20,len(data),'-'*20,' after getting the points')




            for min in minutes:
                data_final = self.get_data_around_stress_survey(all_streams=all_streams,day=day,
                                                               user_id=user_id,raw_byte_array=data,mins=min)

                if len(data_final)>0:
                    json_path = 'stress_likelihood_'+str(min)+'min.json'
                    self.store_stream(json_path,
                                      [all_streams[stress_identifier]],
                                      user_id,
                                      data_final,localtime=False)




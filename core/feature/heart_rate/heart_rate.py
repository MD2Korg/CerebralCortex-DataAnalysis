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
from core.feature.heart_rate.util import *
import math
from datetime import timedelta
from cerebralcortex.core.datatypes.datapoint import DataPoint

feature_class_name = 'heart_rate'


class heart_rate(ComputeFeatureBase):
    """
    This class extracts the pre computed rr interval timeseries and computes a continuous heart rate timeseries 
    with a resolution of two seconds
    """

    def get_and_save_data(self,
                          streams: dict,
                          day: str,
                          stream_identifier: str,
                          user_id: str,
                          json_path: str):
        """
        This function takes all the streams of a user on a specific day alongwith the rr interval datastream name and 
        extracts the rr interval data for the day.
        
        It then unpacks the minute based list of heart rate present in the RR interval data to compute a heart rate 
        timeseries of two second resolution 
        
        :param streams: all the streams of the user
        :param day: day in yyyymmdd string format
        :param stream_identifier: stream name of rr interval
        :param user_id: uuid of user
        :param json_path: name of the file which contains the metadata
        
        """
        rr_interval_data = self.CC.get_stream(streams[stream_identifier]["identifier"],
                                              day=day, user_id=user_id, localtime=False)
        print("-" * 20, " rr interval data ", len(rr_interval_data.data), "-" * 20)
        if not rr_interval_data.data:
            return
        final_data = []
        for dp in rr_interval_data.data:
            if math.isnan(dp.sample[1]):
                continue
            if not list(dp.sample[0]):
                continue
            initial = dp.start_time + timedelta(seconds=4)
            step = timedelta(seconds=2)
            count = 0
            while initial <= dp.end_time and count < len(dp.sample[2][0]):
                if not math.isnan(dp.sample[2][0][count]):
                    final_data.append(
                        DataPoint.from_tuple(start_time=initial, offset=dp.offset, sample=dp.sample[2][0][count]))
                count += 1
                initial += step
        print("-" * 20, ' final data length ', len(final_data), "-" * 20)
        self.store_stream(json_path, [streams[stream_identifier]], user_id, final_data, localtime=False)

    def process(self, user: str, all_days: list):
        """
        Takes the user identifier and the list of days and does the required processing  
        
        :param user: user id string
        :param all_days: list of days to compute
        
        """
        if not list(all_days):
            return

        if self.CC is None:
            return

        if user is None:
            return

        streams = self.CC.get_user_streams(user)

        if streams is None:
            return

        if rr_interval_identifier not in streams:
            return
        user_id = user
        json_path = 'heart_rate.json'
        for day in all_days:
            self.get_and_save_data(streams, day, rr_interval_identifier, user_id, json_path)

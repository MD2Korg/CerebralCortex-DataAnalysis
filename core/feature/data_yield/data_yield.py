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
from core.feature.data_yield.utils.util import *
from core.signalprocessing.window import window_sliding

feature_class_name = 'data_yield'

class data_yield(ComputeFeatureBase):

    def calculate_yield(self,user_id,stream_identifier,all_days,all_streams,json_path):
        for day in all_days:
            final_data = []
            motionsense_raw = self.CC.get_stream(all_streams[stream_identifier]["identifier"],
                                                 day=day,user_id=user_id,localtime=False)
            if not list(motionsense_raw.data):
                continue
            data = admission_control(motionsense_raw.data)
            if not list(data):
                continue
            decoded_data = decode_only(data)
            offset = decoded_data[0].offset
            windowed_decoded_data = window_sliding(decoded_data,window_size=window_size_60sec,
                                                   window_offset=window_size_60sec)
            final_yield = []
            for key in windowed_decoded_data.keys():
                window_data_60sec = windowed_decoded_data[key]
                window_data_10sec = window_sliding(window_data_60sec,
                                                   window_size=window_size_10sec,
                                                   window_offset=window_size_10sec)
                quality = get_quality(window_data_10sec,Fs)
                final_yield.append(DataPoint.from_tuple(start_time=key[0],
                                                        end_time=key[1],
                                                        offset=offset,
                                                        sample = quality))
            if not final_yield:
                continue
            self.store_stream(json_path,
                              [all_streams[stream_identifier]],
                              user_id,
                              final_yield,localtime=False)

    def process(self, user, all_days):
        """

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
        if motionsense_hrv_left not in all_streams and  motionsense_hrv_right not in all_streams:
            return
        user_id = user
        json_path = 'data_yield.json'
        self.calculate_yield(user_id,motionsense_hrv_left,all_days,all_streams,json_path)
        self.calculate_yield(user_id,motionsense_hrv_right,all_days,all_streams,json_path)


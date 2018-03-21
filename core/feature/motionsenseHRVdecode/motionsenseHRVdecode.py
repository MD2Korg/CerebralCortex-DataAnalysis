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

from datetime import datetime

feature_class_name = 'DecodeHRV'


class DecodeHRV(ComputeFeatureBase):

    def get_and_save_data(self,
                          all_streams,
                          all_days,
                          stream_identifier,
                          user_id,json_path):
        """
        all computation and storing of data

        :param all_streams: all streams of a person
        :param all_days: daylist to compute
        :param stream_identifier: left/right wrist HRV raw stream identifier
        :param user_id: user id
        :param json_path: path of json file containing metadata
        :return:
        """
        for day in all_days:
            motionsense_raw = self.CC.get_stream(
                all_streams[stream_identifier]["identifier"],day=day,
                user_id=user_id,localtime=False)
            motionsense_raw_data = admission_control(motionsense_raw.data)
            if len(motionsense_raw_data) > 0:
                data = np.array(motionsense_raw_data)
                offset = data[0].offset
                decoded_sample = get_decoded_matrix(data)
                if not list(decoded_sample):
                    continue
                final_data = []
                for i in range(len(decoded_sample[:, 0])):
                    final_data.append(DataPoint.from_tuple(
                        start_time=datetime.fromtimestamp(decoded_sample[i, -1]),
                        offset=offset, sample=decoded_sample[i, 1:-1]))
                self.store_stream(json_path,[all_streams[stream_identifier]],
                           user_id, final_data)



    def process(self, user, all_days):
        """

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



# Copyright (c) 2018, MD2K Center of Excellence
# - author: Md Azim Ullah
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

from cerebralcortex.core.datatypes.datastream import DataPoint
from core.computefeature import ComputeFeatureBase
from core.signalprocessing.motionsenseHRVdecode.util_get_store import get_stream_days,store_data
from core.signalprocessing.motionsenseHRVdecode.util_helper_functions import get_decoded_matrix
import numpy as np
import uuid
from datetime import datetime

feature_class_name='DecodedHRV'

class DecodeHRV(ComputeFeatureBase):

    def all_users_data(self, study_name: str):
        """
        Process all participants' streams
        :param study_name:
        """
        # get all participants' name-ids
        all_users = self.CC.get_all_users(study_name)

        if all_users:
            for user in all_users:
                self.process_streams(user["identifier"])
        else:
            print(study_name, "- study has 0 users.")

    def process_streams(self, user_id: uuid):
        """
        Contains pipeline execution of all the diagnosis algorithms
        :param user_id:
        :param CC:
        :param config:
        """
        motionsense_hrv_left_raw = "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
        motionsense_hrv_right_raw = "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"

        # get all the streams belong to a participant
        streams = self.CC.get_user_streams(user_id)
        if motionsense_hrv_left_raw in streams:

            stream_days_left = get_stream_days(streams[motionsense_hrv_left_raw]["identifier"],
                                               self.CC)

            if len(stream_days_left)>0:
                for day in stream_days_left:
                    motionsense_left_raw = self.CC.get_stream(streams[motionsense_hrv_left_raw]["identifier"],
                                                         day=day,user_id=user_id)
                    if len(motionsense_left_raw.data)>0:
                        data = np.array(motionsense_left_raw.data)
                        offset = data[0].offset
                        decoded_sample = get_decoded_matrix(data)
                        final_data = []
                        for i in range(len(decoded_sample[:,0])):
                            final_data.append(DataPoint.from_tuple(start_time=datetime.fromtimestamp(
                                decoded_sample[i,-1]),offset=offset,sample=decoded_sample[i,1:-1]))
                        store_data("metadata/decoded_hrv_left_wrist.json",
                                           [streams[motionsense_hrv_left_raw]],
                                           user_id,final_data, self)

        if motionsense_hrv_right_raw in streams:

            stream_days_right = get_stream_days(streams[motionsense_hrv_right_raw]["identifier"],
                                                self.CC)

            if len(stream_days_right)>0:
                for day in stream_days_right:
                    motionsense_right_raw = self.CC.get_stream(streams[motionsense_hrv_right_raw]["identifier"],
                                                          day=day,user_id=user_id)
                    if len(motionsense_right_raw.data)>0:
                        data = np.array(motionsense_right_raw.data)
                        offset = data[0].offset
                        decoded_sample = get_decoded_matrix(data)
                        final_data = []
                        for i in range(len(decoded_sample[:,0])):
                            final_data.append(DataPoint.from_tuple(start_time=datetime.fromtimestamp(
                                decoded_sample[i,-1]),offset=offset,sample=decoded_sample[i,1:-1]))
                        store_data("metadata/decoded_hrv_right_wrist.json",
                                   [streams[motionsense_hrv_right_raw]],
                                   user_id,final_data, self)

    def process(self):
        if self.CC is not None:
            print("Decoding All the Raw MotionsenseHRV Raw Byte Data")
            self.all_users_data("mperf-alabsi")


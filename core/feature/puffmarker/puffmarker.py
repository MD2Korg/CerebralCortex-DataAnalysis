# Copyright (c) 2018, MD2K Center of Excellence
# - Nazir Saleheen <nazir.saleheen@gmail.com>
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


import uuid

from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.core.datatypes.datastream import DataStream
from core.computefeature import ComputeFeatureBase
from core.feature.puffmarker.CONSTANTS import *
from core.feature.puffmarker.admission_control import check_motionsense_hrv_accelerometer, \
    check_motionsense_hrv_gyroscope
from core.feature.puffmarker.puff_classifier import classify_puffs
from core.feature.puffmarker.smoking_episode import generate_smoking_episode
from core.feature.puffmarker.util import get_stream_days, store_data
from core.feature.puffmarker.wrist_features import compute_wrist_feature

feature_class_name = 'PuffMarker'


class PuffMarker(ComputeFeatureBase):

    def __init__(self):
        CC_CONFIG_PATH = '/home/md2k/cc_configuration.yml'
        self.CC = CerebralCortex(CC_CONFIG_PATH)

    def process_puffmarker(self, user_id: uuid,
                           accel_stream_left: DataStream,
                           gyro_stream_left: DataStream,
                           accel_stream_right: DataStream,
                           gyro_stream_right: DataStream):
        """
        1. generates puffmarker wrist feature vectors from accelerometer and gyroscope
        2. classifies each feature vector as either puff or non_puff
        3. constructs smoking episodes from this puffs
        :param accel_stream_left:
        :param gyro_stream_left:
        :param accel_stream_right:
        :param gyro_stream_right:
        """

        accel_stream_left.data = check_motionsense_hrv_accelerometer(accel_stream_left.data)
        accel_stream_right.data = check_motionsense_hrv_accelerometer(accel_stream_right.data)
        gyro_stream_left.data = check_motionsense_hrv_gyroscope(gyro_stream_left.data)
        gyro_stream_right.data = check_motionsense_hrv_gyroscope(gyro_stream_right.data)

        puff_labels_left = []
        puff_labels_right = []

        if (len(accel_stream_left.data) > 0) & (len(gyro_stream_left.data) > 0):
            all_features_left = compute_wrist_feature(accel_stream_left, gyro_stream_left, 'leftwrist',
                                                      fast_moving_avg_size,
                                                      slow_moving_avg_size)
            puff_labels_left = classify_puffs(all_features_left)

        if (len(accel_stream_right.data) > 0) & (len(gyro_stream_right.data) > 0):
            all_features_right = compute_wrist_feature(accel_stream_right, gyro_stream_right, 'rightwrist',
                                                       fast_moving_avg_size,
                                                       slow_moving_avg_size)
            puff_labels_right = classify_puffs(all_features_right)

        for indx in range(len(puff_labels_right)):
            if puff_labels_right[indx].sample == 1:
                puff_labels_right[indx].sample = 2

        puff_labels = puff_labels_right + puff_labels_left
        puff_labels.sort(key=lambda x: x.start_time)

        store_data("metadata/smoking_puff_puffmarker_wrist.json", [accel_stream_right, gyro_stream_right], user_id,
                   puff_labels, self)

        smoking_episodes = generate_smoking_episode(puff_labels)
        store_data("metadata/smoking_episode_puffmarker_wrist.json", [accel_stream_right, gyro_stream_right], user_id,
                   smoking_episodes, self)

    def process_data(self, user_id: uuid):
        """
        Contains pipeline execution of all the diagnosis algorithms
        :param user_id:
        """
        # get all the streams belong to a participant
        streams = self.CC.get_user_streams(user_id)

        stream_days = get_stream_days(streams[motionsense_hrv_accel_left_streamname]["identifier"], self.CC)
        for day in stream_days:
            accel_stream_left = self.CC.get_stream(
                streams[motionsense_hrv_accel_left_streamname]["identifier"], user_id, day,
                data_type=DataSet.COMPLETE)
            gyro_stream_left = self.CC.get_stream(
                streams[motionsense_hrv_gyro_left_streamname]["identifier"], user_id, day,
                data_type=DataSet.COMPLETE)

            accel_stream_right = self.CC.get_stream(
                streams[motionsense_hrv_accel_right_streamname]["identifier"], user_id, day,
                data_type=DataSet.COMPLETE)
            gyro_stream_right = self.CC.get_stream(
                streams[motionsense_hrv_gyro_right_streamname]["identifier"], user_id, day,
                data_type=DataSet.COMPLETE)

            # Calling puffmarker algorithm to get smoking episodes
            self.process_puffmarker(user_id, accel_stream_left, gyro_stream_left, accel_stream_right,
                                    gyro_stream_right)

    def process(self):
        if self.CC is not None:
            print("Processing PuffMarker")

            all_users = self.CC.get_all_users(study_name)

            if all_users:
                for user in all_users:
                    self.process_data(user["identifier"])
            else:
                print(study_name, "- study has 0 users.")

# pm = PuffMarker()
# pm.process()

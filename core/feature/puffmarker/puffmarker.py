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


import numpy as np

from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from core.computefeature import ComputeFeatureBase
from core.feature.puffmarker.PUFFMARKER_CONSTANTS import *
from core.feature.puffmarker.admission_control import check_motionsense_hrv_accelerometer, \
    check_motionsense_hrv_gyroscope
from core.feature.puffmarker.puff_classifier import classify_puffs
from core.feature.puffmarker.smoking_episode import generate_smoking_episode
from core.feature.puffmarker.util import get_stream_days, store_data
from core.feature.puffmarker.wrist_features import compute_wrist_features

feature_class_name = 'PuffMarker'


class PuffMarker(ComputeFeatureBase):
    '''
    Generates smoking episodes from wrist worn inertial sensors (Accelerometer and gyroscope)

    '''

    def __init__(self):
        CC_CONFIG_PATH = '/home/md2k/cc_configuration.yml'
        self.CC = CerebralCortex(CC_CONFIG_PATH)

    def get_input_streams(self, streams, user_id, day):

        accel_stream_left = None
        accel_stream_right = None
        gyro_stream_left = None
        gyro_stream_right = None

        if MOTIONSENSE_HRV_ACCEL_LEFT_STREAMNAME in streams:
            accel_stream_left = self.CC.get_stream(
                streams[MOTIONSENSE_HRV_ACCEL_LEFT_STREAMNAME]["identifier"], user_id, day,
                data_type=DataSet.COMPLETE)
        if MOTIONSENSE_HRV_GYRO_LEFT_STREAMNAME in streams:
            gyro_stream_left = self.CC.get_stream(
                streams[MOTIONSENSE_HRV_GYRO_LEFT_STREAMNAME]["identifier"], user_id, day,
                data_type=DataSet.COMPLETE)
        if MOTIONSENSE_HRV_ACCEL_RIGHT_STREAMNAME in streams:
            accel_stream_right = self.CC.get_stream(
                streams[MOTIONSENSE_HRV_ACCEL_RIGHT_STREAMNAME]["identifier"], user_id, day,
                data_type=DataSet.COMPLETE)
        if MOTIONSENSE_HRV_GYRO_RIGHT_STREAMNAME in streams:
            gyro_stream_right = self.CC.get_stream(
                streams[MOTIONSENSE_HRV_GYRO_RIGHT_STREAMNAME]["identifier"], user_id, day,
                data_type=DataSet.COMPLETE)

        return accel_stream_left, gyro_stream_left, accel_stream_right, gyro_stream_right

    def process(self):
        if self.CC is not None:

            all_users = self.CC.get_all_users(study_name)

            for user in all_users:
                user_id = user['identifier']

                streams = self.CC.get_user_streams(user_id)

                stream_days = []
                if MOTIONSENSE_HRV_ACCEL_LEFT_STREAMNAME in streams:
                    stream_days = get_stream_days(streams[MOTIONSENSE_HRV_ACCEL_LEFT_STREAMNAME]["identifier"],
                                                  self.CC)
                if MOTIONSENSE_HRV_ACCEL_RIGHT_STREAMNAME in streams:
                    temp_stream_days = get_stream_days(
                        streams[MOTIONSENSE_HRV_ACCEL_RIGHT_STREAMNAME]["identifier"], self.CC)
                    stream_days = stream_days + temp_stream_days
                    stream_days = np.unique(stream_days)

                for day in stream_days:

                    accel_stream_left, gyro_stream_left, accel_stream_right, gyro_stream_right = \
                        self.get_input_streams(streams, user_id, day)

                    accel_stream_left.data = check_motionsense_hrv_accelerometer(accel_stream_left.data)
                    accel_stream_right.data = check_motionsense_hrv_accelerometer(accel_stream_right.data)
                    gyro_stream_left.data = check_motionsense_hrv_gyroscope(gyro_stream_left.data)
                    gyro_stream_right.data = check_motionsense_hrv_gyroscope(gyro_stream_right.data)

                    puff_labels_left = []
                    puff_labels_right = []

                    if (len(accel_stream_left.data) > 0) & (len(gyro_stream_left.data) > 0):
                        all_features_left = compute_wrist_features(accel_stream_left,
                                                                   gyro_stream_left,
                                                                   FAST_MOVING_AVG_SIZE,
                                                                   SLOW_MOVING_AVG_SIZE)
                        puff_labels_left = classify_puffs(all_features_left)

                    if (len(accel_stream_right.data) > 0) & (len(gyro_stream_right.data) > 0):
                        all_features_right = compute_wrist_features(accel_stream_right,
                                                                    gyro_stream_right,
                                                                    FAST_MOVING_AVG_SIZE,
                                                                    SLOW_MOVING_AVG_SIZE)
                        puff_labels_right = classify_puffs(all_features_right)

                    for index in range(len(puff_labels_right)):
                        if puff_labels_right[index].sample == PUFF_LABEL_LEFT:
                            puff_labels_right[index].sample = PUFF_LABEL_RIGHT

                    puff_labels = puff_labels_right + puff_labels_left

                    if len(puff_labels) > 0:
                        puff_labels.sort(key=lambda x: x.start_time)

                        store_data("metadata/smoking_puff_puffmarker_wrist.json",
                                   [accel_stream_right, gyro_stream_right], user_id,
                                   puff_labels, self)

                        smoking_episodes = generate_smoking_episode(puff_labels)
                        store_data("metadata/smoking_episode_puffmarker_wrist.json",
                                   [accel_stream_right, gyro_stream_right], user_id,
                                   smoking_episodes, self)

# pm = PuffMarker()
# pm.process()

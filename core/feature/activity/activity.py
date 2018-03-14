# Copyright (c) 2018, MD2K Center of Excellence
# - Sayma Akther <sakther@memphis.edu>
# FIXME - No need for author name, can be obtained from the git commit logs
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
from core.feature.activity.wrist_accelerometer_features import compute_accelerometer_features
from core.feature.activity.activity_classifier import classify_posture, classify_activity
from core.signalprocessing.gravity_filter.gravityFilter import gravityFilter_function
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from core.feature.activity.utils import *
from core.computefeature import ComputeFeatureBase
from core.feature.activity.activity_constants import *

feature_class_name = 'ActivityMarker'


class ActivityMarker(ComputeFeatureBase):
    """
    Detects activity and posture per 10 seconds window from
        motionSense HRV accelerometer and gyroscope

    At first it computes activity and posture from both wrist then
        takes maximum as final label

    """

    def process_activity_and_posture_marker(self, streams, user_id, day, wrist: str):

        """ Process activity and posture detection fro single wrist
        :param streams: all the streams of user with user-id
        :param user_id:
        :param wrist: either left or right
        :return: activity labels and posture lebels for each 10 seconds window
        """

        if wrist in [LEFT_WRIST]:
            accel_stream = self.CC.get_stream(streams[MOTIONSENSE_HRV_ACCEL_LEFT]["identifier"],
                                              day=day,
                                              user_id=user_id,
                                              data_type=DataSet.COMPLETE)
            gyro_stream = self.CC.get_stream(streams[MOTIONSENSE_HRV_GYRO_LEFT]["identifier"],
                                             day=day,
                                             user_id=user_id,
                                             data_type=DataSet.COMPLETE)
        else:
            accel_stream = self.CC.get_stream(streams[MOTIONSENSE_HRV_ACCEL_RIGHT]["identifier"],
                                              day=day,
                                              user_id=user_id, data_type=DataSet.COMPLETE)  # FIXME
            gyro_stream = self.CC.get_stream(streams[MOTIONSENSE_HRV_GYRO_RIGHT]["identifier"],
                                             day=day,
                                             user_id=user_id, data_type=DataSet.COMPLETE)  # FIXME

        if len(accel_stream.data) == 0 or len(gyro_stream.data) == 0:
            return [], []

        gravity_filtered_accel_stream = gravityFilter_function(accel_stream,
                                                               gyro_stream,
                                                               sampling_freq=SAMPLING_FREQ_MOTIONSENSE_ACCEL,
                                                               is_gyro_in_degree=IS_MOTIONSENSE_HRV_GYRO_IN_DEGREE)

        activity_features = compute_accelerometer_features(gravity_filtered_accel_stream,
                                                           window_size=TEN_SECONDS)

        posture_labels = classify_posture(activity_features)
        activity_labels = classify_activity(activity_features)

        return posture_labels, activity_labels

    def process(self, user, all_days):
        if self.CC is not None:
            if user:
                streams = self.CC.get_user_streams(user)
                if not len(streams):
                    self.CC.logging.log('No streams found for user_id %s'
                                        %(user))
                    return

                for day in all_days:
                    posture_labels_left, activity_labels_left = self.process_activity_and_posture_marker(streams,
                                                                                                         user_id, day,
                                                                                                         LEFT_WRIST)
                    posture_labels_right, activity_labels_right = self.process_activity_and_posture_marker(streams,
                                                                                                           user_id, day,
                                                                                                           RIGHT_WRIST)
                    activity_labels = merge_left_right(activity_labels_left,
                                                       activity_labels_right,
                                                       window_size=TEN_SECONDS)
                    posture_labels = merge_left_right(posture_labels_left,
                                                      posture_labels_right,
                                                      window_size=TEN_SECONDS)
                    
                    print("activity_type_stream:",len(activity_labels))
                    print("posture_stream:",len(posture_labels))

                    store_data("metadata/activity_type_10seconds_window.json",
                               [streams[MOTIONSENSE_HRV_ACCEL_RIGHT],
                                streams[MOTIONSENSE_HRV_GYRO_RIGHT]],
                               user_id,
                               activity_labels, "ACTIVITY TYPES", self)
                    store_data("metadata/posture_10seconds_window.json",
                               [streams[MOTIONSENSE_HRV_ACCEL_RIGHT],
                                streams[MOTIONSENSE_HRV_GYRO_RIGHT]],
                               user_id,
                               posture_labels, "POSTURE", self)

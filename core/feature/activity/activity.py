# Copyright (c) 2018, MD2K Center of Excellence
# - Sayma Akther <sakther@memphis.edu>
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

from core.feature.activity.wrist_accelerometer_features import \
    compute_accelerometer_features
from core.feature.activity.activity_classifier import classify_posture, \
    classify_activity
from core.signalprocessing.gravity_filter.gravityFilter import \
    gravityFilter_function
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from core.feature.activity.utils import *
from core.feature.activity.admission_control import *
from core.computefeature import ComputeFeatureBase

feature_class_name = 'ActivityMarker'


class ActivityMarker(ComputeFeatureBase):
    """
    Detects activity and posture per 10 seconds window from
        motionSense HRV accelerometer and gyroscope

    At first it computes activity and posture from both wrist then
        takes maximum as final label

    """

    def get_day_data(self, stream_name, user_id, day):
        day_data = []
        stream_ids = self.CC.get_stream_id(user_id, stream_name)
        for stream_id in stream_ids:
            data_stream = self.CC.get_stream(stream_id["identifier"],
                                             day=day,
                                             user_id=user_id,
                                             data_type=DataSet.COMPLETE)
            day_data.extend(data_stream.data)

        day_data.sort(key=lambda x: x.start_time)

        return day_data, stream_ids

    def process_activity_and_posture_marker(self, streams, user_id, day,
                                            wrist: str,
                                            is_gravity):
        """ Process activity and posture detection fro single wrist
        :param streams: all the streams of user with user-id
        :param user_id:
        :param wrist: either left or right
        :return: activity labels and posture lebels for each 10 seconds window
        """

        if wrist in [LEFT_WRIST]:
            accel_data = self.get_day_data(MOTIONSENSE_HRV_ACCEL_LEFT, user_id,
                                           day)
            gyro_data = self.get_day_data(MOTIONSENSE_HRV_GYRO_LEFT, user_id,
                                          day)

        elif wrist in [RIGHT_WRIST]:
            accel_data = self.get_day_data(MOTIONSENSE_HRV_ACCEL_RIGHT, user_id,
                                           day)
            gyro_data = self.get_day_data(MOTIONSENSE_HRV_GYRO_RIGHT, user_id,
                                          day)
        else:
            return [], []

        if len(accel_data) == 0 or len(gyro_data) == 0:
            return [], []

        if is_gravity:
            if len(accel_data) != len(gyro_data):
                return [], []
            valid_accel_data, valid_gyro_data = check_motionsense_hrv_accel_gyroscope(
                accel_data, gyro_data)
            gravity_filtered_accel_stream = \
                gravityFilter_function(valid_accel_data,
                                       valid_gyro_data,
                                       sampling_freq=SAMPLING_FREQ_MOTIONSENSE_ACCEL,
                                       is_gyro_in_degree=IS_MOTIONSENSE_HRV_GYRO_IN_DEGREE)
            accel_data = gravity_filtered_accel_stream.data
        else:
            accel_data = check_motionsense_hrv_accelerometer(accel_data)

        activity_features = compute_accelerometer_features(accel_data,
                                                           window_size=TEN_SECONDS)

        posture_labels = classify_posture(activity_features, is_gravity)
        activity_labels = classify_activity(activity_features, is_gravity)

        return posture_labels, activity_labels

    def process(self, user, all_days):
        if self.CC is None:
            return

        if not user:
            return

        streams = self.CC.get_user_streams(user)

        if not streams:
            self.CC.logging.log("Activity - no streams found for user: %s" %
                                (user))
            return

        for day in all_days:
            is_gravity = True
            self.CC.logging.log("Processing Activity for user: %s for day %s"
                                %(user, str(day)))

            posture_labels_left, activity_labels_left = \
                self.process_activity_and_posture_marker(streams,
                                                         user, day,
                                                         LEFT_WRIST,
                                                         is_gravity)
            posture_labels_right, activity_labels_right = \
                self.process_activity_and_posture_marker(streams,
                                                         user, day,
                                                         RIGHT_WRIST,
                                                         is_gravity)


            activity_labels = merge_left_right(activity_labels_left,
                                               activity_labels_right,
                                               window_size=TEN_SECONDS)
            posture_labels = merge_left_right(posture_labels_left,
                                              posture_labels_right,
                                              window_size=TEN_SECONDS)

            self.CC.logging.log("is_gravity TRUE activity_type_stream: %d" %
                                (len(activity_labels)))
            self.CC.logging.log("is_gravity TRUE posture_stream: %d " % 
                                (len(posture_labels)))

            if len(activity_labels) > 0:
                self.store_stream(filepath=ACTIVITY_TYPE_10SECONDS_WINDOW,
                                  input_streams=[
                                      streams[MOTIONSENSE_HRV_ACCEL_RIGHT],
                                      streams[MOTIONSENSE_HRV_GYRO_RIGHT]],
                                  user_id=user,
                                  data=activity_labels)

                self.store_stream(filepath=POSTURE_10SECONDS_WINDOW,
                                  input_streams=[
                                      streams[MOTIONSENSE_HRV_ACCEL_RIGHT],
                                      streams[MOTIONSENSE_HRV_GYRO_RIGHT]],
                                  user_id=user,
                                  data=posture_labels)

            # Calculating with accelerometer only
            is_gravity = False
            posture_labels_right, activity_labels_right = \
                self.process_activity_and_posture_marker(streams,
                                                         user, day,
                                                         RIGHT_WRIST,
                                                         is_gravity)
            activity_labels = activity_labels_right
            posture_labels = posture_labels_right

            self.CC.logging.log("is_gravity FALSE activity_type_stream: %d" %
                                (len(activity_labels)))
            self.CC.logging.log("is_gravity FALSE posture_stream: %d " % 
                                 (len(posture_labels)))

            if len(activity_labels) > 0:
                self.store_stream(
                    filepath=ACTIVITY_TYPE_ACCEL_ONLY_10SECONDS_WINDOW,
                    input_streams=[
                        streams[MOTIONSENSE_HRV_ACCEL_RIGHT],
                        streams[MOTIONSENSE_HRV_GYRO_RIGHT]],
                    user_id=user,
                    data=activity_labels)

                self.store_stream(filepath=POSTURE_ACCEL_ONLY_10SECONDS_WINDOW,
                                  input_streams=[
                                      streams[MOTIONSENSE_HRV_ACCEL_RIGHT],
                                      streams[MOTIONSENSE_HRV_GYRO_RIGHT]],
                                  user_id=user,
                                  data=posture_labels)

        self.CC.logging.log("Finished processing Activity for user: %s" % (user))

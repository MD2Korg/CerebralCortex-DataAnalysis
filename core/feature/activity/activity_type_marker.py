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

from cerebralcortex.core.datatypes.datastream import DataStream
from core.feature.activity.wrist_accelerometer_features import compute_accelerometer_features
from core.feature.activity.do_classification import classify_posture, classify_activity
from core.signalprocessing.gravity_filter.gravityFilter import gravityFilter_function
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from core.feature.activity.util import *

from computefeature import ComputeFeatureBase

feature_class_name='activity_marker'


class activity_marker(ComputeFeatureBase):

    def do_activity_marker(self, accel_stream: DataStream, gyro_stream: DataStream):

        acc_sync_filtered = gravityFilter_function(accel_stream, gyro_stream, 25.0)

        accel_features = compute_accelerometer_features(acc_sync_filtered, window_size=10)

        posture_label = classify_posture(accel_features)
        activity_label = classify_activity(accel_features)

        return posture_label, activity_label

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
        motionsense_hrv_accel_right = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
        motionsense_hrv_accel_left = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
        motionsense_hrv_gyro_right = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
        motionsense_hrv_gyro_left = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"

        # get all the streams belong to a participant
        streams = self.CC.get_user_streams(user_id)

        posture_labels_left_all =[]
        posture_labels_right_all =[]
        activity_label_left_all = []
        activity_label_right_all = []

        # stream_end_days = CC.get_stream_duration(streams[motionsense_hrv_gyro_right]["identifier"])

        stream_days = get_stream_days(streams[motionsense_hrv_gyro_left]["identifier"], CC)
        for day in stream_days:

            accel_stream_left = self.CC.get_stream(streams[motionsense_hrv_accel_left]["identifier"], day, data_type=DataSet.COMPLETE)
            gyro_stream_left = self.CC.get_stream(streams[motionsense_hrv_gyro_left]["identifier"], day, data_type=DataSet.COMPLETE)

            print( 'Left---' + user_id + ', ' + day + ', ' +  str(len(accel_stream_left.data))  + ', ' +  str(len(gyro_stream_left.data)))

            # # Calling puffmarker algorithm to get smoking episodes
            if len(accel_stream_left.data) == len(gyro_stream_left.data):
                posture_labels_left, activity_label_left = self.do_activity_marker(accel_stream_left, gyro_stream_left)
                posture_labels_left_all.append(posture_labels_left)
                activity_label_left_all.append(activity_label_left)

        stream_days = get_stream_days(streams[motionsense_hrv_gyro_right]["identifier"], self.CC)
        for day in stream_days:

            accel_stream_right = self.CC.get_stream(streams[motionsense_hrv_accel_right]["identifier"], day, data_type=DataSet.COMPLETE)
            gyro_stream_right = self.CC.get_stream(streams[motionsense_hrv_gyro_right]["identifier"], day, data_type=DataSet.COMPLETE)

            print( 'Right---' + user_id + ', ' + day + ', ' + str(len(accel_stream_right.data)) + ', ' + str(len(gyro_stream_right.data)))

            if len(accel_stream_right.data) == len(gyro_stream_right.data):
                posture_labels_right, activity_label_right = self.do_activity_marker(accel_stream_right, gyro_stream_right)
                posture_labels_right_all.append(posture_labels_right)
                activity_label_right_all.append(activity_label_right)
        store_data("metadata/activity_type_10seconds_window.json", [accel_stream_right, gyro_stream_right], user_id, activity_label_right_all, self)
        store_data("metadata/posture_10seconds_window.json", [accel_stream_right, gyro_stream_right], user_id, posture_labels_right_all, self)

    def process(self):
        if self.CC is not None:
            print("Processing PhoneFeatures")
            self.all_users_data("mperf")

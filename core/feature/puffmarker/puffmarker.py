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


from core.computefeature import ComputeFeatureBase
from core.feature.puffmarker.admission_control import \
    filter_motionsense_hrv_accelerometer, \
    filter_motionsense_hrv_gyroscope
from core.feature.puffmarker.puff_classifier import classify_puffs
from core.feature.puffmarker.smoking_episode import generate_smoking_episode
from core.feature.puffmarker.utils import *
from core.feature.puffmarker.wrist_features import compute_wrist_features
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet

feature_class_name = 'PuffMarker'


class PuffMarker(ComputeFeatureBase):
    '''
    Generates smoking episodes from wrist worn inertial sensors (Accelerometer and Gyroscope)

    1. find hand-to-mouth gesture from gyroscope
    2. filter hand-to-mouth gestures based on hand orientation, duration
    3. classify each hand-to-mouth as either puff or not puff
    4. finally, construct smoking episode from detected puffs if number of puffs is more than 4

    '''

    def get_day_data(self, stream_name: str, user_id, day):
        '''

        :param stream_name: name fo the stream
        :param string user_id: UID of the user
        :param str day: retrieve the data for this day with format 'YYYYMMDD'
        :return: list of datapoints
        '''
        day_data = []
        stream_ids = self.CC.get_stream_id(user_id, stream_name)
        for stream_id in stream_ids:
            data_stream = self.CC.get_stream(stream_id["identifier"],
                                             day=day,
                                             user_id=user_id,
                                             data_type=DataSet.COMPLETE,
                                             localtime=True)
            if data_stream is not None and len(data_stream.data) > 0:
                day_data.extend(data_stream.data)

        day_data.sort(key=lambda x: x.start_time)

        return day_data

    def process(self, user, all_days):
        '''

        :param str user: UUID of the user
        :param List(str) all_days: List of days with format 'YYYYMMDD'
        :return:
        '''
        if self.CC is None:
            return

        if not user:
            return

        streams = self.CC.get_user_streams(user)

        if not streams:
            self.CC.logging.log("PuffMarker - no streams found for user: %s" % (user))
            return

        for day in all_days:

            accel_data_left = self.get_day_data(
                MOTIONSENSE_HRV_ACCEL_LEFT_STREAMNAME, user, day)
            gyro_data_left = self.get_day_data(
                MOTIONSENSE_HRV_GYRO_LEFT_STREAMNAME, user, day)

            accel_data_right = self.get_day_data(
                MOTIONSENSE_HRV_ACCEL_RIGHT_STREAMNAME, user, day)
            gyro_data_right = self.get_day_data(
                MOTIONSENSE_HRV_GYRO_RIGHT_STREAMNAME, user, day)

            accel_data_left = filter_motionsense_hrv_accelerometer(accel_data_left)
            accel_data_right = filter_motionsense_hrv_accelerometer(accel_data_right)
            gyro_data_left = filter_motionsense_hrv_gyroscope(gyro_data_left)
            gyro_data_right = filter_motionsense_hrv_gyroscope(gyro_data_right)

            puff_labels_left = []
            puff_labels_right = []

            if (len(accel_data_left) > 0) and (len(gyro_data_left) > 0):
                if len(accel_data_left) != len(gyro_data_left):
                    gyro_data_left = merge_two_datastream(accel_data_left,
                                                          gyro_data_left)
                accel_data_left = [
                    DataPoint(start_time=dp.start_time, end_time=dp.end_time,
                              offset=dp.offset,
                              sample=list(np.dot(CONV_L, dp.sample)))
                    for dp in accel_data_left]
                gyro_data_left = [
                    DataPoint(start_time=dp.start_time, end_time=dp.end_time,
                              offset=dp.offset,
                              sample=list(np.dot(CONV_L, dp.sample)))
                    for dp in gyro_data_left]
                all_features_left = compute_wrist_features(accel_data_left,
                                                           gyro_data_left,
                                                           FAST_MOVING_AVG_SIZE,
                                                           SLOW_MOVING_AVG_SIZE)
                if len(all_features_left) > 0:
                    puff_labels_left = classify_puffs(all_features_left)

            if (len(accel_data_right) > 0) and (len(gyro_data_right) > 0):
                if len(accel_data_right) != len(gyro_data_right):
                    gyro_data_right = merge_two_datastream(accel_data_right,
                                                           gyro_data_right)

                accel_data_right = [
                    DataPoint(start_time=dp.start_time, end_time=dp.end_time,
                              offset=dp.offset,
                              sample=list(np.dot(CONV_R, dp.sample)))
                    for dp in accel_data_right]
                gyro_data_right = [
                    DataPoint(start_time=dp.start_time, end_time=dp.end_time,
                              offset=dp.offset,
                              sample=list(np.dot(CONV_R, dp.sample)))
                    for dp in gyro_data_right]
                all_features_right = compute_wrist_features(accel_data_right,
                                                            gyro_data_right,
                                                            FAST_MOVING_AVG_SIZE,
                                                            SLOW_MOVING_AVG_SIZE)
                if len(all_features_right) > 0:
                    puff_labels_right = classify_puffs(all_features_right)
                    for index in range(len(puff_labels_right)):
                        if puff_labels_right[index].sample != NON_PUFF_LABEL:
                            puff_labels_right[index].sample = PUFF_LABEL_RIGHT

            puff_labels = puff_labels_right + puff_labels_left

            if len(puff_labels) > 0:
                puff_labels.sort(key=lambda x: x.start_time)
                self.CC.logging.log(
                    "Total hand-to-mouth gestures: " + str(len(puff_labels)))
                self.store_stream(
                    filepath='smoking_puff_puffmarker_wrist.json',
                    input_streams=[
                        streams[MOTIONSENSE_HRV_ACCEL_RIGHT_STREAMNAME],
                        streams[MOTIONSENSE_HRV_GYRO_RIGHT_STREAMNAME]],
                    user_id=user, data=puff_labels, localtime=True)

                smoking_episodes = generate_smoking_episode(puff_labels)

                self.CC.logging.log(
                    "Total smoking episodes: " + str(len(smoking_episodes)))
                if len(smoking_episodes) > 0:
                    self.store_stream(
                        filepath='smoking_episode_puffmarker_wrist.json',
                        input_streams=[
                            streams[MOTIONSENSE_HRV_ACCEL_RIGHT_STREAMNAME],
                            streams[MOTIONSENSE_HRV_GYRO_RIGHT_STREAMNAME]],
                        user_id=user, data=smoking_episodes, localtime=True)

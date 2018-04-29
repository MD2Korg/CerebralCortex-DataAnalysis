# Copyright (c) 2018, MD2K Center of Excellence
# -Mithun Saha <msaha1@memphis.edu>,JEYA VIKRANTH JEYAKUMAR <vikranth94@ucla.edu>
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

from cerebralcortex.cerebralcortex import CerebralCortex
from datetime import timedelta, datetime
from cerebralcortex.core.util.data_types import DataPoint
from core.feature.typing.utils import *
from core.computefeature import ComputeFeatureBase

import pandas as pd

feature_class_name = 'TypingMarker'

common_days = []

motionsense_hrv_accel_right = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
motionsense_hrv_accel_left = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
motionsense_hrv_gyro_right = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
motionsense_hrv_gyro_left = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"


class TypingMarker(ComputeFeatureBase):
    """
    Detects Typing activity: Provides Start time and End-time of typing sessions.
    The typing sessions starts when the participant starts typing and ends when
    no key is pressed for more than 10 seconds. The inference is made from the
    accelerometer and gyroscope values of both MotionSense wrist bands.

    Algorithm::

        Step 1: For each user for each stream finds the stream ids
        Step 2: For each stream id makes a dictionary with stream id as key and list
                of dates as value
        Step 3: Creates a set of dates for each dictionary(left accel and right accel)
                Does not compute for left and right gyro, since if accel data is present,
                then gyro data will be there too
        Step 4: Creates a list of common dates from these two sets
        Step 5: For each of these common dates, brings DataPoints for each stream and
                makes them into dataframes
        Step 6: Combines accel and gyro dataframes to a single dataframe, for left and
                right wrists
        Step 7: Synchronizes left and right dataframes to a single dataframe
        Step 8: Uses the synchronized dataframe to produce typing episodes in a day
                for a user
        Step 9: Stores the typing episodes as DataPoints with start time, end time
                and value as 1 for typing episode or 0 as no typing episode
    """

    def collect_data(self, input_dict: dict, day: str, user_id: str) -> List[DataPoint]:
        """
        This function collects user data of all stream ids for a day

        :param dict input_dict: a dictionary of all stream ids with dates
        :param str day: date on which to operate
        :param str user_id: UUID
        :return: all DataPoints of the day if any stream id has data
                for that day
        :rtype: List(DataPoint)
        """
        all_data = []
        for stream_id in input_dict:
            if day in input_dict[stream_id]:
                data_stream = self.CC.get_stream(stream_id, user_id, day)
                if len(data_stream.data) == 0:
                    continue
                all_data.extend(data_stream.data)
        all_data.sort(key=lambda x: x.start_time)
        return all_data

    def get_common_days(self, user_id: str) -> tuple:
        """
         This function gets both wrists' accelerometer and gyroscope steam ids with dates to
         extract commons days for which both both wrists' accelerometer and gyroscope
         data exist. Also creates left,right accelerometer and gyroscope streams' dictionaries
         of stream ids, containing all the dates of each stream id.

        :param str user_id: UUID
        :return: list of common days and four dictionaries for
                left, right acc and gyro with stream id as key and
                list of dates as values
        :rtype: tuple(List,dict)
        """

        accel_right_stream_ids_with_date = {}
        gyro_right_stream_ids_with_date = {}
        accel_left_stream_ids_with_date = {}
        gyro_left_stream_ids_with_date = {}

        # gets all streams of one user
        streams = self.CC.get_user_streams(user_id)

        if streams:
            for s in [motionsense_hrv_accel_right, motionsense_hrv_gyro_right,
                      motionsense_hrv_accel_left,
                      motionsense_hrv_gyro_left]:

                stream_id_all = self.CC.get_stream_id(user_id, s)
                stream_ids = []

                # converts the dictionary to a list of stream ids
                for stream_id in stream_id_all:
                    stream_ids.append(stream_id['identifier'])

                # for each stream id gets all the days
                for stream_id in stream_ids:
                    stream_dicts = self.CC.get_stream_duration(stream_id)
                    stream_days = []
                    days = stream_dicts["end_time"] - stream_dicts["start_time"]

                    for day in range(days.days + 1):
                        stream_days.append((stream_dicts[
                                                "start_time"] + timedelta(
                            days=day)).strftime('%Y%m%d'))

                    # creates a dictionary of stream ids for each stream,
                    # where each stream id contains all the dates
                    if s == motionsense_hrv_accel_right:
                        accel_right_stream_ids_with_date[
                            stream_id] = stream_days
                    elif s == motionsense_hrv_gyro_right:
                        gyro_right_stream_ids_with_date[stream_id] = stream_days
                    elif s == motionsense_hrv_accel_left:
                        accel_left_stream_ids_with_date[stream_id] = stream_days
                    elif s == motionsense_hrv_gyro_left:
                        gyro_left_stream_ids_with_date[stream_id] = stream_days

        # creates unique days for the accl left and accl right data
        accel_right_unique_days = unique_days_of_one_stream(
            accel_right_stream_ids_with_date)
        accel_left_unique_days = unique_days_of_one_stream(
            accel_left_stream_ids_with_date)

        # creates common days for the accl left and accl right data
        common_days = list(
            accel_right_unique_days.intersection(accel_left_unique_days))
        common_days.sort()

        return common_days, accel_right_stream_ids_with_date, gyro_right_stream_ids_with_date, \
            accel_left_stream_ids_with_date, gyro_left_stream_ids_with_date

    def process(self, user: str, all_days: list):
        """
         This function processes both wrists' accl and gyro data for the
         commons days to create data frames for left, right accl and gyro data.
         Makes two dataframes for left(accl,gyro) and right(accl,gyro).
         Then uses the sync function to sync left and right dataframes.
         Finally uses the typing_episodes function to detect typing episodes.

        :param str user: UUID
        :param list all_days: list of days on which to operate
        """

        if self.CC is None:
            return

        if not user:
            return

        streams = self.CC.get_user_streams(user)

        if not streams:
            self.CC.logging.log(
                "Typing Activity - no streams found for user: %s" %
                (user))
            return

        common_days, accel_right_stream_ids_with_date, \
        gyro_right_stream_ids_with_date, \
        accel_left_stream_ids_with_date, gyro_left_stream_ids_with_date = \
            self.get_common_days(user)

        # print(common_days)
        for day in all_days:
            # print("Processing for date--------------------",day)

            if day not in common_days:
                continue

            # gets all the data for all stream ids of accl_right for a day
            get_all_data = self.collect_data(accel_right_stream_ids_with_date,
                                             day, user)

            if len(get_all_data) == 0:
                continue

            # creats a dataframe with accl_right data of a day
            acc_dataR = get_dataframe(get_all_data,
                                      ['time', 'arx', 'ary', 'arz'])

            # gets all the data for all stream ids of gyro_right for a day
            get_all_data = self.collect_data(gyro_right_stream_ids_with_date,
                                             day, user)

            if len(get_all_data) == 0:
                continue

            # creats a dataframe with gyro_right data of a day
            gyr_dataR = get_dataframe(get_all_data,
                                      ['time', 'grx', 'gry', 'grz'])

            # gets all the data for all stream ids of accl_left for a day
            get_all_data = self.collect_data(accel_left_stream_ids_with_date,
                                             day, user)

            if len(get_all_data) == 0:
                continue

            # creats a dataframe with accl_left data of a day
            acc_dataL = get_dataframe(get_all_data,
                                      ['time', 'alx', 'aly', 'alz'])

            # gets all the data for all stream ids of gyro_left for a day
            get_all_data = self.collect_data(gyro_left_stream_ids_with_date,
                                             day, user)

            if len(get_all_data) == 0:
                continue

            # creats a dataframe with gyro_left data of a day
            gyr_dataL = get_dataframe(get_all_data,
                                      ['time', 'glx', 'gly', 'glz'])

            # merges left accl,gyro to make a single dataframe. Same thing done
            # for right accl and gyro.
            dr = pd.concat((acc_dataR[acc_dataR.columns[0:4]],
                            gyr_dataR[gyr_dataR.columns[1:4]]), axis=1)
            dl = pd.concat((acc_dataL[acc_dataL.columns[0:4]],
                            gyr_dataL[gyr_dataL.columns[1:4]]), axis=1)

            # syncs left and right dataframes
            dataset = sync_left_right_accel(dl, dr)

            # gets the time offset data for the participant
            offset = 0

            if len(get_all_data) > 0:
                offset = get_all_data[0].offset

            # gets the typying episdoes information
            # data = typing_episodes(dataset, offset, self.CC)
            data = typing_episodes(dataset, offset)
            if len(data) == 0:
                # data.append(DataPoint(start_time=get_all_data[0].start_time,
                #                       end_time=get_all_data[-1].start_time, offset=offset,
                #                       sample=0))
                # data.append(DataPoint(get_all_data[0].start_time,get_all_data[-1].start_time,offset,[0,0.0]))
                data.append(DataPoint(start_time=get_all_data[0].start_time,
                                      end_time=get_all_data[-1].start_time, offset=offset,
                                      sample=[0,0.0]))

            # print(data)

            self.store_stream(filepath='typing_episode_1000_milisec_window.json',
                              input_streams=[
                                  streams[motionsense_hrv_accel_right],
                                  streams[motionsense_hrv_gyro_right],
                                  streams[motionsense_hrv_accel_left],
                                  streams[motionsense_hrv_gyro_left]],
                              user_id=user,
                              data=data)

        self.CC.logging.log(
            "Finished processing Typing Activity for user: %s" % (user))

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

from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from core.feature.activity_features.utils import *
from core.computefeature import ComputeFeatureBase
from datetime import timedelta, datetime
from typing import List
from cerebralcortex.core.datatypes.datapoint import DataPoint

feature_class_name = 'ActivityFeature'


class ActivityFeature(ComputeFeatureBase):
    """
    Computes activity features and posture features per hour from
        activity and posture outputs

    """

    def get_day_data(self, stream_name: object, user_id: object,
                     day: object) -> object:
        """
        get list od DataPoint for the stream name

        :param string stream_name: Name of the stream
        :param string user_id: UID of the user
        :param string day: YMD
        :return:
        :rtype: list(DataPoint)
        """
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

    def smoooth_output(self, data):
        smooth_data = [0] * len(data)

        for i in range(len(data)):

            freq = [0] * len(ACTIVITY_LABELS)
            for k in [-6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6]:
                if i + k >= 0 and i + k < len(data):
                    dp = data[i + k]
                    freq[ACTIVTY_LABEL_TO_NUMBER_MAPPING[dp.sample]] = freq[
                                                                           ACTIVTY_LABEL_TO_NUMBER_MAPPING[
                                                                               dp.sample]] + 1
            maxVal = 0
            maxLabel = 0
            for k in range(5):
                if maxVal < freq[k]:
                    maxVal = freq[k]
                    maxLabel = k
            smooth_data[i] = ACTIVITY_LABELS[maxLabel]
        return smooth_data

    def compute_activity_features_hourly(self, STREAMNAME: object,
                                         activity_data: List[DataPoint],
                                         streams: object,
                                         user: object) -> object:
        """
        compute activity output hourly

        :rtype: object
        :param string STREAMNAME:  Name of the stream
        :param list(Datapoint) activity_data: list of Datapoint
        :param streams:
        :param user:
        :return:
        """

        if activity_data is None or len(activity_data) == 0:
            return

        smooth_data = self.smoooth_output(activity_data)

        walking_min_hourly = [0] * 24
        mod_min_hourly = [0] * 24
        high_min_hourly = [0] * 24
        total_min_per_hour = [0] * 24

        for i, v in enumerate(activity_data):
            hr = int(v.start_time.hour)
            label = smooth_data[i]
            if label == WALKING:
                walking_min_hourly[hr] = walking_min_hourly[hr] + 1
            if label == MODERATE_ACT:
                mod_min_hourly[hr] = mod_min_hourly[hr] + 1
            if label == HIGH_ACT:
                high_min_hourly[hr] = high_min_hourly[hr] + 1
            total_min_per_hour[hr] = total_min_per_hour[hr] + 1

        walking_min_hourly = [v / 6 for v in walking_min_hourly]
        mod_min_hourly = [v / 6 for v in mod_min_hourly]
        high_min_hourly = [v / 6 for v in high_min_hourly]
        total_min_per_hour = [v / 6 for v in total_min_per_hour]

        y = activity_data[0].start_time.year
        m = activity_data[0].start_time.month
        d = activity_data[0].start_time.day
        offset = activity_data[0].offset

        walk_data = []
        mod_data = []
        high_data = []
        for hour in range(0, 24):
            start_time = get_local_datetime(year=y, month=m, day=d, hour=hour,
                                            minute=0, second=0, offset=offset)
            end_time = start_time + timedelta(minutes=59)
            walk_data.append(
                DataPoint(start_time=start_time, end_time=end_time,
                          offset=offset,
                          sample=[walking_min_hourly[hour],
                                  total_min_per_hour[hour]]))
            mod_data.append(
                DataPoint(start_time=start_time, end_time=end_time,
                          offset=offset,
                          sample=[mod_min_hourly[hour],
                                  total_min_per_hour[hour]]))
            high_data.append(
                DataPoint(start_time=start_time, end_time=end_time,
                          offset=offset,
                          sample=[high_min_hourly[hour],
                                  total_min_per_hour[hour]]))

        if STREAMNAME == ACTIVITY_STREAMNAME:
            self.store_stream(filepath=WALKING_HOURLY,
                              input_streams=[streams[ACTIVITY_STREAMNAME]],
                              user_id=user,
                              data=walk_data, localtime=True)
            self.store_stream(filepath=MODERATE_ACTIVITY_HOURLY,
                              input_streams=[streams[ACTIVITY_STREAMNAME]],
                              user_id=user,
                              data=mod_data, localtime=True)
            self.store_stream(filepath=HIGH_ACTIVITY_HOURLY,
                              input_streams=[streams[ACTIVITY_STREAMNAME]],
                              user_id=user,
                              data=high_data, localtime=True)

        if STREAMNAME == ACCEL_ONLY_ACTIVITY_STREAMNAME:
            self.store_stream(filepath=WALKING_HOURLY_ACCEL_ONLY,
                              input_streams=[
                                  streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                              user_id=user,
                              data=walk_data, localtime=True)
            self.store_stream(filepath=MODERATE_ACTIVITY_HOURLY_ACCEL_ONLY,
                              input_streams=[
                                  streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                              user_id=user,
                              data=mod_data, localtime=True)
            self.store_stream(filepath=HIGH_ACTIVITY_HOURLY_ACCEL_ONLY,
                              input_streams=[
                                  streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                              user_id=user,
                              data=high_data, localtime=True)

        return walking_min_hourly, mod_min_hourly, high_min_hourly, total_min_per_hour

    def compute_posture_features_hourly(self, posture_data: object,
                                        streams: object,
                                        user: object) -> object:
        """

        :param posture_data:
        :param streams:
        :param user:
        :return:
        """

        if posture_data is None or len(posture_data) == 0:
            return

        lying_min_hourly = [0] * 24
        sitting_min_hourly = [0] * 24
        standing_min_hourly = [0] * 24
        total_posture_min_hourly = [0] * 24
        for v in posture_data:
            hr = int(v.start_time.hour)
            label = v.sample
            if label == LYING:
                lying_min_hourly[hr] = lying_min_hourly[hr] + 1
            if label == SITTING:
                sitting_min_hourly[hr] = sitting_min_hourly[hr] + 1
            if label == STANDING:
                standing_min_hourly[hr] = standing_min_hourly[hr] + 1
            total_posture_min_hourly[hr] = total_posture_min_hourly[hr] + 1

        y = posture_data[0].start_time.year
        m = posture_data[0].start_time.month
        d = posture_data[0].start_time.day
        offset = posture_data[0].offset

        lying_data = []
        sitting_data = []
        standing_data = []
        for hour in range(0, 24):
            start_time = get_local_datetime(year=y, month=m, day=d, hour=hour,
                                            minute=0, second=0, offset=offset)
            end_time = start_time + timedelta(minutes=59)

            lying_data.append(
                DataPoint(start_time=start_time, end_time=end_time,
                          offset=offset,
                          sample=[lying_min_hourly[hour],
                                  total_posture_min_hourly[hour]]), localtime=True)
            sitting_data.append(
                DataPoint(start_time=start_time, end_time=end_time,
                          offset=offset,
                          sample=[sitting_min_hourly[hour],
                                  total_posture_min_hourly[hour]]), localtime=True)
            standing_data.append(
                DataPoint(start_time=start_time, end_time=end_time,
                          offset=offset,
                          sample=[standing_min_hourly[hour],
                                  total_posture_min_hourly[hour]]), localtime=True)

        self.store_stream(filepath=LYING_HOURLY,
                          input_streams=[streams[POSTURE_STREAMNAME]],
                          user_id=user,
                          data=lying_data, localtime=True)
        self.store_stream(filepath=SITTING_HOURLY,
                          input_streams=[streams[POSTURE_STREAMNAME]],
                          user_id=user,
                          data=sitting_data, localtime=True)
        self.store_stream(filepath=STANDING_HOURLY,
                          input_streams=[streams[POSTURE_STREAMNAME]],
                          user_id=user,
                          data=standing_data, localtime=True)

    def compute_hourly_mean_for_time_of_day(self, D: dict,
                                            days: object) -> object:

        """
        computes

        :rtype: object
        :param D: dictionary of activity output hourly
        :param list days:  all days
        :return: mean for hourly activity output
        """

        mean_walk_hourly = [0] * 24
        mean_mod_hourly = [0] * 24
        mean_high_hourly = [0] * 24
        day_count = [0] * 24

        for day in days:
            walkH, modH, highH, hourCount = D[day]
            for i in range(24):
                if hourCount[i] >= 30:
                    mean_walk_hourly[i] = mean_walk_hourly[i] + (
                            walkH[i] * 60) / hourCount[i]
                    mean_mod_hourly[i] = mean_mod_hourly[i] + (modH[i] * 60) / \
                                         hourCount[i]
                    mean_high_hourly[i] = mean_high_hourly[i] + (
                            highH[i] * 60) / hourCount[i]
                    day_count[i] = day_count[i] + 1

        for i in range(24):
            if day_count[i] > 0:
                mean_walk_hourly[i] = mean_walk_hourly[i] / day_count[i]
                mean_mod_hourly[i] = mean_mod_hourly[i] / day_count[i]
                mean_high_hourly[i] = mean_high_hourly[i] / day_count[i]

        return mean_walk_hourly, mean_mod_hourly, mean_high_hourly

    def compute_hourly_mean_for_day_of_week(self, D: dict,
                                            days: object) -> object:

        """

        :rtype: object
        :param D:
        :param days:
        :return:
        """

        dayOfWeek_mean = dict()

        for week in range(7):
            mean_walk_hourly = [0] * 24
            mean_mod_hourly = [0] * 24
            mean_high_hourly = [0] * 24
            nCount = [0] * 24

            same_week_days = [day for day in days if datetime.strptime(day,
                                                                       '%Y%m%d').weekday() == week]

            for day in same_week_days:
                walkH, modH, highH, hourCount = D[day]
                for i in range(24):
                    if hourCount[i] >= 30:
                        mean_walk_hourly[i] = mean_walk_hourly[i] + (
                                walkH[i] * 60) / hourCount[i]
                        mean_mod_hourly[i] = mean_mod_hourly[i] + (
                                modH[i] * 60) / hourCount[i]
                        mean_high_hourly[i] = mean_high_hourly[i] + (
                                highH[i] * 60) / hourCount[i]
                        nCount[i] = nCount[i] + 1

            for i in range(24):
                if nCount[i] > 0:
                    mean_walk_hourly[i] = mean_walk_hourly[i] / nCount[i]
                    mean_mod_hourly[i] = mean_mod_hourly[i] / nCount[i]
                    mean_high_hourly[i] = mean_high_hourly[i] / nCount[i]

            dayOfWeek_mean[week] = [mean_walk_hourly, mean_mod_hourly,
                                    mean_high_hourly]
        return dayOfWeek_mean

    def imputation_by_mean_data(self, D: dict, days: object, user_id: object,
                                STREAMNAME: object, streams: object,
                                offset: object) -> object:
        """

        :rtype: object
        :param D:
        :param days:
        :param user_id:
        :param STREAMNAME:
        :param streams:
        :param offset:
        """
        mean_walk_hourly_tofd, mean_mod_hourly_tofd, mean_high_hourly_tofd = \
            self.compute_hourly_mean_for_time_of_day(D, days)

        dayOfWeek_hourly_mean = self.compute_hourly_mean_for_day_of_week(D,
                                                                         days)

        for day in days:

            daytime = datetime.strptime(day, '%Y%m%d')
            week_id = daytime.weekday()

            mean_walk_hourly_dofw, mean_mod_hourly_dofw, mean_high_hourly_dofw = \
                dayOfWeek_hourly_mean[week_id]

            walkH_prev, modH_prev, highH_prev, hourCount = D[day]

            total_walking = sum(walkH_prev)
            total_mod = sum(modH_prev)
            total_high = sum(highH_prev)

            walkH_tofd = [0] * 24
            modH_tofd = [0] * 24
            highH_tofd = [0] * 24
            walkH_dofw = [0] * 24
            modH_dofw = [0] * 24
            highH_dofw = [0] * 24

            for i in range(24):
                if hourCount[i] < 30:
                    walkH_tofd[i] = mean_walk_hourly_tofd[i]
                    modH_tofd[i] = mean_mod_hourly_tofd[i]
                    highH_tofd[i] = mean_high_hourly_tofd[i]
                    walkH_dofw[i] = mean_walk_hourly_dofw[i]
                    modH_dofw[i] = mean_mod_hourly_dofw[i]
                    highH_dofw[i] = mean_high_hourly_dofw[i]
                else:
                    walkH_tofd[i] = (walkH_prev[i] * 60) / hourCount[i]
                    modH_tofd[i] = (modH_prev[i] * 60) / hourCount[i]
                    highH_tofd[i] = (highH_prev[i] * 60) / hourCount[i]

                    walkH_dofw[i] = (walkH_prev[i] * 60) / hourCount[i]
                    modH_dofw[i] = (modH_prev[i] * 60) / hourCount[i]
                    highH_dofw[i] = (highH_prev[i] * 60) / hourCount[i]

            total_imputed_time_of_day_walk = min(120, sum(walkH_tofd))
            total_imputed_time_of_day_mod = sum(modH_tofd)
            total_imputed_time_of_day_high = min(30, sum(highH_tofd))

            total_imputed_day_of_week_walk = min(120, sum(walkH_dofw))
            total_imputed_day_of_week_mod = sum(modH_dofw)
            total_imputed_day_of_week_high = min(30, sum(highH_dofw))

            y = int(day[:4])
            m = int(day[4:6])
            d = int(day[6:8])

            start_time = get_local_datetime(y, m, d, 0, 0, 0, offset=offset)
            end_time = get_local_datetime(y, m, d, 23, 59, 59, offset=offset)

            if STREAMNAME == ACTIVITY_STREAMNAME:
                self.store_stream(filepath=WALKING_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time,
                                                  offset=offset,
                                                  sample=total_walking)], localtime=True)
                self.store_stream(filepath=WALKING_IMPUTED_TIME_OF_DAY_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time,
                                                  offset=offset,
                                                  sample=total_imputed_time_of_day_walk)], localtime=True)
                self.store_stream(filepath=WALKING_IMPUTED_DAY_OF_WEEK_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time,
                                                  offset=offset,
                                                  sample=total_imputed_day_of_week_walk)], localtime=True)

                self.store_stream(filepath=MODERATE_ACTIVITY_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time,
                                                  offset=offset,
                                                  sample=total_mod)], localtime=True)
                self.store_stream(filepath=MODERATE_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_imputed_time_of_day_mod)], localtime=True)
                self.store_stream(filepath=MODERATE_ACTIVITY_IMPUTED_DAY_OF_WEEK_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                end_time=end_time, offset=offset,
                                                sample=total_imputed_day_of_week_mod)], localtime=True)

                self.store_stream(filepath=HIGH_ACTIVITY_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time,
                                                  offset=offset,
                                                  sample=total_high)], localtime=True)
                self.store_stream(filepath=HIGH_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_imputed_time_of_day_high)], localtime=True)
                self.store_stream(filepath=HIGH_ACTIVITY_IMPUTED_DAY_OF_WEEK_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_imputed_day_of_week_high)], localtime=True)

            if STREAMNAME == ACCEL_ONLY_ACTIVITY_STREAMNAME:
                self.store_stream(filepath=WALKING_DAILY,
                                  input_streams=[
                                      streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time,
                                                  offset=offset,
                                                  sample=total_walking)], localtime=True)
                self.store_stream(filepath=WALKING_IMPUTED_TIME_OF_DAY_DAILY,
                                  input_streams=[
                                      streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time,
                                                  offset=offset,
                                                  sample=total_imputed_time_of_day_walk)], localtime=True)
                self.store_stream(filepath=WALKING_IMPUTED_DAY_OF_WEEK_DAILY,
                                  input_streams=[
                                      streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time,
                                                  offset=offset,
                                                  sample=total_imputed_day_of_week_walk)], localtime=True)

                self.store_stream(filepath=MODERATE_ACTIVITY_DAILY,
                                  input_streams=[
                                      streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time,
                                                  offset=offset,
                                                  sample=total_mod)], localtime=True)
                self.store_stream(
                    filepath=MODERATE_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY,
                    input_streams=[streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                    user_id=user_id,
                    data=[DataPoint(start_time=start_time,
                                    end_time=end_time, offset=offset,
                                    sample=total_imputed_time_of_day_mod)], localtime=True)
                self.store_stream(
                    filepath=HIGH_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY,
                    input_streams=[
                        streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                    user_id=user_id,
                    data=[DataPoint(start_time=start_time,
                                    end_time=end_time, offset=offset,
                                    sample=total_imputed_day_of_week_mod)], localtime=True)

                self.store_stream(filepath=HIGH_ACTIVITY_DAILY,
                                  input_streams=[
                                      streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time,
                                                  offset=offset,
                                                  sample=total_high)], localtime=True)
                self.store_stream(
                    filepath=HIGH_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY,
                    input_streams=[
                        streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                    user_id=user_id,
                    data=[DataPoint(start_time=start_time,
                                    end_time=end_time, offset=offset,
                                    sample=total_imputed_time_of_day_high)], localtime=True)
                self.store_stream(
                    filepath=HIGH_ACTIVITY_IMPUTED_DAY_OF_WEEK_DAILY,
                    input_streams=[
                        streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                    user_id=user_id,
                    data=[DataPoint(start_time=start_time,
                                    end_time=end_time, offset=offset,
                                    sample=total_imputed_day_of_week_high)], localtime=True)

    def process(self, user: str, all_days: list):
        """

        :param user:
        :param all_days:
        """
        if self.CC is None:
            return

        if not user:
            return

        streams = self.CC.get_user_streams(user)

        if not streams:
            self.CC.logging.log(
                "Activity and posture features - no streams found for user: %s" %
                (user))
            return

        D = dict()
        D_accel = dict()

        day_list = []
        day_list_accel = []

        offset = 0
        for day in all_days:
            activity_data = self.get_day_data(ACTIVITY_STREAMNAME, user, day)
            if len(activity_data) > 0:
                walking_min_hourly, mod_min_hourly, high_min_hourly, total_min_per_hour = \
                    self.compute_activity_features_hourly(ACTIVITY_STREAMNAME,
                                                          activity_data,
                                                          streams,
                                                          user)
                D[day] = [walking_min_hourly, mod_min_hourly, high_min_hourly,
                          total_min_per_hour]
                day_list.append(day)

            activity_data_accel_only = \
                self.get_day_data(ACCEL_ONLY_ACTIVITY_STREAMNAME, user, day)
            if len(activity_data_accel_only) > 0:
                walking_min_hourly_accel_only, mod_min_hourly_accel_only, high_min_hourly_accel_only, total_min_per_hour_accel_only = \
                    self.compute_activity_features_hourly(
                        ACCEL_ONLY_ACTIVITY_STREAMNAME,
                        activity_data_accel_only,
                        streams, user)
                D_accel[day] = [walking_min_hourly_accel_only,
                                mod_min_hourly_accel_only,
                                high_min_hourly_accel_only,
                                total_min_per_hour_accel_only]
                day_list_accel.append(day)

            posture_data = self.get_day_data(POSTURE_STREAMNAME, user, day)
            self.compute_posture_features_hourly(posture_data, streams, user)
            if len(activity_data) > 0:
                offset = activity_data[0].offset

        self.imputation_by_mean_data(D, day_list, user,
                                     ACTIVITY_STREAMNAME, streams, offset)

        self.imputation_by_mean_data(D_accel, day_list_accel, user,
                                     ACCEL_ONLY_ACTIVITY_STREAMNAME, streams,
                                     offset)

        self.CC.logging.log(
            "Finished processing activity and posture features for user: %s" % (
                user))

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
from typing import List, Tuple, Dict
from cerebralcortex.core.datatypes.datapoint import DataPoint

feature_class_name = 'ActivityFeature'


class ActivityFeature(ComputeFeatureBase):
    """Computes activity features and posture features per hour from activity and posture outputs

        Notes:
            1.

        References:
            1.
        """

    def get_day_data(self, stream_name: str, user_id: str, day: str) -> List[DataPoint]:
        """Get a list of data points for the specified day

        Args:
            stream_name: input stream name
            user_id: input user id
            day: day string in YYYYMMDD format

        Returns:
            List[DataPoint]: A list of DataPoints
        """

        day_data = []
        stream_ids = self.CC.get_stream_id(user_id, stream_name)
        for stream_id in stream_ids:
            data_stream = self.CC.get_stream(stream_id["identifier"], day=day,
                                             user_id=user_id, data_type=DataSet.COMPLETE)

            if data_stream is not None and len(data_stream.data) > 0:
                day_data.extend(data_stream.data)

        day_data.sort(key=lambda x: x.start_time)

        return day_data

    def compute_activity_features_hourly(self, stream_name: str, activity_data: List[DataPoint],
                                         streams: dict, user: str) \
            -> Tuple[List[float], List[float], List[float], List[float]]:
        """Compute activity output hourly

        Args:
            stream_name: Name of the stream
            activity_data: Activity DataPoints
            streams:
            user:

        Returns:
           Tuple[List[float], List[float], List[float], List[float]]: walking_minutes_per_hour, moderate_minutes_per_hour, high_minutes_per_hour, total_minutes_per_hour
        """

        if activity_data is None or len(activity_data) == 0:
            return

        walking_minutes_per_hour = [0] * 24
        moderate_minutes_per_hour = [0] * 24
        high_minutes_per_hour = [0] * 24
        total_minutes_per_hour = [0] * 24

        for value in activity_data:
            hr = int(value.start_time.hour)
            if value.sample == WALKING:
                walking_minutes_per_hour[hr] = walking_minutes_per_hour[hr] + 1
            elif value.sample == MODERATE_ACT:
                moderate_minutes_per_hour[hr] = moderate_minutes_per_hour[hr] + 1
            elif value.sample == HIGH_ACT:
                high_minutes_per_hour[hr] = high_minutes_per_hour[hr] + 1

            total_minutes_per_hour[hr] = total_minutes_per_hour[hr] + 1

        walking_minutes_per_hour = [v / 6 for v in walking_minutes_per_hour]
        moderate_minutes_per_hour = [v / 6 for v in moderate_minutes_per_hour]
        high_minutes_per_hour = [v / 6 for v in high_minutes_per_hour]
        total_minutes_per_hour = [v / 6 for v in total_minutes_per_hour]

        offset = activity_data[0].offset

        walk_data = []
        moderate_data = []
        high_data = []
        for hour in range(0, 24):
            start_time = get_local_datetime(year=activity_data[0].start_time.year,
                                            month=activity_data[0].start_time.month,
                                            day=activity_data[0].start_time.day,
                                            hour=hour,
                                            minute=0,
                                            second=0,
                                            offset=offset)

            end_time = start_time + timedelta(hours=1)

            walk_data.append(DataPoint(start_time=start_time, end_time=end_time, offset=offset,
                                       sample=[walking_minutes_per_hour[hour], total_minutes_per_hour[hour]]))
            moderate_data.append(DataPoint(start_time=start_time, end_time=end_time, offset=offset,
                                           sample=[moderate_minutes_per_hour[hour], total_minutes_per_hour[hour]]))
            high_data.append(DataPoint(start_time=start_time, end_time=end_time, offset=offset,
                                       sample=[high_minutes_per_hour[hour], total_minutes_per_hour[hour]]))

        if stream_name == ACTIVITY_STREAMNAME:
            self.store_stream(filepath=WALKING_HOURLY,
                              input_streams=[streams[ACTIVITY_STREAMNAME]],
                              user_id=user,
                              data=walk_data)
            self.store_stream(filepath=MODERATE_ACTIVITY_HOURLY,
                              input_streams=[streams[ACTIVITY_STREAMNAME]],
                              user_id=user,
                              data=moderate_data)
            self.store_stream(filepath=HIGH_ACTIVITY_HOURLY,
                              input_streams=[streams[ACTIVITY_STREAMNAME]],
                              user_id=user,
                              data=high_data)

        if stream_name == ACCEL_ONLY_ACTIVITY_STREAMNAME:
            self.store_stream(filepath=WALKING_HOURLY_ACCEL_ONLY,
                              input_streams=[streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                              user_id=user,
                              data=walk_data)
            self.store_stream(filepath=MODERATE_ACTIVITY_HOURLY_ACCEL_ONLY,
                              input_streams=[streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                              user_id=user,
                              data=moderate_data)
            self.store_stream(filepath=HIGH_ACTIVITY_HOURLY_ACCEL_ONLY,
                              input_streams=[streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                              user_id=user,
                              data=high_data)

        return walking_minutes_per_hour, moderate_minutes_per_hour, high_minutes_per_hour, total_minutes_per_hour

    def compute_posture_features_hourly(self, posture_data: List[DataPoint], streams: dict, user: str)
        """Compute posture output hourly

        Args:
            posture_data: Posture DataPoints
            streams:
            user:
        """

        if posture_data is None or len(posture_data) == 0:
            return

        lying_minutes_per_hour = [0] * 24
        sitting_minutes_per_hour = [0] * 24
        standing_minutes_per_hour = [0] * 24
        total_posture_minutes_per_hour = [0] * 24

        for value in posture_data:
            hr = int(value.start_time.hour)
            if value.sample == LYING:
                lying_minutes_per_hour[hr] = lying_minutes_per_hour[hr] + 1
            if value.sample == SITTING:
                sitting_minutes_per_hour[hr] = sitting_minutes_per_hour[hr] + 1
            if value.sample == STANDING:
                standing_minutes_per_hour[hr] = standing_minutes_per_hour[hr] + 1
            total_posture_minutes_per_hour[hr] = total_posture_minutes_per_hour[hr] + 1

        offset = posture_data[0].offset

        lying_data = []
        sitting_data = []
        standing_data = []
        for hour in range(0, 24):
            start_time = get_local_datetime(year=posture_data[0].start_time.year,
                                            month=posture_data[0].start_time.month,
                                            day=posture_data[0].start_time.day,
                                            hour=hour,
                                            minute=0,
                                            second=0,
                                            offset=offset)

            end_time = start_time + timedelta(hours=1)

            lying_data.append(DataPoint(start_time=start_time, end_time=end_time,
                                        offset=offset,
                                        sample=[lying_minutes_per_hour[hour],
                                                total_posture_minutes_per_hour[hour]]))
            sitting_data.append(DataPoint(start_time=start_time, end_time=end_time,
                                          offset=offset,
                                          sample=[sitting_minutes_per_hour[hour],
                                                  total_posture_minutes_per_hour[hour]]))
            standing_data.append(DataPoint(start_time=start_time, end_time=end_time,
                                           offset=offset,
                                           sample=[standing_minutes_per_hour[hour],
                                                   total_posture_minutes_per_hour[hour]]))

        self.store_stream(filepath=LYING_HOURLY,
                          input_streams=[streams[POSTURE_STREAMNAME]],
                          user_id=user,
                          data=lying_data)
        self.store_stream(filepath=SITTING_HOURLY,
                          input_streams=[streams[POSTURE_STREAMNAME]],
                          user_id=user,
                          data=sitting_data)
        self.store_stream(filepath=STANDING_HOURLY,
                          input_streams=[streams[POSTURE_STREAMNAME]],
                          user_id=user,
                          data=standing_data)

    def compute_hourly_mean_for_time_of_day(self, activity: dict, days: List[str]) \
            -> Tuple[List[float], List[float], List[float]]:
        """

        Args:
            activity: activity output by hour
            days: all available days

        Returns:
            Tuple[List[float], List[float], List[float]]: mean_walking_minutes_per_hour, mean_moderate_minutes_per_hour, mean_high_minutes_per_hour
        """

        mean_walking_minutes_per_hour = [0] * 24
        mean_moderate_minutes_per_hour = [0] * 24
        mean_high_minutes_per_hour = [0] * 24
        day_count = [0] * 24

        for day in days:
            walk, moderate, high, hour_count = activity[day]
            for i in range(24):
                if hour_count[i] >= 30:
                    mean_walking_minutes_per_hour[i] += (walk[i] * 60) / hour_count[i]
                    mean_moderate_minutes_per_hour[i] += (moderate[i] * 60) / hour_count[i]
                    mean_high_minutes_per_hour[i] += (high[i] * 60) / hour_count[i]
                    day_count[i] = day_count[i] + 1

        for i in range(24):
            if day_count[i] > 0:
                mean_walking_minutes_per_hour[i] /= day_count[i]
                mean_moderate_minutes_per_hour[i] /= day_count[i]
                mean_high_minutes_per_hour[i] /= day_count[i]

        return mean_walking_minutes_per_hour, mean_moderate_minutes_per_hour, mean_high_minutes_per_hour

    def compute_hourly_mean_for_day_of_week(self, activity: dict, days: List[str]) -> Dict[int]:
        """

        Args:
            activity:
            days:

        Returns:

        """

        day_of_week_mean = dict()

        for day_of_week in range(7):
            mean_walking_minutes_per_hour = [0] * 24
            mean_moderate_minutes_per_hour = [0] * 24
            mean_high_minutes_per_hour = [0] * 24
            count = [0] * 24

            same_week_days = [day for day in days if datetime.strptime(day, '%Y%m%d').weekday() == day_of_week]

            for day in same_week_days:
                walking_hour, moderate_hour, high_hour, hour_count = activity[day]
                for i in range(24):
                    if hour_count[i] >= 30:
                        mean_walking_minutes_per_hour[i] += (walking_hour[i] * 60) / hour_count[i]
                        mean_moderate_minutes_per_hour[i] += (moderate_hour[i] * 60) / hour_count[i]
                        mean_high_minutes_per_hour[i] += (high_hour[i] * 60) / hour_count[i]
                        count[i] = count[i] + 1

            for i in range(24):
                if count[i] > 0:
                    mean_walking_minutes_per_hour[i] /= count[i]
                    mean_moderate_minutes_per_hour[i] /= count[i]
                    mean_high_minutes_per_hour[i] /= count[i]

            day_of_week_mean[day_of_week] = [mean_walking_minutes_per_hour,
                                             mean_moderate_minutes_per_hour,
                                             mean_high_minutes_per_hour]
        return day_of_week_mean

    def imputation_by_mean_data(self, activity: dict, days: List[str], user_id: str, stream_name: str, streams: dict, offset: int):


        mean_walk_minutes_per_hour, mean_moderate_minutes_per_hour, mean_high_minutes_per_hour = self.compute_hourly_mean_for_time_of_day(activity, days)

        day_of_week_per_hour_mean = self.compute_hourly_mean_for_day_of_week(activity,days)

        for day in days:

            daytime = datetime.strptime(day, '%Y%m%d')
            week_id = daytime.weekday()

            walking_by_hour, moderate_by_hour, high_by_hour = day_of_week_per_hour_mean[week_id]

            previous_walking_hour, previous_moderate_hour, previous_high_hour, hour_count = activity[day]

            total_walking = sum(previous_walking_hour)
            total_mod = sum(previous_moderate_hour)
            total_high = sum(previous_high_hour)

            walkH_tofd = [0] * 24
            modH_tofd = [0] * 24
            highH_tofd = [0] * 24
            walkH_dofw = [0] * 24
            modH_dofw = [0] * 24
            highH_dofw = [0] * 24

            for i in range(24):
                if hour_count[i] < 30:
                    walkH_tofd[i] = mean_walk_minutes_per_hour[i]
                    modH_tofd[i] = mean_moderate_minutes_per_hour[i]
                    highH_tofd[i] = mean_high_minutes_per_hour[i]
                    walkH_dofw[i] = walking_by_hour[i]
                    modH_dofw[i] = moderate_by_hour[i]
                    highH_dofw[i] = high_by_hour[i]
                else:
                    walkH_tofd[i] = (previous_walking_hour[i] * 60) / hour_count[i]
                    modH_tofd[i] = (previous_moderate_hour[i] * 60) / hour_count[i]
                    highH_tofd[i] = (previous_high_hour[i] * 60) / hour_count[i]

                    walkH_dofw[i] = (previous_walking_hour[i] * 60) / hour_count[i]
                    modH_dofw[i] = (previous_moderate_hour[i] * 60) / hour_count[i]
                    highH_dofw[i] = (previous_high_hour[i] * 60) / hour_count[i]

            total_imputed_time_of_day_walk = sum(walkH_tofd)
            total_imputed_time_of_day_mod = sum(modH_tofd)
            total_imputed_time_of_day_high = sum(highH_tofd)

            total_imputed_day_of_week_walk = sum(walkH_dofw)
            total_imputed_day_of_week_mod = sum(modH_dofw)
            total_imputed_day_of_week_high = sum(highH_dofw)

            y = int(day[:4])
            m = int(day[4:6])
            d = int(day[6:8])

            start_time = get_local_datetime(y, m, d, 0, 0, 0, offset=offset)
            end_time = get_local_datetime(y, m, d, 23, 59, 59, offset=offset)

            if stream_name == ACTIVITY_STREAMNAME:
                self.store_stream(filepath=WALKING_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_walking)])
                self.store_stream(filepath=WALKING_IMPUTED_TIME_OF_DAY_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_imputed_time_of_day_walk)])
                self.store_stream(filepath=WALKING_IMPUTED_DAY_OF_WEEK_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_imputed_day_of_week_walk)])

                self.store_stream(filepath=MODERATE_ACTIVITY_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_mod)])
                self.store_stream(
                    filepath=MODERATE_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY,
                    input_streams=[streams[ACTIVITY_STREAMNAME]],
                    user_id=user_id,
                    data=[DataPoint(start_time=start_time,
                                    end_time=end_time, offset=offset,
                                    sample=total_imputed_time_of_day_mod)])
                self.store_stream(filepath=HIGH_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_imputed_day_of_week_mod)])

                self.store_stream(filepath=HIGH_ACTIVITY_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_high)])
                self.store_stream(filepath=HIGH_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_imputed_time_of_day_high)])
                self.store_stream(filepath=HIGH_ACTIVITY_IMPUTED_DAY_OF_WEEK_DAILY,
                                  input_streams=[streams[ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_imputed_day_of_week_high)])

            if stream_name == ACCEL_ONLY_ACTIVITY_STREAMNAME:
                self.store_stream(filepath=WALKING_DAILY,
                                  input_streams=[streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_walking)])
                self.store_stream(filepath=WALKING_IMPUTED_TIME_OF_DAY_DAILY,
                                  input_streams=[streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_imputed_time_of_day_walk)])
                self.store_stream(filepath=WALKING_IMPUTED_DAY_OF_WEEK_DAILY,
                                  input_streams=[streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_imputed_day_of_week_walk)])

                self.store_stream(filepath=MODERATE_ACTIVITY_DAILY,
                                  input_streams=[streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_mod)])
                self.store_stream(
                    filepath=MODERATE_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY,
                    input_streams=[streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                    user_id=user_id,
                    data=[DataPoint(start_time=start_time,
                                    end_time=end_time, offset=offset,
                                    sample=total_imputed_time_of_day_mod)])
                self.store_stream(filepath=HIGH_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY,
                                  input_streams=[streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_imputed_day_of_week_mod)])

                self.store_stream(filepath=HIGH_ACTIVITY_DAILY,
                                  input_streams=[streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_high)])
                self.store_stream(filepath=HIGH_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY,
                                  input_streams=[streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_imputed_time_of_day_high)])
                self.store_stream(filepath=HIGH_ACTIVITY_IMPUTED_DAY_OF_WEEK_DAILY,
                                  input_streams=[streams[ACCEL_ONLY_ACTIVITY_STREAMNAME]],
                                  user_id=user_id,
                                  data=[DataPoint(start_time=start_time,
                                                  end_time=end_time, offset=offset,
                                                  sample=total_imputed_day_of_week_high)])

    def process(self, user: str, all_days: List[str]):
        """Main entry point for a feature computation module

        Args:
            user: User id (UUID)
            all_days: What days (YYYYMMDD) to compute over

        """

        if self.CC is None:
            return

        if not user:
            return

        streams = self.CC.get_user_streams(user)

        if not streams:
            self.CC.logging.log("Activity and posture features - no streams found for user: %s" % user)
            return

        activity_data = dict()
        activity_data_accelerometer = dict()

        day_list = []
        day_list_accelerometer = []

        offset = 0
        for day in all_days:
            activity_data = self.get_day_data(ACTIVITY_STREAMNAME, user, day)
            if len(activity_data) > 0:
                walking_min_hourly, mod_min_hourly, high_min_hourly, total_min_per_hour = self.compute_activity_features_hourly(ACTIVITY_STREAMNAME,activity_data, streams,user)
                activity_data[day] = [walking_min_hourly, mod_min_hourly, high_min_hourly,total_min_per_hour]
                day_list.append(day)

            activity_data_accelerometer_only = self.get_day_data(ACCEL_ONLY_ACTIVITY_STREAMNAME, user, day)
            if len(activity_data_accelerometer_only) > 0:
                walking_min_hourly_accelerometer_only, mod_min_hourly_accelerometer_only, high_min_hourly_accelerometer_only, total_min_per_hour_accelerometer_only = self.compute_activity_features_hourly(ACCEL_ONLY_ACTIVITY_STREAMNAME, activity_data_accelerometer_only,streams, user)
                activity_data_accelerometer[day] = [walking_min_hourly_accelerometer_only,
                                                    mod_min_hourly_accelerometer_only,
                                                    high_min_hourly_accelerometer_only,
                                                    total_min_per_hour_accelerometer_only]
                day_list_accelerometer.append(day)

            posture_data = self.get_day_data(POSTURE_STREAMNAME, user, day)
            self.compute_posture_features_hourly(posture_data, streams, user)

            if len(activity_data) > 0:
                offset = activity_data[0].offset

        self.imputation_by_mean_data(activity_data, day_list, user,ACTIVITY_STREAMNAME, streams, offset)

        self.imputation_by_mean_data(activity_data_accelerometer, day_list_accelerometer, user,ACCEL_ONLY_ACTIVITY_STREAMNAME, streams,offset)

        self.CC.logging.log("Finished processing activity and posture features for user: %s" % user)

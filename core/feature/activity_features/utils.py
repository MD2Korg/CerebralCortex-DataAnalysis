# Copyright (c) 2018, MD2K Center of Excellence
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

from datetime import timedelta, timezone, datetime

ACTIVITY_STREAMNAME = 'org.md2k.data_analysis.feature.activity.wrist.10_seconds'
ACCEL_ONLY_ACTIVITY_STREAMNAME = 'org.md2k.data_analysis.feature.activity.wrist.accel_only.10_seconds'
POSTURE_STREAMNAME = 'org.md2k.data_analysis.feature.posture.wrist.10_seconds'

NO_ACT = 'NO'
LOW_ACT = 'LOW'
WALKING = 'WALKING'
MODERATE_ACT = 'MOD'
HIGH_ACT = 'HIGH'
ACTIVITY_LABELS = [NO_ACT, LOW_ACT, WALKING, MODERATE_ACT, HIGH_ACT]
ACTIVTY_LABEL_TO_NUMBER_MAPPING = {NO_ACT: 0, LOW_ACT: 1, WALKING: 2, MODERATE_ACT: 3, HIGH_ACT: 4}

LYING = "lying"
SITTING = 'sitting'
STANDING = 'standing'
POSTURE_LABELS = [LYING, SITTING, STANDING]

# json filename
WALKING_HOURLY = 'walking_time_hourly.json'
MODERATE_ACTIVITY_HOURLY = 'moderate_activity_time_hourly.json'
HIGH_ACTIVITY_HOURLY = 'high_activity_time_hourly.json'

WALKING_DAILY = 'walking_time_daily.json'
MODERATE_ACTIVITY_DAILY = 'moderate_activity_time_daily.json'
HIGH_ACTIVITY_DAILY = 'high_activity_time_daily.json'

WALKING_IMPUTED_TIME_OF_DAY_DAILY = 'walking_time_imputed_time_of_day_daily.json'
MODERATE_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY = 'moderate_activity_time_imputed_time_of_day_daily.json'
HIGH_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY = 'high_activity_time_imputed_time_of_day_daily.json'

WALKING_IMPUTED_DAY_OF_WEEK_DAILY = 'walking_time_imputed_day_of_week_daily.json'
MODERATE_ACTIVITY_IMPUTED_DAY_OF_WEEK_DAILY = 'moderate_activity_time_imputed_day_of_week_daily.json'
HIGH_ACTIVITY_IMPUTED_DAY_OF_WEEK_DAILY = 'high_activity_time_imputed_day_of_week_daily.json'

WALKING_HOURLY_ACCEL_ONLY = 'walking_time_hourly_accel_only.json'
MODERATE_ACTIVITY_HOURLY_ACCEL_ONLY = 'moderate_activity_time_hourly_accel_only.json'
HIGH_ACTIVITY_HOURLY_ACCEL_ONLY = 'high_activity_time_hourly_accel_only.json'

WALKING_DAILY_ACCEL_ONLY = 'walking_time_daily_accel_only.json'
MODERATE_ACTIVITY_DAILY_ACCEL_ONLY = 'moderate_activity_time_daily_accel_only.json'
HIGH_ACTIVITY_DAILY_ACCEL_ONLY = 'high_activity_time_daily_accel_only.json'

WALKING_IMPUTED_TIME_OF_DAY_DAILY_ACCEL_ONLY = 'walking_time_imputed_time_of_day_daily_accel_only.json'
MODERATE_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY_ACCEL_ONLY = 'moderate_activity_time_imputed_time_of_day_daily_accel_only.json'
HIGH_ACTIVITY_IMPUTED_TIME_OF_DAY_DAILY_ACCEL_ONLY = 'high_activity_time_imputed_time_of_day_daily_accel_only.json'

WALKING_IMPUTED_DAY_OF_WEEK_DAILY_ACCEL_ONLY = 'walking_time_imputed_day_of_week_daily_accel_only.json'
MODERATE_ACTIVITY_IMPUTED_DAY_OF_WEEK_DAILY_ACCEL_ONLY = 'moderate_activity_time_imputed_day_of_week_daily_accel_only.json'
HIGH_ACTIVITY_IMPUTED_DAY_OF_WEEK_DAILY_ACCEL_ONLY = 'high_activity_time_imputed_day_of_week_daily_accel_only.json'

LYING_HOURLY = 'lying_time_hourly.json'
SITTING_HOURLY = 'sitting_time_hourly.json'
STANDING_HOURLY = 'standing_time_hourly.json'



def get_local_datetime(year, month, day, hour=0, minute=0, second=0, offset=0):
    """

    :rtype: object
    :param year:
    :param month:
    :param day:
    :param hour:
    :param minute:
    :param second:
    :param offset:
    :return:
    """
    tz = timezone(timedelta(milliseconds=offset))
    return datetime(year, month, day, hour, minute, second, tzinfo=tz)

ACTIVITY_STREAMNAME = 'org.md2k.data_analysis.feature.activity.wrist.10_seconds'
ACCEL_ONLY_ACTIVITY_STREAMNAME = 'org.md2k.data_analysis.feature.activity.wrist.accel_only.10_seconds'
POSTURE_STREAMNAME = 'org.md2k.data_analysis.feature.posture.wrist.10_seconds'

NO_ACT = 'NO'
LOW_ACT = 'LOW'
WALKING = 'WALKING'
MODERATE_ACT = 'MOD'
HIGH_ACT = 'HIGH'
ACTIVITY_LABELS = [NO_ACT, LOW_ACT, WALKING, MODERATE_ACT, HIGH_ACT]

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

from datetime import timedelta, timezone, datetime

def get_local_datetime(year, month, day, hour=0, minute=0, second=0, offset=0):
    tz = timezone(timedelta(milliseconds=offset))
    return datetime(year, month, day, hour, min, second, tzinfo=tz)

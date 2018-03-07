study_name = 'mperf'

# Sampling frequency
SAMPLING_FREQ_MOTIONSENSE_ACCEL = 25.0
SAMPLING_FREQ_MOTIONSENSE_GYRO = 25.0

IS_MOTIONSENSE_HRV_GYRO_IN_DEGREE = True

# input filenames
MOTIONSENSE_HRV_ACCEL_RIGHT = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
MOTIONSENSE_HRV_ACCEL_LEFT = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
MOTIONSENSE_HRV_GYRO_RIGHT = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
MOTIONSENSE_HRV_GYRO_LEFT = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"

LEFT_WRIST = 'left_wrist'
RIGHT_WRIST = 'right_wrist'

# Window size
TEN_SECONDS = 10

POSTURE_MODEL_FILENAME = 'core/feature/activity/models/posture_randomforest.model'
ACTIVITY_MODEL_FILENAME = 'core/feature/activity/models/activity_level_randomforest.model'

# Output labels
ACTIVITY_LABELS = ["NO", "LOW", "WALKING", "MOD", "HIGH"]
POSTURE_LABELS = ["lying", "sitting", "standing"]

ACTIVITY_LABELS_INDEX_MAP = {"NO": 0, "LOW": 1, "WALKING": 2, "MOD": 3, "HIGH": 4}
POSTURE_LABELS_INDEX_MAP = {"lying": 0, "sitting": 1, "standing": 2}


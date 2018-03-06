study_name = 'mperf'

# Sampling frequency
SAMPLING_FREQ_RIP = 21.33
SAMPLING_FREQ_MOTIONSENSE_ACCEL = 25
SAMPLING_FREQ_MOTIONSENSE_GYRO = 25

# MACD (moving average convergence divergence) related threshold
FAST_MOVING_AVG_SIZE = 20
SLOW_MOVING_AVG_SIZE = 205

# Hand orientation related threshold
MIN_ROLL = -20
MAX_ROLL = 65
MIN_PITCH = -125
MAX_PITCH = -40

# MotionSense sample range
MIN_MSHRV_ACCEL = -2.5
MAX_MSHRV_ACCEL = 2.5

MIN_MSHRV_GYRO = -250
MAX_MSHRV_GYRO = 250

# Puff label
PUFF_LABEL_RIGHT = 2
PUFF_LABEL_LEFT = 1
NO_PUFF = 0

# Input stream names required for puffmarker
MOTIONSENSE_HRV_ACCEL_LEFT_STREAMNAME = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
MOTIONSENSE_HRV_GYRO_LEFT_STREAMNAME = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
MOTIONSENSE_HRV_ACCEL_RIGHT_STREAMNAME = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
MOTIONSENSE_HRV_GYRO_RIGHT_STREAMNAME = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"

PUFFMARKER_MODEL_FILENAME = 'core/feature/puffmarker/model_file/puffmarker_wrist_randomforest.model'

# Outputs stream names
PUFFMARKER_WRIST_SMOKING_EPISODE = "org.md2k.data_analysis.feature.puffmarker.smoking_episode"
PUFFMARKER_WRIST_SMOKING_PUFF = "org.md2k.data_analysis.feature.puffmarker.smoking_puff"

# smoking episode
MINIMUM_TIME_DIFFERENCE_FIRST_AND_LAST_PUFFS = 7 * 60  # seconds
MINIMUM_INTER_PUFF_DURATION = 5  # seconds
MINIMUM_PUFFS_IN_EPISODE = 4

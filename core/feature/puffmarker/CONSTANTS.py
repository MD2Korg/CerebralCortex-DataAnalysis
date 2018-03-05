study_name = 'mperf'

SAMPLING_FREQ_RIP = 21.33
SAMPLING_FREQ_MOTIONSENSE_ACCEL = 25
SAMPLING_FREQ_MOTIONSENSE_GYRO = 25

# MACD (moving average convergence divergence) realted threshold
fast_moving_avg_size = 20
slow_moving_avg_size = 205

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


# Stream names required for puffmarker
motionsense_hrv_accel_left_streamname = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
motionsense_hrv_gyro_left_streamname = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
motionsense_hrv_accel_right_streamname = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
motionsense_hrv_gyro_right_streamname = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"

#### OUTPUTS STREAM NAMES
puffmarker_wrist_smoking_episode = "org.md2k.data_analysis.feature.puffmarker.smoking_episode"
puffmarker_wrist_smoking_puff= "org.md2k.data_analysis.feature.puffmarker.smoking_puff"


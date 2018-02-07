from core.signalprocessing.gravity_filter.madgwickahrs import *
from core.signalprocessing.gravity_filter.util_quaternion import *
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream

def gravityFilter_function(accl_stream, gyro_stream, Fs=25.0):

    accl = [value.sample for value in accl_stream.data]
    gyro = [value.sample for value in gyro_stream.data]

	# if gyro in degree
    gyro = [[degree_to_radian(v[0]), degree_to_radian(v[1]), degree_to_radian(v[2])] for v in gyro]
    
#     Fs = 16.0
    AHRS_motion = MadgwickAHRS(sampleperiod=(1/Fs), beta=0.4)
    
    quaternion_motion = []

    for t in range(len(accl)):
        AHRS_motion.update_imu(gyro[t], accl[t])
        quaternion_motion.append(AHRS_motion.quaternion.q)

#   filtering out the gravity
    wtist_orientation = [[v[1], v[2], v[3], v[0]] for v in quaternion_motion];
    
    Acc_sync_filtered = []
    gravity_reference = [0, 0, 1, 0];
    for t in range(len(wtist_orientation)):
        Q_temp = Quaternion_multiplication(Quatern_Conjugate(wtist_orientation[t]),gravity_reference)
        gravity_temp = Quaternion_multiplication(Q_temp,  wtist_orientation[t])
        
        x_filtered = accl[t][0] - gravity_temp[0]
        y_filtered = accl[t][1] - gravity_temp[1]
        z_filtered = accl[t][2] - gravity_temp[2]

        Acc_sync_filtered.append([x_filtered, y_filtered, z_filtered])
        Acc_sync_filtered.append(DataPoint() [x_filtered, y_filtered, z_filtered])

    accl_filtered_data = [DataPoint(start_time=value.start_time, end_time=value.end_time, offset=value.offset, sample=Acc_sync_filtered[index]) for  index, value in enumerate(accl_stream.data)]
    accl_filtered_stream = DataStream.from_datastream([accl_stream])
    accl_filtered_stream.data = accl_filtered_data

    return accl_filtered_stream

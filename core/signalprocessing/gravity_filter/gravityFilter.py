# Copyright (c) 2018, MD2K Center of Excellence
# - Sayma Akther <sakther@memphis.edu>
# - Sugavanam, Nithin <sugavanam.3@buckeyemail.osu.edu>
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

from signalprocessing.gravity_filter.madgwickahrs import MadgwickAHRS
from signalprocessing.gravity_filter.util_quaternion import *
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

    for t, value in enumerate(accl):
        AHRS_motion.update_imu(gyro[t], accl[t])
        quaternion_motion.append(AHRS_motion.quaternion.q)

#   filtering out the gravity
    wtist_orientation = [[v[1], v[2], v[3], v[0]] for v in quaternion_motion];
    
    Acc_sync_filtered = []
    gravity_reference = [0, 0, 1, 0]
    for t, value in enumerate(wtist_orientation):
        Q_temp = Quaternion_multiplication(Quatern_Conjugate(value),gravity_reference)
        gravity_temp = Quaternion_multiplication(Q_temp,  wtist_orientation[t])
        
        x_filtered = accl[t][0] - gravity_temp[0]
        y_filtered = accl[t][1] - gravity_temp[1]
        z_filtered = accl[t][2] - gravity_temp[2]

        Acc_sync_filtered.append([x_filtered, y_filtered, z_filtered])

    accl_filtered_data = [DataPoint(start_time=value.start_time, end_time=value.end_time, offset=value.offset, sample=Acc_sync_filtered[index]) for  index, value in enumerate(accl_stream.data)]
    accl_filtered_stream = DataStream.from_datastream([accl_stream])
    accl_filtered_stream.data = accl_filtered_data

    return accl_filtered_stream

# Copyright (c) 2018, MD2K Center of Excellence
# All rights reserved.
# author: Md Azim Ullah
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
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from cerebralcortex.core.datatypes.datapoint import DataPoint
from typing import List
import numpy as np
from core.feature.motionsenseHRVdecode.util_raw_byte_decode \
    import Preprc

motionsense_hrv_left_raw = \
    "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
motionsense_hrv_right_raw = \
    "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"


def get_decoded_matrix(data:np.ndarray,row_length=22):
    """
    given the raw byte array containing lists it returns the decoded values

    :param data: input matrix(*,22) containing raw bytes

    :return: a matrix each row of which contains consecutively sequence
    number,acclx,accly,acclz,gyrox,gyroy,gyroz,red,infrared,green leds,
    timestamp
    """
    ts = [i.start_time.timestamp() for i in data]
    sample = np.zeros((len(ts),row_length))
    sample[:,0] = ts;sample[:,1] = ts
    for k in range(len(ts)):
        sample[k,2:] = [np.int8(np.float(dp)) for dp in (data[k].sample.split(','))]
    ts_temp = np.array([0]+list(np.diff(ts)))
    ind = np.where(ts_temp>1)[0]
    initial = 0
    sample_final  = [0]*int(row_length/2)
    for k in ind:
        sample_temp = Preprc(raw_data=sample[initial:k,:])
        initial = k
        if not list(sample_temp):
            continue
        sample_final = np.vstack((sample_final,sample_temp.values))
    sample_temp = Preprc(raw_data=sample[initial:,:])
    if np.shape(sample_temp)[0]>0:
        sample_final = np.vstack((sample_final,sample_temp.values))
    if np.shape(sample_final)[0] == 1:
        return []
    return sample_final[1:,:]


def admission_control(data:List[DataPoint])->List[DataPoint]:
    final_data = []
    for i,dp in enumerate(data):
        if isinstance(dp.sample,str) and len(dp.sample.split(','))==20:
            final_data.append(dp)
    return final_data
# Copyright (c) 2019, MD2K Center of Excellence
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
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from core.computefeature import ComputeFeatureBase
from core.feature.DecodeHRV.util_raw_byte_decode import Preprc
import numpy as np
from datetime import datetime
from cerebralcortex.core.datatypes.datapoint import DataPoint
from typing import List
feature_class_name = 'DecodeHRV'


class DecodeHRV(ComputeFeatureBase):
    """
    This class takes as input raw datastreams from motionsenseHRV and decodes them to get the Accelerometer, Gyroscope 
    PPG, Sequence number timeseries. Last of all it does timestamp correction on all the timeseries and saves them. 
    """


    def isDatapointsWithinRange(self,red,infrared,green):
        red = np.asarray(red, dtype=np.float32)
        infrared = np.asarray(infrared, dtype=np.float32)
        green = np.asarray(green, dtype=np.float32)
        a =  len(np.where((red >= 40000)& (red<=170000))[0]) < .66*3*25
        b = len(np.where((infrared >= 110000)& (infrared<=230000))[0]) < .66*3*25
        c = len(np.where((green >= 2000)& (green<=20000))[0]) < .66*3*25
        if a and b and c:
            return False
        return True


    def compute_quality(self,window):
        """

        :param window: a window containing list of DataPoints
        :return: an integer reptresenting the status of the window 0= attached, 1 = not attached
        """
        if len(window)==0:
            return 1 #not attached
        red = window[:,0]
        infrared = window[:,1]
        green = window[:,2]
        if not self.isDatapointsWithinRange(red,infrared,green):
            return 1
        if np.mean(red) < 5000 and np.mean(infrared) < 5000:
            return 1
        if not (np.mean(red)>np.mean(green) and np.mean(infrared)>np.mean(red)):
            return 1
        diff = 30000
        if np.mean(red)<130000:
            diff = 15000
        if not np.mean(red) - np.mean(green) > diff:
            return 1
        if not np.mean(infrared) - np.mean(red) >diff:
            return 1
        temp  = [1,1,1]
        if np.std(red)<14:
            temp[0] = 0
        if np.std(infrared)<15:
            temp[1] = 0
        if np.std(green)<13:
            temp[2] = 0
        if np.sum(temp)<2:
            return 1
        return 0


    def get_clean_ppg(self,ppg_data):
        start_ts = ppg_data[0,0]
        final_data = np.zeros((0,11))
        ind = np.array([1,2,3])
        while start_ts < ppg_data[-1,0]:
            index = np.where((ppg_data[:,0]>=start_ts)&(ppg_data[:,0]<start_ts+3000))[0]
            if len(index) > 0:
                temp_data = ppg_data[index,:]
                temp_data = temp_data[:,ind]
                temp_all_data = ppg_data[index,:]
                if self.compute_quality(temp_data)==0:
                    temp_all_data = np.insert(temp_all_data,10,1,axis=1)
                    final_data = np.concatenate((final_data,temp_all_data))
                else:
                    temp_all_data = np.insert(temp_all_data,10,-1,axis=1)
                    final_data = np.concatenate((final_data,temp_all_data))
            start_ts = start_ts + 3000
        return final_data


    def get_decoded_matrix(self,data: np.ndarray, row_length=22):
        """
        given the raw byte array containing lists it returns the decoded values

        :param row_length:
        :param data: input matrix(*,22) containing raw bytes

        :return: a matrix each row of which contains consecutively sequence
        number,acclx,accly,acclz,gyrox,gyroy,gyroz,red,infrared,green leds,
        timestamp
        """
        ts = data[:,0]
        sample = np.zeros((len(ts), row_length))
        sample[:, 0] = ts
        sample[:, 1] = ts
        sample[:,2:] = data[:,2:]
        ts_temp = np.array([0] + list(np.diff(ts)))
        ind = np.where(ts_temp > 300)[0]
        initial = 0
        sample_final = [0] * int(row_length / 2)
        for k in ind:
            sample_temp = Preprc(raw_data=sample[initial:k, :])
            initial = k
            if not list(sample_temp):
                continue
            sample_final = np.vstack((sample_final, sample_temp.values))
        sample_temp = Preprc(raw_data=sample[initial:, :])
        if np.shape(sample_temp)[0] > 0:
            sample_final = np.vstack((sample_final, sample_temp.values))
        if np.shape(sample_final)[0] == 1:
            return []
        return sample_final[1:, :]

    def admission_control(self,data):
        """

        :rtype: list
        :param list data:
        :return:
        """
        final_data = []
        for dp in data:
            if isinstance(dp.sample, str) and len(dp.sample.split(',')) == 20:
                final_data.append(dp)
            if isinstance(dp.sample, list) and len(dp.sample) == 20:
                final_data.append(dp)
        return final_data

    def return_numpy_array_from_datastream_raw(self,data):
        if len(data)==0:
            return np.array([])
        final_data = np.zeros((len(data),20+2))
        for i,dp in enumerate(data):
            final_data[i,:2] = [dp.start_time.timestamp()*1000,dp.offset]
            if isinstance(dp.sample, str):
                str_sample = str(dp.sample)
                str_sample_list = str_sample.split(',')
                Vals = [np.int8(np.float(val)) for val in str_sample_list]
            elif isinstance(dp.sample, list):
                Vals = [np.int8(val) for val in dp.sample]
            final_data[i,2:] = Vals
        return final_data

    def get_datastream_raw(self,
                           identifier:str,
                           day:str,
                           user_id:str,
                           localtime:bool)->List[DataPoint]:
        stream_ids = self.CC.get_stream_id(user_id,identifier)
        data = []
        for stream_id in stream_ids:
            temp_data = self.CC.get_stream(stream_id=stream_id['identifier'],user_id=user_id,day=day,localtime=localtime)
            if len(temp_data.data)>0:
                data.extend(temp_data.data)
        return data


    def save_data(self,decoded_data,offset,tzinfo,json_path,all_streams,user_id,localtime=False):
        final_data = []
        for i in range(len(decoded_data[:, 0])):
            final_data.append(DataPoint.from_tuple(
                start_time=datetime.utcfromtimestamp((decoded_data[i, 0])/1000).replace(tzinfo=tzinfo),
                offset=offset, sample=decoded_data[i, 1:]))
        print(final_data[0],all_streams)
        self.store_stream(json_path, [all_streams],
                          user_id, final_data, localtime=localtime)


    def get_and_save_data(self,
                          all_streams: dict,
                          all_days: list,
                          stream_identifiers: list,
                          user_id: str,
                          json_paths: str,
                          localtime=False):
        """
        all computation and storing of data

        :param all_streams: all streams of a person
        :param all_days: daylist to compute
        :param stream_identifier: left/right wrist HRV raw stream identifier
        :param user_id: user id
        :param json_path: name of json file containing metadata
        
        """

        for day in all_days:
            motionsense_raw = []
            for s in stream_identifiers:
                motionsense_raw.extend(self.get_datastream_raw(s,day,user_id,localtime=localtime))
            if len(motionsense_raw)<100:
                continue
            print(motionsense_raw[0],'first datapoint')
            tzinfo = motionsense_raw[0].start_time.tzinfo
            motionsense_raw = self.admission_control(motionsense_raw)
            if len(motionsense_raw)<100:
                continue
            motionsense_raw_data = self.return_numpy_array_from_datastream_raw(motionsense_raw)
            if len(motionsense_raw_data)<100:
                continue
            offset = motionsense_raw_data[0,1]
            motionsense_raw_data = motionsense_raw_data[motionsense_raw_data[:,0].argsort()]
            decoded_data = self.get_decoded_matrix(motionsense_raw_data)
            if not list(decoded_data):
                continue
            ind_led = np.array([10,7,8,9,1,2,3,4,5,6])
            decoded_data = decoded_data[:,ind_led]
            decoded_data[:,4:7] = decoded_data[:,4:7]*2/16384
            decoded_data[:,7:] = 500.0 * decoded_data[:,7:] / 32768
            marked_data = self.get_clean_ppg(decoded_data)
            if len(marked_data)<100:
                continue
            self.save_data(marked_data,offset,tzinfo,json_paths,all_streams,user_id,localtime)


    def process(self, user, all_days: list):
        """

        Takes the user identifier and the list of days and does the required processing
        
        :param user: user id string
        :param all_days: list of days to compute

        
        """
        if not all_days:
            return
        if self.CC is not None:
            if user:
                streams = self.CC.get_user_streams(user_id=user)
                if streams is None:
                    return None
                user_id = user
                all_streams_left = ['RAW--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST',
                                    "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST",
                                    "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV_PLUS--LEFT_WRIST",
                                    "org.md2k.motionsense.motion_sense_hrv.left_wrist.raw"]
                for s in all_streams_left:
                    if s in streams:
                        json_path = 'decoded_hrv_left_wrist.json'
                        self.get_and_save_data(streams[s], all_days,
                                               all_streams_left,
                                               user_id, json_path)
                        break
                all_streams_right = ['RAW--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST',
                                     "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST",
                                     "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV_PLUS--RIGHT_WRIST",
                                     "org.md2k.motionsense.motion_sense_hrv.right_wrist.raw"]
                for s in all_streams_right:
                    if s in streams:
                        json_path = 'decoded_hrv_right_wrist.json'
                        self.get_and_save_data(streams[s], all_days,
                                               all_streams_right,
                                               user_id, json_path)
                        break
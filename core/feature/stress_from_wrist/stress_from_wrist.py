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
from core.feature.stress_from_wrist.utils.util import acceptable,window_size, \
    window_offset,led_decode_left_wrist,led_decode_right_wrist, \
    Fs,get_final_windowed_data,get_RR_interval_score_HR_for_all,\
    qualtrics_identifier,get_model
import warnings
warnings.filterwarnings("ignore")
from core.computefeature import ComputeFeatureBase
from dateutil import tz
from datetime import timedelta
from core.feature.stress_from_wrist.utils.ecg_feature_computation import \
    ecg_feature_computation
import math
import numpy as np
from scipy.stats import iqr
from scipy.stats import skew
from scipy.stats import kurtosis
from scipy.stats.mstats import gmean, hmean
from copy import deepcopy

feature_class_name = 'stress_from_wrist'

class stress_from_wrist(ComputeFeatureBase):

    def get_data_around_stress_survey(self,
                                      all_streams,
                                      day,
                                      user_id,
                                      raw_byte_array,
                                      offset):
        if qualtrics_identifier in all_streams:
            data = self.CC.get_stream(all_streams[qualtrics_identifier][
                                          'identifier'], user_id=user_id, day=day,localtime=False)
            if len(data.data) > 0:
                data = data.data
                s1 = data[0].end_time
                tzlocal = tz.tzoffset('IST', offset/1000)
                s1 = s1.replace(tzinfo=tzlocal)
                final_data = []
                for dp in raw_byte_array:
                    if s1>dp.start_time and dp.start_time+timedelta(
                            minutes=41)>s1:
                        final_data.append(dp)
                return final_data
        return []


    def windowing(self,streams,decoded_left_raw,decoded_right_raw,user_id,
                  day,window_size,window_offset,acceptable,Fs):
        if not decoded_left_raw.data:
            offset = decoded_right_raw.data[0].offset
            right_data = \
                self.get_data_around_stress_survey(streams,day,
                                                   user_id,
                                                   decoded_right_raw.data,
                                                   offset)
            final_windowed_data = \
                get_final_windowed_data([],
                                        right_data,
                                        window_size=window_size,
                                        window_offset=window_offset)
        elif not decoded_right_raw.data:
            offset = decoded_left_raw.data[0].offset
            left_data = \
                self.get_data_around_stress_survey(streams,day,
                                                   user_id,
                                                   decoded_left_raw.data,
                                                   offset)

            final_windowed_data = get_final_windowed_data(
                left_data,[],
                window_size=window_size,
                window_offset=window_offset)
        else:
            offset = decoded_left_raw.data[0].offset
            right_data = \
                self.get_data_around_stress_survey(streams,day,
                                                   user_id,
                                                   decoded_right_raw.data,
                                                   offset)
            left_data = \
                self.get_data_around_stress_survey(streams,day,
                                                   user_id,
                                                   decoded_left_raw.data,
                                                   offset)

            final_windowed_data = get_final_windowed_data(
                left_data,
                right_data,
                window_size=window_size,
                window_offset=window_offset,
                acceptable=acceptable,
                Fs=Fs)
        return final_windowed_data

    def get_nan_free_HR(self,hr_data):
        hr_data = 60000/hr_data
        hr_data_nan_free = []
        for k in hr_data:
            if not math.isnan(k) and not math.isinf(k):
                hr_data_nan_free.append(k)
        return hr_data_nan_free

    def get_stress_feature_one_window(self,RR_Interval_Realizations,HR_list):
        temp = np.zeros((len(RR_Interval_Realizations),20))
        for i in range(np.shape(temp)[0]):
            rr_final = RR_Interval_Realizations[i]*1000/25
            temp[i,1] = np.percentile(rr_final,85)
            temp[i,2] = np.percentile(rr_final,25)
            temp[i,3] = np.mean(rr_final)
            temp[i,4] = np.median(rr_final)
            temp[i,5] = np.std(rr_final)
            temp[i,6] = iqr(rr_final)
            temp[i,7] = skew(rr_final)
            temp[i,8] = kurtosis(rr_final)
            b = np.copy(ecg_feature_computation(rr_final,
                                                rr_final))
            temp[i,10] = b[1]
            temp[i,11] = b[2]
            temp[i,12] = b[3]
            temp[i,13] = b[4]
            temp[i,14] = b[7]
            temp[i,9] = b[-1]
            temp[i,0] = np.nanmean(HR_list)
            temp[i,15] = np.nanstd(HR_list)
            temp[i,16] = np.nanmean(np.diff(HR_list))
            temp[i,17] = np.nanstd(np.diff(HR_list))
            temp[i,18] = gmean(HR_list)
            temp[i,19] = hmean(HR_list)
        feature = []
        for i in range(np.shape(temp)[1]):
            feature.append(np.median(temp[:,i]))
        return feature

    def get_feature_matrix(self,final_rr_interval_list):
        window_features = []
        for dp in final_rr_interval_list:
            if len(dp.sample)==3:
                RR_Interval_Realizations = dp.sample[0]
                HR_list = self.get_nan_free_HR(dp.sample[2][0])
                if len(HR_list)>0 and len(RR_Interval_Realizations)>0:
                    feature = self.get_stress_feature_one_window(
                        RR_Interval_Realizations,HR_list)
                    window_features.append(deepcopy(dp))
                    window_features[-1].sample = feature
        return window_features


    def process(self, user:str, all_days):

        """
        :param user: user id string
        :param all_days: list of days to compute

        """
        if not list(all_days):
            return

        if self.CC is None:
            return

        if user is None:
            return

        streams = self.CC.get_user_streams(user)

        if streams is None:
            return

        user_id = user

        if qualtrics_identifier not in streams:
            return

        if led_decode_left_wrist not in streams and \
                led_decode_right_wrist not in streams:
            return

        for day in all_days:
            decoded_left_raw = self.CC.get_stream(streams[
                                                  led_decode_left_wrist][
                                                  "identifier"],
                                                  day=day,
                                                  user_id=user_id,
                                                  localtime=True)
            decoded_right_raw = self.CC.get_stream(streams[
                                                   led_decode_right_wrist][
                                                   "identifier"],
                                                   day=day, user_id=user_id,
                                                   localtime=True)

            if not decoded_left_raw.data and not decoded_right_raw.data:
                continue

            final_windowed_data = self.windowing(streams,decoded_left_raw,
                                                 decoded_right_raw,
                                                 user_id,
                                                 day,window_size,
                                                 window_offset,
                                                 acceptable,Fs)

            if not final_windowed_data:
                continue
            final_rr_interval_list = get_RR_interval_score_HR_for_all(
                final_windowed_data)
            window_features = self.get_feature_matrix(final_rr_interval_list)
            model,scaler = get_model()
            final_stress = []
            for dp in window_features:
                sample = np.array(dp.sample)
                sample = scaler.transform(sample.reshape(1,-1))
                stress = model.predict(sample)
                final_stress.append(deepcopy(dp))
                final_stress[-1].sample = stress[0]
            json_path = 'stress_wrist.json'
            self.store_stream(json_path,
                              [streams[led_decode_left_wrist],
                              streams[led_decode_right_wrist]],
                              user_id,
                              final_stress)




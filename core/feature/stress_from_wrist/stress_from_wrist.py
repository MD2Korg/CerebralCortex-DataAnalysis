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

from core.computefeature import ComputeFeatureBase
from core.feature.stress_from_wrist.utils.util import *
from core.feature.stress_from_wrist.utils.ecg_feature_computation import ecg_feature_computation
import math
import numpy as np
from scipy.stats import iqr
from scipy.stats import skew
from scipy.stats import kurtosis
from copy import deepcopy

feature_class_name = 'stress_from_wrist'

class stress_from_wrist(ComputeFeatureBase):


    def get_feature_for_one_window(self,rr):
        temp = np.zeros((len(rr),no_of_feature))
        for k,rr_list in enumerate(rr):
            rr_final = rr_list*1000/25
            temp[k,0] = np.percentile(rr_final,82)
            temp[k,1] = np.percentile(rr_final,18)
            temp[k,2] = np.mean(rr_final)
            temp[k,3] = np.median(rr_final)
            temp[k,4] = np.std(rr_final)
            temp[k,5] = iqr(rr_final)
            temp[k,6] = skew(rr_final)
            temp[k,7] = kurtosis(rr_final)
            b = np.copy(ecg_feature_computation(np.array(rr_final),np.array(rr_final)))
            temp[k,8] = b[1]
            temp[k,9] = b[2]
            temp[k,10] = b[3]
            temp[k,11] = b[4]
            temp[k,12] = b[7]
            temp[k,13] = b[-1]
            pc = []
            for p in range(1,100,1):
                pc.append(np.percentile(rr_final,p))
            temp[k,14] = np.median(np.diff(pc))
            temp[k,15] = np.std(np.diff(pc))
        feature_one_row = np.zeros(1,no_of_feature)
        for j in range(np.shape(temp)[1]):
            feature_one_row[0,j] = np.median(temp[:,j])
        return feature_one_row


    def get_and_save_data(self,streams,day,stream_identifier,user_id,model,scaler,json_path):
        rr_interval_data = self.CC.get_stream(streams[stream_identifier]["identifier"],
                                          day=day,user_id=user_id,localtime=False)
        if not rr_interval_data.data:
            return
        final_data= []
        for dp in rr_interval_data.data:
            if math.isnan(dp.sample[1]):
               continue
            if not list(dp.sample[0]):
                continue
            feature_one_row = self.get_feature_for_one_window(dp.sample[0])
            feature_one_row = scaler.transform(feature_one_row)
            stress_value = model.predict(feature_one_row)
            final_data.append(deepcopy(dp))
            final_data[-1].sample = stress_value
        self.store_stream(json_path,streams[stream_identifier],user_id,final_data,localtime=False)




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

        if rr_interval_identifier not in streams:
            return
        user_id = user
        json_path = 'stress_wrist.json'
        model,scaler = get_model()
        for day in all_days:
            self.get_and_save_data(streams,day,rr_interval_identifier,user_id,model,scaler,json_path)





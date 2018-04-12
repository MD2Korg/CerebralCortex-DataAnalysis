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
import numpy as np
from core.signalprocessing.window import window_sliding
from core.feature.stress_from_respiration.utils.util import *
feature_class_name = 'stress_from_respiration'

from cerebralcortex.core.datatypes.datapoint import DataPoint

class stress_from_respiration(ComputeFeatureBase):

    """
    This class applies a pretrained Support Vector Machine model with radial basis kernel to one minute of 
    respiration cycle features 
    and produces a binary output of Stress/Not Stressed.
    
    The model was trained with python scikit-learn library with 21 participants data.
    The hyperparameters of the model are:
        1. C = 10.0
        2. Gamma = 0.01
    The model takes 14 input features as listed:
        1.  inspiration_duration
        2.  expiration_duration
        3.  respiration_duration
        4.  inspiration_expiration_duration_ratio
        5.  stretch
        6.  inspiration_velocity
        7.  expiration_velocity
        8.  skewness
        9.  kurtosis
        10.  entropy
        11.  inspiration_expiration_velocity_ratio
        12.  inspiration_expiration_area_ratio
        13.  expiration_respiration_duration_ratio
        14.  resspiration_area_inspiration_duration_ratio
    
    In every one minute of data there will be more than 1 respiration cycles each with these 14 features pre-calculated.
    We take the median of each of this feature found within the minute, 
    transform the (1,14) shape feature row according to a pre-trained standard 
    transformation and apply the Support Vector Machine model to get the binary output
    
    The model trained from respiration here has its theoretical underpinnings described in the following paper:
    
    K. Hovsepian, M. al’Absi, E. Ertin, T. Kamarck, M. Nakajima, and S. Kumar, 
    "cStress: Towards a Gold Standard for Continuous Stress Assessment in the Mobile Environment," 
    ACM UbiComp, pp. 493-504, 2015.
        
    """
    def process(self, user:str, all_days):

        """
        Takes the user identifier and the list of days and does the required processing  
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

        if respiration_cycle_feature not in streams:
            return

        for day in all_days:
            respiration_cycle_stream = self.CC.get_stream(streams[
                                                      respiration_cycle_feature][
                                                      "identifier"],
                                                      day=day,
                                                      user_id=user_id,
                                                      localtime=False)
            if len(respiration_cycle_stream.data) < window_size:
                continue
            offset = respiration_cycle_stream.data[0].offset
            windowed_data = window_sliding(respiration_cycle_stream.data,
                                           window_size=window_size,
                                           window_offset=window_offset)
            final_stress = []
            model,scaler = get_model()
            for key in windowed_data.keys():
                st = key[0]
                et = key[0]
                sample = np.array([i.sample for i in windowed_data[key]])
                if np.shape(sample)[0]>1:
                    sample_final = np.zeros((1,14))
                    for k in range(14):
                        sample_final[0,k] = np.median(sample[:,k])
                    sample_transformed = scaler.transform(sample_final)
                    stress = model.predict(sample_transformed)
                    final_stress.append(DataPoint.from_tuple(start_time=st,
                                                             end_time=et,
                                                             sample=stress[0],
                                                             offset=offset))
            json_path = 'stress_respiration.json'
            self.store_stream(json_path,
                              [streams[respiration_cycle_feature]],
                              user_id,
                              final_stress,localtime=False)

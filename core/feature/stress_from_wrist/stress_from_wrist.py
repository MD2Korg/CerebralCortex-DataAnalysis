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
    """
    This class extracts all the RR interval data that is present for a person on a specific day.
    Calculates the necessary features from the rr interval data and assigns each minute of the day as 
    Stress/Not stress state.
    
    Algorithm::

        Input:
            RR interval datastream. 
            Each DataPoint contains the following three things
                1. A list of RR-interval array. Each entry in the list corresponds to a realization 
                of the position of R peaks in that minute
                2. Standard Deviation of Heart Rate within the minute
                3. A list corresponding to the heart rate values calculated from variable realizations 
                of the RR interval on a sliding window of window size = 8 second and window offset = 2 second.

        Steps:
            1. Extract all the RR interval data for a user on a specific day
            2. Extract all the 16 features per minute of the RR interval data
            3. Standardize each feature row and output stress/not stress state
        
        Output:
            A datastream containing a list of datapoints, each DataPoint represents one minute where sample=0
            means the user was not stressed and sample=1 means the person was stressed

        List of Features:
            1. 82nd perentile
            2. 18th perentile
            3. mean
            4. median
            5. standard deviation
            6. inter quartile deviation
            7. skewness
            8. kurtosis
            9. Energy in very low frequency range
            10. Energy in very high frequency range
            11. Energy in low frequency range
            12. Ration of low to high frequency energy 
            13. quartile deviation
            14. heart rate
            15. median of inter percentile difference
            16. standard deviation of inter percentile difference 

    :References
            K. Hovsepian, M. al’Absi, E. Ertin, T. Kamarck, M. Nakajima, and S. Kumar, 
            "cStress: Towards a Gold Standard for Continuous Stress Assessment in the Mobile Environment," 
            ACM UbiComp, pp. 493-504, 2015.
    
    """

    def get_feature_for_one_window(self, rr: list) -> np.ndarray:
        """
        This function takes as input a list of many realizations of where the R peaks are present in one minute of 
        PPG data and calculates 16 features from it.


        :Features:
            1. 82nd percentile
            2. 18th percentile
            3. mean
            4. median
            5. standard deviation
            6. inter quartile deviation
            7. skewness
            8. kurtosis
            9. Energy in very low frequency range
            10. Energy in very high frequency range
            11. Energy in low frequency range
            12. Ration of low to high frequency energy 
            13. quartile deviation
            14. heart rate
            15. median of inter percentile difference
            16. standard deviation of inter percentile difference 
        
        :param list rr: A list of numpy array. Each array contains a realization of RR interval timeseries for the same one
        minute of data 
        :return: A numpy array of shape(1,16) representing the 16 features calculated from one minute of data
        :rtype: np.ndarray
        """

        temp = np.zeros((len(rr), no_of_feature))
        for k, rr_list in enumerate(rr):
            rr_final = rr_list * 1000 / 25
            temp[k, 0] = np.percentile(rr_final, 82)
            temp[k, 1] = np.percentile(rr_final, 18)
            temp[k, 2] = np.mean(rr_final)
            temp[k, 3] = np.median(rr_final)
            temp[k, 4] = np.std(rr_final)
            temp[k, 5] = iqr(rr_final)
            temp[k, 6] = skew(rr_final)
            temp[k, 7] = kurtosis(rr_final)
            b = np.copy(ecg_feature_computation(np.array(rr_final), np.array(rr_final)))
            temp[k, 8] = b[1]
            temp[k, 9] = b[2]
            temp[k, 10] = b[3]
            temp[k, 11] = b[4]
            temp[k, 12] = b[7]
            temp[k, 13] = b[-1]
            pc = []
            for p in range(1, 100, 1):
                pc.append(np.percentile(rr_final, p))
            temp[k, 14] = np.median(np.diff(pc))
            temp[k, 15] = np.std(np.diff(pc))

        feature_one_row = np.zeros((1, no_of_feature))
        for j in range(np.shape(temp)[1]):
            feature_one_row[0, j] = np.median(temp[:, j])

        return feature_one_row

    def get_and_save_data(self, streams: dict,
                          day: str,
                          stream_identifier: str,
                          user_id: str,
                          json_path: str,
                          model,
                          scaler):
        """
        This  function takes all the streams of one user and extracts all the RR interval data the person has for a 
        specific day and calculates a feature matrix of shape (m,16) where m is the number of minutes of rr interval data
        present for the day.
        
        It then transforms the feature matrix on a row by row basis with a pre trained standard transformation and 
        applies the stress model to get a binary output of 0/1 meaning stress/not stress.
        
        It saves the stress datastream
        
        :param dict streams: All the streams of a user
        :param str day: day in string format
        :param str stream_identifier: stream name of rr interval
        :param str user_id: uuid of user
        :param str json_path: the name of the json file where the metadata of stress from wrist is written
        :param model: a sklearn logistic regression model trained  
        :param scaler: a sklearn standard transformation trained to standardize the feature the feature values
        
        """

        rr_interval_data = self.CC.get_stream(streams[stream_identifier]["identifier"],
                                              day=day, user_id=user_id, localtime=False)
        print('-' * 20, " Got rr interval data ", len(rr_interval_data.data), '-' * 20)
        if not rr_interval_data.data:
            return
        final_data = []
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
        print('-' * 20, " Got Stress data ", len(final_data), '-' * 20)
        self.store_stream(json_path, [streams[stream_identifier]], user_id, final_data, localtime=False)

    def process(self, user: str, all_days):

        """
        Takes the user identifier and the list of days and does the required processing  
        :param str user: user id string
        :param list all_days: list of days to compute
        
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
        model, scaler = get_model()
        for day in all_days:
            self.get_and_save_data(streams, day, rr_interval_identifier, user_id, json_path, model, scaler)

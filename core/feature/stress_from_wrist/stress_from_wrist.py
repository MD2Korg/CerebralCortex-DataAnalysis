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
from scipy.stats import variation
from sklearn.preprocessing import StandardScaler
from cerebralcortex.core.datatypes.datapoint import DataPoint
from datetime import datetime,timedelta
import pytz

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

        :Features:
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

    def get_feature_for_one_window(self,
                                   rr:list)->np.ndarray:
        """
        This function takes as input a list of many realizations of where the R peaks are present in one minute of 
        PPG data and calculates 16 features from it.
        The features are:
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
        
        :param list rr: A list of numpy array. Each array contains a realization of RR interval timeseries for the same one
        minute of data 
        :return: A numpy array of shape(1,16) representing the 16 features calculated from one minute of data
        :rtype: np.ndarray
        """

        temp = np.zeros((len(rr),no_of_feature))
        for k,rr_list in enumerate(rr):
            rr_final = rr_list*1000/25
            temp[k,0] = np.percentile(rr_final,82)
            temp[k,1] = np.percentile(rr_final,18)
            temp[k,2] = np.mean(rr_final)
            temp[k,3] = np.median(rr_final)
            temp[k,4] = np.std(rr_final)
            temp[k,5] = iqr(rr_final)

            b = np.copy(ecg_feature_computation(np.cumsum(rr_final),np.array(rr_final)))
            temp[k,6] = b[1]
            temp[k,7] = b[2]
            temp[k,8] = b[3]
            temp[k,9] = b[4]
            temp[k,10] = b[-1]
            pc = np.linspace(1,99,99)
            pc_arr = list(map(lambda x:np.percentile(rr_final,x),pc))
            temp[k,11] = np.std(np.diff(pc_arr))
            temp[k,12] = np.std(pc_arr)
            temp[k,13] = variation(rr_final)
        feature_one_row = np.zeros((1,no_of_feature))
        for j in range(np.shape(temp)[1]):
            feature_one_row[0,j] = np.median(temp[:,j])
        return feature_one_row


    def get_and_save_data(self,streams:dict,
                          day:str,
                          stream_identifier:str,
                          user_id:str,
                          json_path:list):
        """
        This  function takes all the streams of one user and extracts all the RR interval data the person has for a 
        specific day and calculates a feature matrix of shape (m,16) where m is the number of minutes of rr interval data
        present for the day.
        
        It then transforms the feature matrix on a row by row basis with a pre trained standard transformation and 
        applies the stess model to get a binary output of 0/1 meaning stress/not stress.
        
        It saves the stress datastream
        
        :param dict streams: All the streams of a user
        :param str day: day in string format
        :param str stream_identifier: stream name of rr interval
        :param str user_id: uuid of user
        :param list json_path: the names of the json files where the metadata of stress from wrist is written
        
        """

        rr_interval_data = self.CC.get_stream(streams[stream_identifier]["identifier"],
                                              day=day,user_id=user_id,localtime=False)
        print('-'*20," Got rr interval data ", len(rr_interval_data.data) ,'-'*20)

        activity_data = self.CC.get_stream(streams[activity_identifier]["identifier"],
                                           day=day,user_id=user_id,localtime=False)
        if not rr_interval_data.data:
            return

        ts_arr = [i.start_time for i in activity_data.data]
        sample_arr = [i.sample for i in activity_data.data]

        feature_matrix = []
        st_et_offset_array = []
        for dp in rr_interval_data.data:
            ind = np.array([sample_arr[i] for i,item in enumerate(ts_arr) if ts_arr[i]>=dp.start_time and ts_arr[i]<= dp.end_time])

            if list(ind).count('WALKING')+list(ind).count('MOD')+list(ind).count('HIGH')  >= len(ind)*.33:
                continue

            if math.isnan(dp.sample[1]):
                continue

            if not list(dp.sample[0]):
                continue
            feature_one_row = self.get_feature_for_one_window(dp.sample[0])
            feature_matrix.append(feature_one_row)
            st_et_offset_array.append([dp.start_time,dp.offset,dp.end_time])
        if not list(feature_matrix):
            return

        model,scaler = get_model()

        feature_matrix = np.array(feature_matrix).reshape(len(feature_matrix),no_of_feature)
        normalized_feature_matrix = StandardScaler().fit_transform(feature_matrix)
        transformed_feature_matrix = scaler.transform(normalized_feature_matrix)
        stress_value = model.predict(transformed_feature_matrix)
        final_binary_data = []
        for i,dp in enumerate(st_et_offset_array):
            final_binary_data.append(DataPoint.from_tuple(start_time=dp[0],end_time=dp[-1],
                                                          offset=dp[1],
                                                          sample=stress_value[i]))
        print('-'*20,' got stress data ',len(final_binary_data),'-'*20)
        self.store_stream(json_path[0],[streams[stream_identifier]],user_id,final_binary_data,localtime=False)

        stress_likelihood_value = model.predict_proba(transformed_feature_matrix)
        final_likelihood_data = []
        for i,dp in enumerate(st_et_offset_array):
            final_likelihood_data.append(DataPoint.from_tuple(start_time=dp[0],end_time=dp[-1],
                                                              offset=dp[1],
                                                              sample=stress_likelihood_value[i][1]))

        self.store_stream(json_path[1],[streams[stream_identifier]],user_id,final_likelihood_data,localtime=False)

        final_hourly_data = []
        start_dp = final_binary_data[0].start_time
        offset = final_binary_data[0].offset
        start = datetime(year=start_dp.year,month=start_dp.month,day=start_dp.day,hour=start_dp.hour,tzinfo=pytz.UTC)
        init_index = 0
        while start <= final_binary_data[-1].start_time:
            finish = start+timedelta(minutes=59)
            data_in_hour_tuple = np.array([(i,dp.sample) for i,dp in enumerate(final_binary_data[init_index:]) if
                                           finish >= dp.start_time >= start])
            data_in_hour = np.array([i[1] for i in data_in_hour_tuple])
            index_collection = np.array([i[0] for i in data_in_hour_tuple])
            start = start+timedelta(hours=1)
            if not list(data_in_hour):
                continue

            if len(data_in_hour)<10:
                continue

            init_index = max(index_collection)
            hourly_stress_prob = len(np.where(data_in_hour==1)[0])/len(data_in_hour)
            final_hourly_data.append(DataPoint.from_tuple(start_time=start-timedelta(hours=1),
                                                          offset=offset,sample=hourly_stress_prob))
        self.store_stream(json_path[2],[streams[stream_identifier]],user_id,final_hourly_data,localtime=False)

    def process(self, user_id: str, all_days: List[str]):
        """This is the main entry point for feature computation and is called by the main driver application

        Args:
            user_id: User identifier in UUID format
            all_days: List of all days to run this feature over

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
        json_path = ['stress_wrist.json','stress_wrist_likelihood.json','stress_wrist_minute_likelihood.json']
        for day in all_days:
            self.get_and_save_data(streams,day,rr_interval_identifier,user_id,json_path)

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
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from core.computefeature import ComputeFeatureBase
from core.feature.rr_interval.utils.util_helper_functions import *
from datetime import timedelta

feature_class_name = 'rr_interval'


class rr_interval(ComputeFeatureBase):
    """
    This class takes the raw datastream of motionsenseHRV and motionsenseHRV+ which contains a byte array 
    in each DataPoint and decodes them to get the PPG signal in RED,INFRARED,GREEN channel. This is done for
    both left and right/only left/only right sensors whichever is applicable for the person wearing the sensor suite.
    Depending on the presence of PPG signal this code tries to combine information of both the wrists in a one minute 
    window. Then a subspace based method is applied to generate the initial likelihood of the presence of R-peaks in 
    the ppg signals.
    
    This likelihood array is then used to compute the R-peaks through an Bayesian IP algorithm. 
    
    The class outputs three things for every minute of acceptable PPG signal present:
    
        1. A list of RR-interval array. Each entry in the list corresponds to a realization of the position of R peaks 
        in that minute
        2. Standard Deviation of Heart Rate within the minute
        3. A list corresponding to the heart rate values calculated from variable realizations of the RR interval on a 
        sliding window of window size = 8 second and window offset = 2 second.
    """

    def get_data_around_stress_survey(self,
                                      all_streams:dict,
                                      day:str,
                                      user_id:str,
                                      raw_byte_array:list)->list:
        """
        This function checks for qualtrics stress survey data present on the day 
        specified and finds those DataPoints which are only 60 minutes behind the
        time of taking the survey. The motivation is to predict the stress value 
        we would be more concerned with the 60 minutes of data beforehand 
        
        :rtype: list
        :param dict all_streams: a dictionery of all the streams of the partiipant
        :param str day: a string in 'YYYYMMDD' format
        :param str user_id: uuid string representing the user identifier
        :param list raw_byte_array: A list of all the DataPoints for that user on that day
        
        :return: A list of only those DataPoints those are 60 minutes behind the timing of stress survey
        """
        if qualtrics_identifier in all_streams:
            data = self.CC.get_stream(all_streams[qualtrics_identifier][
                                          'identifier'], user_id=user_id, day=day,localtime=False)
            if len(data.data) > 0:
                data = data.data
                final_data = []
                s1 = data[0].end_time
                for dp in raw_byte_array:
                    s2 = dp.start_time
                    if s2 <= s1 <= s2 + timedelta(minutes=120):
                        final_data.append(dp)
                return final_data
        return []

    def process(self, user:str, all_days:list):
        """
        Takes the user identifier and the list of days and does the required processing

        :param user: user id string
        :param all_days: list of days to compute
        """
        if not all_days:
            return
        if self.CC is None:
            return
        if not user:
            return

        all_streams = self.CC.get_user_streams(user_id=user)

        if all_streams is None:
            return

        if motionsense_hrv_left_raw not in all_streams and  \
                        motionsense_hrv_right_raw not in all_streams and \
                        motionsense_hrv_left_raw_cat not in all_streams and  \
                        motionsense_hrv_right_raw_cat not in all_streams:
            return
        # 
        # if qualtrics_identifier not in all_streams:
        #     return

        user_id = user
        for day in all_days:
            # if day_presence in all_streams:
            #     presence = get_latest_stream(self,day_presence,day,user_id,False)
            #     if len(presence)>0:
            #         if presence[0].sample:
            #             continue

            left_data = []
            right_data = []

            if motionsense_hrv_left_raw in all_streams:
                left_data = get_datastream(self.CC,motionsense_hrv_left_raw,day,user_id,False)


            if not left_data:
                if motionsense_hrv_left_raw_cat in all_streams:
                    left_data = get_datastream(self.CC,motionsense_hrv_left_raw_cat,day,user_id,False)



            if motionsense_hrv_right_raw in all_streams:
                right_data = get_datastream(self.CC,motionsense_hrv_right_raw,day,user_id,False)

            if not right_data:
                if motionsense_hrv_right_raw_cat in all_streams:
                    right_data = get_datastream(self.CC,motionsense_hrv_right_raw_cat,day,user_id,False)


            if not left_data and not right_data:
                continue

            left_data = admission_control(left_data)
            right_data = admission_control(right_data)

            print('-'*20,len(left_data),'-'*20,len(right_data),'-'*20,' after admission control length')

            if not left_data and not right_data:
                print('-'*20," No data after admission control ",'-'*20)
                continue


            # left_data = self.get_data_around_stress_survey(all_streams=all_streams,day=day,
            #                                                user_id=user_id,raw_byte_array=left_data)
            # right_data = self.get_data_around_stress_survey(all_streams=all_streams,day=day,
            #                                                user_id=user_id,raw_byte_array=right_data)


            # if not left_data and not right_data:
            #     print('-'*20," No data before 120 minutes of stress survey ",'-'*20)
            #     continue

            left_decoded_data = decode_only(left_data)
            right_decoded_data = decode_only(right_data)
            print('-'*20,len(left_decoded_data),'-'*20,len(right_decoded_data),'-'*20,' decoded length')


            window_data = find_sample_from_combination_of_left_right(left_decoded_data,right_decoded_data)
            if not list(window_data):
                print('-'*20," No window data available ",'-'*20)
                continue
            print('-'*20,len(window_data),'-'*20,' window length')

            final_data_pres = [deepcopy(window_data[0])]
            final_data_pres[0].sample = True


            int_RR_dist_obj,H,w_l,w_r,fil_type = get_constants()
            ecg_pks = []
            final_data = []
            # activity_data = self.CC.get_stream(all_streams[activity_identifier]["identifier"],
            #                                    day=day,user_id=user_id,localtime=False)
            # ts_arr = [i.start_time for i in activity_data.data]
            # sample_arr = [i.sample for i in activity_data.data]

            for dp in window_data:
                # ind = np.array([sample_arr[i] for i,item in enumerate(ts_arr) if ts_arr[i]>=dp.start_time and ts_arr[i]<= dp.end_time])
                # if list(ind).count('WALKING')+list(ind).count('MOD')+list(ind).count('HIGH')  >= len(ind)*.33:
                #     continue
                RR_interval_all_realization,score,HR = [],np.nan,[]
                led_input = dp.sample
                try:
                    [RR_interval_all_realization,score,HR,Time_collection] = GLRT_bayesianIP_HMM(led_input,
                                                                                 H,w_r,w_l,ecg_pks,
                                                                                 int_RR_dist_obj)
                except Exception:
                    continue
                if not list(RR_interval_all_realization):
                    continue
                print("Finished one window successfully with score", score, np.nanmean(HR))
                final_data.append(deepcopy(dp))
                final_data[-1].sample = np.array([RR_interval_all_realization,score,HR,Time_collection])



            if motionsense_hrv_left_raw in all_streams:
                self.store_stream('rr_interval_data_presence.json',
                                  [all_streams[motionsense_hrv_left_raw]],
                                  user_id,
                                  final_data_pres,
                                  localtime=False)

            elif motionsense_hrv_right_raw in all_streams:
                self.store_stream('rr_interval_data_presence.json',
                                  [all_streams[motionsense_hrv_right_raw]],
                                  user_id,
                                  final_data_pres,
                                  localtime=False)
            elif motionsense_hrv_left_raw_cat in all_streams:
                self.store_stream('rr_interval_data_presence.json',
                                  [all_streams[motionsense_hrv_left_raw_cat]],
                                  user_id,
                                  final_data_pres,
                                  localtime=False)
            else:
                self.store_stream('rr_interval_data_presence.json',
                                  [all_streams[motionsense_hrv_right_raw_cat]],
                                  user_id,
                                  final_data_pres,
                                  localtime=False)

            if not list(final_data):
                continue




            json_path = 'rr_interval.json'
            if motionsense_hrv_left_raw in all_streams:
                self.store_stream(json_path,
                              [all_streams[motionsense_hrv_left_raw]],
                              user_id,
                              final_data,localtime=False)

            elif motionsense_hrv_right_raw in all_streams:
                self.store_stream(json_path,
                                  [all_streams[motionsense_hrv_right_raw]],
                                  user_id,
                                  final_data,localtime=False)
            elif motionsense_hrv_left_raw_cat in all_streams:
                self.store_stream(json_path,
                                  [all_streams[motionsense_hrv_left_raw_cat]],
                                  user_id,
                                  final_data,localtime=False)
            else:
                self.store_stream(json_path,
                                  [all_streams[motionsense_hrv_right_raw_cat]],
                                  user_id,
                                  final_data,localtime=False)






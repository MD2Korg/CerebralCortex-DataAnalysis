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
    def get_data_around_stress_survey(self,
                                      all_streams,
                                      day,
                                      user_id,
                                      raw_byte_array):
        if qualtrics_identifier in all_streams:
            data = self.CC.get_stream(all_streams[qualtrics_identifier][
                                          'identifier'], user_id=user_id, day=day,localtime=True)
            if len(data.data) > 0:
                data = data.data
                final_data = []
                s1 = data[0].end_time + timedelta(seconds = data[0].offset/1000)
                for dp in raw_byte_array:
                    s2 = dp.start_time + timedelta(seconds = dp.offset/1000)
                    if s1 > s2 and s2 + timedelta(minutes=60) > s1 :
                        final_data.append(dp)
                return final_data
        return []



    def process(self, user, all_days):
        """

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

        if motionsense_hrv_left_raw not in all_streams and  motionsense_hrv_right_raw not in all_streams:
            return
        if qualtrics_identifier not in all_streams:
            return

        user_id = user
        for day in all_days:
            qualtrics = self.CC.get_stream(all_streams[qualtrics_identifier]["identifier"],
                                           day=day,user_id=user_id,localtime=True)
            if len(qualtrics.data)==0:
                continue
            motionsense_raw_left = self.CC.get_stream(all_streams[motionsense_hrv_left_raw]["identifier"],
                                                      day=day,user_id=user_id,localtime=True)
            motionsense_raw_right = self.CC.get_stream(all_streams[motionsense_hrv_right_raw]["identifier"],
                                                       day=day,user_id=user_id,localtime=True)

            if not motionsense_raw_left.data and not motionsense_raw_right.data:
                continue


            motionsense_raw_left_data = admission_control(motionsense_raw_left.data)
            motionsense_raw_right_data = admission_control(motionsense_raw_right.data)


            left_data = self.get_data_around_stress_survey(all_streams,day,user_id,
                                                           motionsense_raw_left_data)
            right_data = self.get_data_around_stress_survey(all_streams,day,user_id,
                                                            motionsense_raw_right_data)


            if not left_data and not right_data:
                continue


            left_decoded_data = decode_only(left_data)
            right_decoded_data = decode_only(right_data)

            window_data = find_sample_from_combination_of_left_right(left_decoded_data,right_decoded_data)

            int_RR_dist_obj,H,w_l,w_r,fil_type = get_constants()
            ecg_pks = []
            final_data = []
            for dp in window_data:
                RR_interval_all_realization,score,HR = [],0,[]
                led_input = dp.sample
                try:
                    [RR_interval_all_realization,score,HR] = GLRT_bayesianIP_HMM(led_input,
                                                                                    H,w_r,w_l,ecg_pks,
                                                                                int_RR_dist_obj)
                except Exception:
                    continue
                if not list(RR_interval_all_realization) and not list(HR):
                    continue
                print("Finished one window successfully")
                final_data.append(deepcopy(dp))
                final_data[-1].sample = np.array([RR_interval_all_realization,score,HR])
            # json_path = 'rr_interval.json'
            # self.store_stream(json_path,
            #                   [all_streams[motionsense_hrv_left_raw]],
            #                   user_id,
            #                   final_data,localtime=False)





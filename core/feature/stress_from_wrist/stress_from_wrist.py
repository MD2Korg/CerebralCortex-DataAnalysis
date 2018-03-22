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
from core.feature.motionsenseHRVdecode.motionsenseHRVdecode import DecodeHRV
from core.feature.stress_from_wrist.utils.util import Fs,acceptable,window_size, \
    window_offset,led_decode_left_wrist,led_decode_right_wrist, \
    led_decode_left_wrist1,led_decode_right_wrist1,get_constants,get_stream_days
from cerebralcortex.cerebralcortex import CerebralCortex
from core.feature.stress_from_wrist.utils.combine_left_right_ppg import *
import warnings
warnings.filterwarnings("ignore")
from copy import deepcopy
from core.feature.stress_from_wrist.utils.preprocessing_LED import *
from core.signalprocessing.window import window_sliding

CC = CerebralCortex()
users = CC.get_all_users("mperf-alabsi")
# x = DecodeHRV()
for user in users:
    streams = CC.get_user_streams(user['identifier'])
    user_id = user["identifier"]
    if led_decode_left_wrist1 in streams:

        stream_days_left = get_stream_days(streams[led_decode_left_wrist1][
                                               "identifier"],
                                           CC)

        stream_days_right = get_stream_days(streams[led_decode_right_wrist1][
                                                "identifier"],CC)
        common_days = list(set(stream_days_left) & set(stream_days_right))
        left_only_days = list(set(stream_days_left) - set(stream_days_right))
        right_only_days = list(set(stream_days_right) - set(stream_days_left))
        union_of_days_list = list(set(stream_days_left) | set(
            stream_days_right))

        for day in union_of_days_list:
            if day in common_days:
                decoded_left_raw = CC.get_stream(streams[
                                                     led_decode_left_wrist1][
                                                         "identifier"],
                                                     day=day, user_id=user_id)
                decoded_right_raw = CC.get_stream(streams[
                                                      led_decode_right_wrist1][
                                                     "identifier"],
                                                 day=day, user_id=user_id)

                final_windowed_data = find_sample_from_combination_of_left_right_or_one(
                    decoded_left_raw.data,decoded_right_raw.data,
                    window_size=window_size,window_offset=window_offset,
                    Fs=Fs,acceptable=acceptable)
            elif day in left_only_days:
                decoded_left_raw = CC.get_stream(streams[
                                                     led_decode_left_wrist1][
                                                     "identifier"],
                                                 day=day, user_id=user_id)

                windowed_data = window_sliding(decoded_left_raw.data,
                                                     window_size=window_size,
                                                     window_offset=window_offset)
                final_windowed_data = []
                for key in windowed_data.keys():
                    final_windowed_data.append(DataPoint.from_tuple(
                        start_time=key[0],
                        end_time=key[1],
                        sample = np.array([i.sample[6:] for i in windowed_data[
                        key]])))
            else:
                decoded_right_raw = CC.get_stream(streams[
                                                      led_decode_right_wrist1][
                                                      "identifier"],
                                                  day=day, user_id=user_id)

                windowed_data = window_sliding(decoded_right_raw.data,
                                               window_size=window_size,
                                               window_offset=window_offset)
                final_windowed_data = []
                for key in windowed_data.keys():
                    final_windowed_data.append(DataPoint.from_tuple(
                        start_time=key[0],
                        end_time=key[1],
                        sample = np.array([i.sample[6:] for i in windowed_data[
                            key]])))

            print(final_windowed_data)
            int_RR_dist_obj,H,w_l,w_r,fil_type = get_constants()

            # for dp in final_windowed_data:
            #     try:
            #         st = dp.start_time
            #         et = dp.end_time
            #         print(np.shape(dp.sample['left']),np.shape(
            #             dp.sample['right']),st,et,user_id)
            #         if not list(dp.sample['left']):
            #             print('0000000000000000000000000000000')
            #             X_ppg = dp.sample['right']
            #             t_start = dp.start_time.timestamp()
            #             t_end = dp.end_time.timestamp()
            #             Fs_ppg = (np.shape(X_ppg)[0]/(t_end-t_start))
            #             X_ppg_fil = preProcessing(X0=X_ppg,Fs=Fs_ppg,fil_type=fil_type)
            #             RR_interval_all_realization,score,HR = GLRT_bayesianIP_HMM(
            #                 X_ppg_fil,H,w_r,w_l,[],int_RR_dist_obj)
            #             if not RR_interval_all_realization:
            #                 continue
            #             print('saving only')
            #             data = np.array([RR_interval_all_realization,score,HR,label,
            #                              user_id,day])
            #             print(score)
            #             np.savez('./windows1/'+str(count),data=data)
            #             count+=1
            #         elif not list(dp.sample['right']):
            #             print('11111111111111111111111111111')
            #             X_ppg = dp.sample['left']
            #             t_start = dp.start_time.timestamp()
            #             t_end = dp.end_time.timestamp()
            #             Fs_ppg = (np.shape(X_ppg)[0]/(t_end-t_start))
            #             X_ppg_fil = preProcessing(X0=X_ppg,Fs=Fs_ppg,fil_type=fil_type)
            #             RR_interval_all_realization,score,HR = GLRT_bayesianIP_HMM(
            #                 X_ppg_fil,H,w_r,w_l,[],int_RR_dist_obj)
            #             if not RR_interval_all_realization:
            #                 continue
            #             print('saving only')
            #             data = np.array([RR_interval_all_realization,score,HR,label,
            #                              user_id,day])
            #             print(score)
            #             np.savez('./windows1/'+str(count),data=data)
            #             count+=1
            #         elif np.shape(dp.sample['left'])[0]>0 and np.shape(
            #                 dp.sample['right'])[0]>0:
            #
            #             X_ppg = deepcopy(dp.sample['left'])
            #             print(X_ppg)
            #             np.savetxt('./windows1/left.csv',X_ppg,delimiter=',')
            #             t_start = dp.start_time.timestamp()
            #             t_end = dp.end_time.timestamp()
            #             Fs_ppg = (np.shape(X_ppg)[0]/(t_end-t_start))
            #             print(Fs_ppg)
            #             X_ppg_fil = preProcessing(X0=X_ppg,Fs=Fs_ppg,fil_type=fil_type)
            #             RR_interval_all_realization_l,score_l, \
            #             HR_l = GLRT_bayesianIP_HMM(
            #                 X_ppg_fil,H,w_r,w_l,[],int_RR_dist_obj)
            #
            #             X_ppg = deepcopy(dp.sample['right'])
            #             t_start = dp.start_time.timestamp()
            #             t_end = dp.end_time.timestamp()
            #             Fs_ppg = (np.shape(X_ppg)[0]/(t_end-t_start))
            #             print(Fs_ppg)
            #             X_ppg_fil = preProcessing(X0=X_ppg,Fs=Fs_ppg,fil_type=fil_type)
            #
            #
            #             RR_interval_all_realization_r,score_r, \
            #             HR_r = GLRT_bayesianIP_HMM(
            #                 X_ppg_fil,H,w_r,w_l,[],int_RR_dist_obj)
            #             print(score_l,score_r)
            #             if not RR_interval_all_realization_l and not \
            #                     RR_interval_all_realization_r:
            #                 continue
            #             elif not RR_interval_all_realization_l:
            #                 data = np.array([RR_interval_all_realization_r,
            #                                  score_r,HR_r,label,
            #                                  user_id,day])
            #                 np.savez('./windows1/'+str(count),data=data)
            #                 count+=1
            #                 print(score_r)
            #                 print('saving right')
            #                 np.savetxt('./windows1/left.csv',dp.sample['right'])
            #                 print('stopppppppppppppppppppppp')
            #                 break
            #             elif not RR_interval_all_realization_r:
            #                 data = np.array([RR_interval_all_realization_l,
            #                                  score_l,HR_l,label,
            #                                  user_id,day])
            #                 np.savez('./windows1/'+str(count),data=data)
            #                 count+=1
            #                 print(score_l)
            #                 print('saving left')
            #             else:
            #                 print('saving_comparison')
            #
            #                 data = np.array([RR_interval_all_realization_l,
            #                                  score_l,HR_l,label,
            #                                  user_id,day])
            #                 np.savez('./windows1/'+str(count),data=data)
            #                 print(score_l)
            #                 count+=1
            #                 data = np.array([RR_interval_all_realization_r,
            #                                  score_r,HR_r,label,
            #                                  user_id,day])
            #                 np.savez('./windows1/'+str(count),data=data)
            #                 print(score_r)
            #                 count+=1
            #
            #     except Exception:
            #         pass

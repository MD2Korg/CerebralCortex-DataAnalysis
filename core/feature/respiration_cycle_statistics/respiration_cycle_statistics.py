# Copyright (c) 2018, MD2K Center of Excellence
# All rights reserved.
# author: Md Azim Ullah, Rummana Bari
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
from core.feature.respiration_cycle_statistics.utils.peak_valley import \
    compute_peak_valley
from core.feature.respiration_cycle_statistics.utils. \
    rip_cycle_feature_computation import rip_cycle_feature_computation
from core.feature.respiration_cycle_statistics.utils.util import *

feature_class_name = 'respiration_cycle_statistics'


class respiration_cycle_statistics(ComputeFeatureBase):
    """
    This class combines the respiration raw datastream and 
    respiration baseline datastream, applies the peak valley 
    detection code to find out the respiration cycles and then 
    computes 21 features for each respiration cycle and 
    stores them
      
    ..Algorithm:
        1. Filter the combined respiration raw and baseline datastream to get rid of the signal at 
        times when the person was not wearing the sensor suite
        2. Identify the Respiration Cycles by getting the peak and valley points
        3. Compute the features for each cycle
    
    """

    def get_feature_matrix(self,final_respiration:List[DataPoint])->List[DataPoint]:

        """
        
        :param final_respiration: A list of datapoints after combining 
        the respiration datastream and the respiration baseline datastream
        
        :return: A list of datapoints. Each datapoint contains a list of 21 values 
        each representing the 21 separate features
        calculated from one respiration cycle
        The features are,
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
        15.  power_.05-.2_Hz
        16.  power_.201-.4_Hz
        17.  power_.401-.6_Hz
        18.  power_.601-.8_Hz
        19.  power_.801-1_Hz
        20.  correlation_previous_cycle
        21.  correlation_next_cycle
        
        """
        if final_respiration is None:
            return []
        if not final_respiration:
            return []

        respiration_final = [i for i in final_respiration if (not isinstance(
            i.sample,list)) and i.sample>0]

        if not respiration_final:
            return []

        sample = np.array([i.sample for i in respiration_final])
        ts = np.array([i.start_time.timestamp() for i in respiration_final])

        sample_smoothed_detrened = smooth_detrend(sample,ts)

        sample_filtered,ts_filtered,indexes =filter_bad_rip(ts,
                                                            sample_smoothed_detrened)

        if len(indexes)==0:
            return []

        respiration_final_smoothed_detrended_filtered =np.array(respiration_final)[indexes]
        offset = respiration_final_smoothed_detrended_filtered[0].offset
        peak,valley = compute_peak_valley(rip=respiration_final_smoothed_detrended_filtered)

        if len(peak)==0 or len(valley)==0:
            return []

        feature = rip_cycle_feature_computation(peak,valley)

        inspiration_duration, expiration_duration, respiration_duration, \
        inspiration_expiration_ratio, stretch= feature[2:7]

        cycle_quality, corr_pre_cycle,corr_post_cycle = \
            return_neighbour_cycle_correlation(sample_filtered,ts_filtered,
                                               inspiration_duration)
        quality_area_velocity_shape = \
            respiration_area_shape_velocity_calculation(sample_filtered,
                                                        ts_filtered,peak,
                                                        cycle_quality)
        cycle_quality,area_Inspiration,area_Expiration, \
        area_Respiration,area_ie_ratio, \
        velocity_Inspiration,velocity_Expiration, shape_skew, shape_kurt \
            = quality_area_velocity_shape

        entropy_array = spectral_entropy_calculation(sample_filtered,
                                                     ts_filtered,
                                                     cycle_quality)

        energyX,FQ_05_2_Hz,FQ_201_4_Hz,FQ_401_6_Hz,FQ_601_8_Hz, \
        FQ_801_1_Hz = spectral_energy_calculation(sample_filtered,
                                                  ts_filtered,
                                                  cycle_quality)

        conversation_feature = []
        for i,dp in enumerate(cycle_quality):
            if dp.sample == Quality.UNACCEPTABLE:
                continue
            temp = np.zeros((21,))
            temp[0] = inspiration_duration[i].sample
            temp[1] = expiration_duration[i].sample
            temp[2] = respiration_duration[i].sample
            temp[3] = temp[0]/temp[1]
            temp[4] = stretch[i].sample
            temp[5] = velocity_Inspiration[i].sample
            temp[6] = velocity_Expiration[i].sample
            temp[7] = shape_skew[i].sample
            temp[8] = shape_kurt[i].sample
            temp[9] = entropy_array[i].sample
            temp[10] = temp[5]/temp[6]
            temp[11] = area_ie_ratio[i].sample
            temp[12] = temp[1]/temp[2]
            temp[13] = area_Respiration[i].sample/temp[0]
            temp[14] = FQ_05_2_Hz[i].sample
            temp[15] = FQ_201_4_Hz[i].sample
            temp[16] = FQ_401_6_Hz[i].sample
            temp[17] = FQ_601_8_Hz[i].sample
            temp[18] = FQ_801_1_Hz[i].sample
            temp[19] = corr_pre_cycle[i].sample
            temp[20] = corr_post_cycle[i].sample
            conversation_feature.append(deepcopy(dp))
            conversation_feature[-1].sample = temp
            conversation_feature[-1].offset = offset

        return conversation_feature



    def process(self, user:str, all_days:list):

        """
        Takes the user identifier and the list of days and does the required processing  
        :param user: user id string
        :param all_days: list of days to compute

        """
        if not list(all_days):
            return

        if self.CC is not None:
            if user:
                streams = self.CC.get_user_streams(user)

                if streams is None:
                    return

                user_id = user

                if respiration_raw_autosenseble in streams:
                    for day in all_days:
                        rip_raw = self.CC.get_stream(streams[
                                                     respiration_raw_autosenseble][
                                                     "identifier"], day=day,
                                                     user_id=user_id,
                                                     localtime=False)

                        rip_baseline = self.CC.get_stream(streams[
                                                          respiration_baseline_autosenseble][
                                                          "identifier"],day=day,
                                                          user_id=user_id,
                                                          localtime=False)
                        if not rip_raw.data:
                            continue
                        elif not rip_baseline.data:
                            respiration_data = admission_control(rip_raw.data)
                            if len(respiration_data)>0:
                                final_respiration = respiration_data
                            else:
                                continue
                        else:
                            respiration_data = admission_control(rip_raw.data)
                            baseline_data = admission_control(rip_baseline.data)
                            if not respiration_data:
                                continue
                            elif not baseline_data:
                                final_respiration = respiration_data
                            else:
                                final_respiration = get_recovery(
                                    respiration_data,baseline_data,Fs=Fs)

                        feature_matrix = self.get_feature_matrix(final_respiration)
                        if not feature_matrix:
                            continue
                        json_path = 'respiration_cycle_feature.json'
                        self.store_stream(json_path,
                                          [streams[respiration_raw_autosenseble]],
                                          user_id,
                                          feature_matrix,localtime=False)






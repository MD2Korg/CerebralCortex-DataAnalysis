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
    Fs,get_final_windowed_data,get_RR_interval_score_HR_for_all,qualtrics_identifier
import warnings
warnings.filterwarnings("ignore")
from core.computefeature import ComputeFeatureBase
from dateutil.parser import parse
import json

feature_class_name = 'stress_from_wrist'

class stress_from_wrist(ComputeFeatureBase):

    def get_data_around_stress_survey(self,
                                      all_streams,
                                      day,
                                      user_id,
                                      raw_byte_array):
        if qualtrics_identifier in all_streams:
            data = self.CC.get_stream(all_streams[qualtrics_identifier][
                                          'identifier'], user_id=user_id, day=day,localtime=False)
            if len(data.data) > 0:
                data = data.data
                s1 = parse(json.loads(data[0].sample)["RecordedDate"])
                final_data = []
                for dp in raw_byte_array:
                    if s1.timestamp()>=dp.start_time.timestamp():
                        final_data.append(dp)
                return final_data
        return []


    def windowing(self,streams,decoded_left_raw,decoded_right_raw,user_id,
                  day,window_size,window_offset,acceptable,Fs):
        if not decoded_left_raw.data:
            right_data = \
                self.get_data_around_stress_survey(streams,day,
                                                   user_id,
                                                   decoded_right_raw.data)
            final_windowed_data = \
                get_final_windowed_data([],
                                        right_data,
                                        window_size=window_size,
                                        window_offset=window_offset)
        elif not decoded_right_raw.data:
            left_data = \
                self.get_data_around_stress_survey(streams,day,
                                                   user_id,
                                                   decoded_left_raw.data)

            final_windowed_data = get_final_windowed_data(
                left_data,[],
                window_size=window_size,
                window_offset=window_offset)
        else:
            right_data = \
                self.get_data_around_stress_survey(streams,day,
                                                   user_id,
                                                   decoded_right_raw.data)
            left_data = \
                self.get_data_around_stress_survey(streams,day,
                                                   user_id,
                                                   decoded_left_raw.data)

            final_windowed_data = get_final_windowed_data(
                left_data,
                right_data,
                window_size=window_size,
                window_offset=window_offset,
                acceptable=acceptable,
                Fs=Fs)
        return final_windowed_data



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
                                                  user_id=user_id)
            decoded_right_raw = self.CC.get_stream(streams[
                                                   led_decode_right_wrist][
                                                   "identifier"],
                                                   day=day, user_id=user_id)

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




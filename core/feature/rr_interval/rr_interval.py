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

from core.feature.rr_interval.utils.util import *
from core.feature.rr_interval.utils.get_store import *
from cerebralcortex.cerebralcortex import CerebralCortex
from core.feature.rr_interval.utils.combine_left_right_ppg import *

CC = CerebralCortex()
users = CC.get_all_users("mperf-alabsi")

for user in users[1:2]:
    streams = CC.get_user_streams(user['identifier'])
    user_id = user["identifier"]
    user_data_collection = {}
    if led_decode_left_wrist in streams:
        stream_days_left = get_stream_days(streams[led_decode_left_wrist]["identifier"],
                                           CC)
        stream_days_right = get_stream_days(streams[led_decode_left_wrist][
                                            "identifier"],CC)
        common_days = list(set(stream_days_left) & set(stream_days_right))
        left_only_days = list(set(stream_days_left) - set(stream_days_right))
        right_only_days = list(set(stream_days_right) - set(stream_days_left))
        union_of_days_list = list(set(stream_days_left) | set(
            stream_days_right))

        for day in union_of_days_list:
            if day in common_days:
                decoded_left_raw = CC.get_stream(streams[led_decode_left_wrist][
                                                         "identifier"],
                                                     day=day, user_id=user_id)
                decoded_right_raw = CC.get_stream(streams[
                                                      led_decode_right_wrist][
                                                     "identifier"],
                                                 day=day, user_id=user_id)

                final_windowed_data = find_sample_from_combination_of_left_right_or_one(
                    decoded_left_raw.data,decoded_right_raw.data,
                    window_size=window_size,window_offset=window_offset,
                    Fs=Fs,acceptable=acceptable)
            elif day in left_only_days:
                decoded_left_raw = CC.get_stream(streams[led_decode_left_wrist][
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
                                                      led_decode_right_wrist][
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
            for dp in final_windowed_data:
                print(np.shape(dp.sample))
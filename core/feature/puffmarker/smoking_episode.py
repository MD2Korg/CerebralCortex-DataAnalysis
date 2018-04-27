# Copyright (c) 2018, MD2K Center of Excellence
# - Nazir Saleheen <nazir.saleheen@gmail.com>
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


from core.feature.puffmarker.utils import *
from cerebralcortex.core.datatypes.datapoint import DataPoint


def get_smoking_wrist(only_puff_list, start_index, end_index):
    '''
    Based on majority voting detects smoking hand

    :param only_puff_list:
    :param start_index:
    :param end_index:
    :return:
    '''
    n_left_wrist = 0
    n_right_wirst = 0
    i = start_index
    while i < end_index:
        if only_puff_list[i].sample == 1:
            n_left_wrist = n_left_wrist + 1
        else:
            n_right_wirst = n_right_wirst + 1
        i = i + 1
    if n_right_wirst > n_left_wrist:
        return PUFF_LABEL_RIGHT
    return PUFF_LABEL_LEFT


def generate_smoking_episode(puff_labels) -> List[DataPoint]:
    '''
    Generates smoking episodes from classified puffs
    :param puff_labels:
    :return: list of smoking episodes
    '''
    only_puffs = [dp for dp in puff_labels if dp.sample > 0]

    smoking_episode_data = []

    cur_index = 0
    while cur_index < len(only_puffs):
        temp_index = cur_index
        dp = only_puffs[temp_index]
        prev = dp
        temp_index = temp_index + 1
        if temp_index >= len(only_puffs):
            break
        while (((only_puffs[temp_index].start_time - dp.start_time <= timedelta(
                seconds=MINIMUM_TIME_DIFFERENCE_FIRST_AND_LAST_PUFFS))
                | (only_puffs[
                       temp_index].start_time - prev.start_time < timedelta(
                    seconds=MINIMUM_INTER_PUFF_DURATION)))):
            prev = only_puffs[temp_index]
            temp_index = temp_index + 1
            if temp_index >= len(only_puffs):
                break
        temp_index = temp_index - 1
        if (temp_index - cur_index + 1) >= MINIMUM_PUFFS_IN_EPISODE:
            wrist = get_smoking_wrist(only_puffs, cur_index, temp_index)
            smoking_episode_data.append(
                DataPoint(start_time=only_puffs[cur_index].start_time,
                          end_time=only_puffs[temp_index].end_time,
                          offset=only_puffs[cur_index].offset,
                          sample=wrist))

            cur_index = temp_index + 1
        else:
            cur_index = cur_index + 1
    return smoking_episode_data

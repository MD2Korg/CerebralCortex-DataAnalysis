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


from cerebralcortex.core.datatypes.datapoint import DataPoint
from datetime import timedelta

MINIMUM_TIME_DIFFERENCE_BETWEEN_EPISODES = 10 * 60 * 1000
MINIMUM_TIME_DIFFERENCE_FIRST_AND_LAST_PUFFS = 7  # minutes
MINIMUM_INTER_PUFF_DURATION = 5  # seconds
MINIMUM_PUFFS_IN_EPISODE = 4

def getSmokingWrist(onlyPuffList, indx, end_indx):
    nLeftWrst = 0
    nRightWrst = 0
    i = indx
    while (i < end_indx):
        if (onlyPuffList[i].sample == 1):
            nLeftWrst = nLeftWrst + 1
        else:
            nRightWrst = nRightWrst + 1
        i = i + 1
    if (nRightWrst > nLeftWrst):
        return 2
    return 1


def generate_smoking_episode(puff_labels):

    only_puff_list = [dp for dp in puff_labels if dp.sample > 0]

    smoking_episode_data = []

    indx = 0
    while (indx < len(only_puff_list)):
        i = indx
        dp = only_puff_list[i]
        prev = dp
        i = i + 1
        if i >= len(only_puff_list):
            break
        while (
        ((only_puff_list[i].start_time - dp.start_time <= timedelta(minutes=MINIMUM_TIME_DIFFERENCE_FIRST_AND_LAST_PUFFS))
         | (only_puff_list[i].start_time - prev.start_time < timedelta(seconds=MINIMUM_INTER_PUFF_DURATION)))):
            prev = only_puff_list[i]
            i = i + 1
            if i >= len(only_puff_list):
                break
        i = i - 1
        if (i - indx + 1 >= MINIMUM_PUFFS_IN_EPISODE):
            wrst = getSmokingWrist(only_puff_list, indx, i)
            smoking_episode_data.append(
                DataPoint(start_time=only_puff_list[indx].start_time, end_time=only_puff_list[i].start_time,
                          sample=(wrst * 100) + (i - indx + 1)))

            indx = i + 1
        else:
            indx = indx + 1
    return smoking_episode_data

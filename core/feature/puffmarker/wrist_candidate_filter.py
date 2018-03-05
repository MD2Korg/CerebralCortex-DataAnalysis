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


from typing import List

import numpy as np

from cerebralcortex.core.datatypes.datapoint import DataPoint
from core.feature.puffmarker.CONSTANT import *


def filter_with_duration(gyr_intersections: List[DataPoint]):
    gyr_intersections_filtered = []

    for I in gyr_intersections:
        dur = (I.end_time - I.start_time).total_seconds()
        if (dur >= 1.0) & (dur <= 5.0):
            gyr_intersections_filtered.append(I)

    return gyr_intersections_filtered


def filter_with_roll_pitch(gyr_intersections: List[DataPoint], roll_list: List[DataPoint], pitch_list: List[DataPoint]):
    gyr_intersections_filtered = []

    for I in gyr_intersections:
        start_index = I.sample[0]
        end_index = I.sample[1]

        roll_sub = [roll_list[i].sample for i in range(start_index, end_index)]
        pitch_sub = [pitch_list[i].sample for i in range(start_index, end_index)]

        mean_roll = np.mean(roll_sub)
        mean_pitch = np.mean(pitch_sub)

        if (mean_roll > MIN_ROLL) & (mean_roll <= MAX_ROLL) & (mean_pitch >= MIN_PITCH) & (mean_pitch <= MAX_PITCH):
            gyr_intersections_filtered.append(I)

    return gyr_intersections_filtered

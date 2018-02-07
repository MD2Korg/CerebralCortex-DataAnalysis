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


import numpy as np
from cerebralcortex.core.datatypes.datastream import DataStream

def filterDuration(gyr_intersections: DataStream):
    gyr_intersections_filtered = []

    for I in gyr_intersections.data:
        dur = (I.end_time - I.start_time).total_seconds()
        if (dur >= 1.0) & (dur <= 5.0):
            gyr_intersections_filtered.append(I)

    gyr_intersections_filtered_datastream = DataStream.from_datastream([gyr_intersections])
    gyr_intersections_filtered_datastream.data = gyr_intersections_filtered
    return gyr_intersections_filtered_datastream

def filterRollPitch(gyr_intersections_stream: DataStream, roll_stream: DataStream, pitch_stream: DataStream):
    gyr_intersections_filtered = []

    for I in gyr_intersections_stream.data:
        sIndex = I.sample[0]
        eIndex = I.sample[1]

        roll_sub = [roll_stream.data[i].sample for i in range(sIndex, eIndex)]
        pitch_sub = [pitch_stream.data[i].sample for i in range(sIndex, eIndex)]

        mean_roll = np.mean(roll_sub)
        mean_pitch = np.mean(pitch_sub)

        if (mean_roll > -20) & (mean_roll <= 65) & (mean_pitch >= - 125) & (mean_pitch <= - 40):
            gyr_intersections_filtered.append(I)

    gyr_intersections_filtered_stream = DataStream.from_datastream([gyr_intersections_stream])
    gyr_intersections_filtered_stream.data = gyr_intersections_filtered
    return gyr_intersections_filtered_stream

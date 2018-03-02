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


import math
import numpy as np
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream
from core.signalprocessing.vector import magnitude
from core.feature.puffmarker.util import segmentationUsingTwoMovingAverage, smooth
from core.feature.puffmarker.wrist_candidate_filter import filterDuration, filterRollPitch

def calculate_roll_pitch_yaw_tream(accel_stream: DataStream):
    roll_stream = calculate_roll_stream(accel_stream)
    pitch_stream = calculate_pitch_stream(accel_stream)
    yaw_stream = calculate_yaw_stream(accel_stream)

    return roll_stream, pitch_stream, yaw_stream

def calculate_roll_stream(accel_stream: DataStream) :
    roll = []
    for dp in accel_stream.data:
        ax = dp.sample[0]
        ay = dp.sample[1]
        az = dp.sample[2]
        rll = 180 * math.atan2(ax, math.sqrt(ay * ay + az * az)) / math.pi
        roll.append(DataPoint(start_time=dp.start_time, end_time=dp.end_time, offset=dp.offset, sample=rll))

    roll_datastream = DataStream.from_datastream([accel_stream])
    roll_datastream.data = roll
    return roll_datastream

def calculate_pitch_stream(accel_stream: DataStream):
    pitch = []
    for dp in accel_stream.data:
        ay = dp.sample[1]
        az = dp.sample[2]
        ptch = 180 * math.atan2(-ay, -az) / math.pi
        pitch.append(DataPoint(start_time=dp.start_time, end_time=dp.end_time, offset=dp.offset, sample=ptch))

    pitch_datastream = DataStream.from_datastream([accel_stream])
    pitch_datastream.data = pitch
    return pitch_datastream

def calculate_yaw_stream(accel_stream: DataStream):
    yaw = []
    for dp in accel_stream.data:
        ax = dp.sample[0]
        ay = dp.sample[1]
        yw = 180 * math.atan2(ay, ax) / math.pi
        yaw.append(DataPoint(start_time=dp.start_time, end_time=dp.end_time, offset=dp.offset, sample=yw))

    yaw_datastream = DataStream.from_datastream([accel_stream])
    yaw_datastream.data = yaw
    return yaw_datastream

def computeBasicFeatures(data):
    mean = np.mean(data)
    median = np.median(data)
    sd = np.std(data)
    quartile = np.percentile(data, 75) - np.percentile(data, 25)

    return mean, median, sd, quartile

def compute_candidate_Features(gyr_intersections_stream, gyr_mag_stream, roll_stream, pitch_stream, yaw_stream, wrist: str):
    all_features = []
    offset = gyr_mag_stream.data[0].offset

    for I in gyr_intersections_stream.data:
        sTime = I.start_time
        eTime = I.end_time
        sIndex = I.sample[0]
        eIndex = I.sample[1]

        roll_sub = [roll_stream.data[i].sample for i in range(sIndex, eIndex)]
        pitch_sub = [pitch_stream.data[i].sample for i in range(sIndex, eIndex)]
        yaw_sub = [yaw_stream.data[i].sample for i in range(sIndex, eIndex)]

        Gmag_sub = [gyr_mag_stream.data[i].sample for i in range(sIndex, eIndex)]

        duration = 1000 * (eTime - sTime).total_seconds()

        roll_mean, roll_median, roll_sd, roll_quartile = computeBasicFeatures(roll_sub)
        pitch_mean, pitch_median, pitch_sd, pitch_quartile = computeBasicFeatures(pitch_sub)
        yaw_mean, yaw_median, yaw_sd, yaw_quartile = computeBasicFeatures(yaw_sub)

        gyro_mean, gyro_median, gyro_sd, gyro_quartile = computeBasicFeatures(Gmag_sub)

        f = [duration, roll_mean, roll_median, roll_sd, roll_quartile, pitch_mean, pitch_median, pitch_sd,
             pitch_quartile, yaw_mean, yaw_median, yaw_sd, yaw_quartile, gyro_mean, gyro_median, gyro_sd,
             gyro_quartile]

        all_features.append(DataPoint(start_time=sTime, end_time=eTime, offset=offset, sample=f))

    return all_features


def compute_wrist_feature(accel_stream: DataStream, gyro_stream: DataStream, wrist: str, fast_size=13, slow_soze=131):

    gyr_mag_stream = magnitude(gyro_stream)

    roll_stream, pitch_stream, yaw_stream = calculate_roll_pitch_yaw_tream(accel_stream)

    gyr_mag_800 = smooth(gyr_mag_stream, fast_size)
    gyr_mag_8000 = smooth(gyr_mag_stream, slow_soze)

    gyr_intersections = segmentationUsingTwoMovingAverage(gyr_mag_8000, gyr_mag_800, 0, 4)

    gyr_intersections = filterDuration(gyr_intersections)
    gyr_intersections = filterRollPitch(gyr_intersections, roll_stream, pitch_stream)

    all_features = compute_candidate_Features(gyr_intersections, gyr_mag_stream, roll_stream, pitch_stream, yaw_stream, wrist)

    return all_features

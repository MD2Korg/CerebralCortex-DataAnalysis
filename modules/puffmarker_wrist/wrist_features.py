from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream
from modules.puffmarker_wrist.util import segmentationUsingTwoMovingAverage, smooth, magnitude
from modules.puffmarker_wrist.wrist_candidate_filter import filterDuration, filterRollPitch
import math
import numpy as np

def calculateRollPitchYawStream(accel_stream: DataStream):
    roll_stream = calculateRollStream(accel_stream)
    pitch_stream = calculatePitchStream(accel_stream)
    yaw_stream = calculateYawStream(accel_stream)

    return roll_stream, pitch_stream, yaw_stream

def calculateRollStream(accel_stream: DataStream) :
    roll = []
    for dp in accel_stream.data:
        ax = dp.sample[0]
        ay = dp.sample[1]
        az = dp.sample[2]
        rll = 180 * math.atan2(ax, math.sqrt(ay * ay + az * az)) / math.pi
        roll.append(DataPoint.from_tuple(start_time=dp.start_time, end_time=dp.end_time, sample=rll))

    roll_datastream = DataStream.from_datastream([accel_stream])
    roll_datastream.data = roll
    return roll_datastream

def calculatePitchStream(accel_stream: DataStream):
    pitch = []
    for dp in accel_stream.data:
        ax = dp.sample[0]
        ay = dp.sample[1]
        az = dp.sample[2]
        ptch = 180 * math.atan2(-ay, -az) / math.pi
        pitch.append(DataPoint.from_tuple(start_time=dp.start_time, end_time=dp.end_time, sample=ptch))

    pitch_datastream = DataStream.from_datastream([accel_stream])
    pitch_datastream.data = pitch
    return pitch_datastream

def calculateYawStream(accel_stream: DataStream):
    yaw = []
    for dp in accel_stream.data:
        ax = dp.sample[0]
        ay = dp.sample[1]
        az = dp.sample[2]
        yw = 180 * math.atan2(ay, ax) / math.pi
        yaw.append(DataPoint.from_tuple(start_time=dp.start_time, end_time=dp.end_time, sample=yw))

    yaw_datastream = DataStream.from_datastream([accel_stream])
    yaw_datastream.data = yaw
    return yaw_datastream

def computeBasicFeatures(data):

    mean = np.mean(data)
    median = np.median(data)
    sd = np.std(data)
    quartile = np.percentile(data, 75) - np.percentile(data, 25)

    return mean, median, sd, quartile

def compute_candidate_Features(gyr_intersections_stream, gyr_mag_stream, roll_stream, pitch_stream, yaw_stream, accel_stream):

    all_features = []

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

        f = [duration, roll_mean, roll_median, roll_sd, roll_quartile, pitch_mean, pitch_median, pitch_sd, pitch_quartile, yaw_mean, yaw_median, yaw_sd, yaw_quartile, gyro_mean, gyro_median, gyro_sd, gyro_quartile]

        all_features.append(DataPoint.from_tuple(start_time=sTime, end_time=eTime, sample=f))

    feature_vector_stream = DataStream.from_datastream([gyr_intersections_stream])
    feature_vector_stream.data = all_features
    return feature_vector_stream

def compute_wrist_feature(accel_stream: DataStream, gyro_stream: DataStream):

    fastSize = 13
    slowSize = 131

    gyr_mag_stream = magnitude(gyro_stream)

    roll_stream, pitch_stream, yaw_stream = calculateRollPitchYawStream(accel_stream)

    gyr_mag_800 = smooth(gyr_mag_stream, fastSize)
    gyr_mag_8000 = smooth(gyr_mag_stream, slowSize)

    gyr_intersections = segmentationUsingTwoMovingAverage(gyr_mag_8000, gyr_mag_800, 0, 4)

    gyr_intersections = filterDuration(gyr_intersections)
    gyr_intersections = filterRollPitch(gyr_intersections, roll_stream, pitch_stream)

    all_features = compute_candidate_Features(gyr_intersections, gyr_mag_stream, roll_stream, pitch_stream, yaw_stream, accel_stream)

    return all_features

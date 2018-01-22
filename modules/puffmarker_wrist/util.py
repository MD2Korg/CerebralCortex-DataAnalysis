from typing import List


import numpy as np
from numpy.linalg import norm

from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream

def magnitude(datastream: DataStream) -> DataStream:
    """

    :param datastream:
    :return: magnitude of the dataastream
    """
    result = DataStream.from_datastream(input_streams=[datastream])
    if datastream.data is None or len(datastream.data) == 0:
        result.data = []
        return result

    input_data = np.array([i.sample for i in datastream.data])

    data = norm(input_data, axis=1).tolist()

    result.data = [DataPoint.from_tuple(start_time=v.start_time, sample=data[i])
                   for i, v in enumerate(datastream.data)]

    return result



def smooth(datastream: DataStream,
           span: int = 5) -> DataStream:

    data = datastream.data
    """
    Smooths data using moving average filter over a span.
    The first few elements of data_smooth are given by
    data_smooth(1) = data(1)
    data_smooth(2) = (data(1) + data(2) + data(3))/3
    data_smooth(3) = (data(1) + data(2) + data(3) + data(4) + data(5))/5
    data_smooth(4) = (data(2) + data(3) + data(4) + data(5) + data(6))/5

    for more details follow the below links:
    https://www.mathworks.com/help/curvefit/smooth.html
    http://stackoverflow.com/a/40443565

    :return: data_smooth
    :param data:
    :param span:
    """

    if data is None or len(data) == 0:
        return []

    sample = [i.sample for i in data]
    sample_middle = np.convolve(sample, np.ones(span, dtype=int), 'valid') / span
    divisor = np.arange(1, span - 1, 2)
    sample_start = np.cumsum(sample[:span - 1])[::2] / divisor
    sample_end = (np.cumsum(sample[:-span:-1])[::2] / divisor)[::-1]
    sample_smooth = np.concatenate((sample_start, sample_middle, sample_end))

    data_smooth = []

    if len(sample_smooth) == len(data):
        for i, item in enumerate(data):
            dp = DataPoint.from_tuple(sample=sample_smooth[i], start_time=item.start_time, end_time=item.end_time)
            data_smooth.append(dp)
    else:
        raise Exception("Smoothed data length does not match with original data length.")

    data_smooth_stream = DataStream.from_datastream([datastream])
    data_smooth_stream.data = data_smooth
    return data_smooth_stream


def moving_average_curve(data: List[DataPoint],
                         window_length: int) -> List[DataPoint]:
    """
    Moving average curve from filtered (using moving average) samples.

    :return: mac
    :param data:
    :param window_length:
    """
    if data is None or len(data) == 0:
        return []

    sample = [i.sample for i in data]
    mac = []
    for i in range(window_length, len(sample) - (window_length + 1)):
        sample_avg = np.mean(sample[i - window_length:i + window_length + 1])
        mac.append(DataPoint.from_tuple(sample=sample_avg, start_time=data[i].start_time, end_time=data[i].end_time))

    return mac


def segmentationUsingTwoMovingAverage(slowMovingAverageDataStream: DataStream
                                      , fastMovingAverageDataStream: DataStream
                                      , THRESHOLD: float, near: int):

    slowMovingAverage = np.array([data.sample for data in slowMovingAverageDataStream.data])
    fastMovingAverage = np.array([data.sample for data in fastMovingAverageDataStream.data])

    indexList = [0]*len(slowMovingAverage)
    curIndex = 0

    for i in range(len(slowMovingAverage)):
        diff = slowMovingAverage[i] - fastMovingAverage[i]
        if diff > THRESHOLD:
            if curIndex == 0:
                indexList[curIndex] = i
                curIndex = curIndex + 1
                indexList[curIndex] = i
            else:
                if i <= indexList[curIndex] + near :
                    indexList[curIndex] = i
                else:
                    curIndex = curIndex + 1
                    indexList[curIndex] = i
                    curIndex = curIndex + 1
                    indexList[curIndex] = i

    output = []
    if curIndex > 0:
        for i in range(0, curIndex, 2):
            sIndex = indexList[i]
            eIndex = indexList[i+1]
            sTime = slowMovingAverageDataStream.data[sIndex].start_time
            eTime = slowMovingAverageDataStream.data[eIndex].start_time
            output.append(DataPoint(start_time=sTime, end_time=eTime, sample=[indexList[i], indexList[i + 1]]))

    intersectionPoints = DataStream.from_datastream([slowMovingAverageDataStream])
    intersectionPoints.data = output
    return intersectionPoints





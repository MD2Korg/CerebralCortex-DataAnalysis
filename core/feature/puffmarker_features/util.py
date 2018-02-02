import numpy as np
import core.signalprocessing.vector as vector
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream

def smooth(datastream: DataStream,
           span: int = 5) -> DataStream:

    data = datastream.data
    data_smooth = vector.smooth(data, span)

    data_smooth_stream = DataStream.from_datastream([datastream])
    data_smooth_stream.data = data_smooth
    return data_smooth_stream

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





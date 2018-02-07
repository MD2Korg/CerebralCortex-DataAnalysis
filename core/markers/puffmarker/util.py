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

import uuid
from datetime import timedelta
import numpy as np
from typing import List
import core.signalprocessing.vector as vector
from cerebralcortex.cerebralcortex import CerebralCortex
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

    for index, value in enumerate(slowMovingAverage):
        diff = slowMovingAverage[index] - fastMovingAverage[index]
        if diff > THRESHOLD:
            if curIndex == 0:
                indexList[curIndex] = index
                curIndex = curIndex + 1
                indexList[curIndex] = index
            else:
                if index <= indexList[curIndex] + near :
                    indexList[curIndex] = index
                else:
                    curIndex = curIndex + 1
                    indexList[curIndex] = index
                    curIndex = curIndex + 1
                    indexList[curIndex] = index

    output = []
    if curIndex > 0:
        for index in range(0, curIndex, 2):
            sIndex = indexList[index]
            eIndex = indexList[index+1]
            sTime = slowMovingAverageDataStream.data[sIndex].start_time
            eTime = slowMovingAverageDataStream.data[eIndex].start_time
            output.append(DataPoint(start_time=sTime, end_time=eTime, sample=[indexList[index], indexList[index + 1]]))

    intersectionPoints = DataStream.from_datastream([slowMovingAverageDataStream])
    intersectionPoints.data = output
    return intersectionPoints

def get_stream_days(stream_id: uuid, CC: CerebralCortex) -> List:
    """
    Returns a list of days (string format: YearMonthDay (e.g., 20171206)
    :param stream_id:
    """
    stream_dicts = CC.get_stream_duration(stream_id)
    stream_days = []
    days = stream_dicts["end_time"]-stream_dicts["start_time"]
    for day in range(days.days+1):
        stream_days.append((stream_dicts["start_time"]+timedelta(days=day)).strftime('%Y%m%d'))
    return stream_days




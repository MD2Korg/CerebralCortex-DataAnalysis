# Copyright (c) 2018, MD2K Center of Excellence
# -Mithun Saha <amimithun@gmail.com>
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

from cerebralcortex.cerebralcortex import CerebralCortex
from pprint import pprint
from scipy.io import savemat
from datetime import timedelta
from collections import OrderedDict
from cerebralcortex.core.util.data_types import DataPoint
from collections import OrderedDict
from typing import List

import math
import datetime
import pandas as pd
import pytz
import numpy as np

typing_stream_name = 'org.md2k.data_analysis.feature.typing.episodes.day'
office_stream_name = 'org.md2k.data_analysis.gps_episodes_and_semantic_location_from_model'
beacon_stream_name = 'org.md2k.data_analysis.feature.beacon.work_beacon_context'


def target_in_fraction_of_context(target_total_time: dict,
                                  context_with_time: dict,
                                  offset: int, context: str) -> List[DataPoint]:
    """
    This function calculates duration of typing - total and per hour(in minutes)
    in office and in office around office beacon.

    :param dict target_total_time: a dictionary of typing total time in a day
    :param dict context_with_time: a dictionary of office/beacon start and end
                                    times in a day
    :param int offset: offset for local time
    :param str context: office(work)/work beacon(1)
    :return: DataPoints denoting duration of typing - total and per hour(in minutes)
             in office and in office around office beacon.
    :rtype: List(DataPoint)

    """
    outputstream = []  # list of DataPoints for output

    total_context_time = timedelta(0)

    for context_slot in context_with_time[context]:
        total_context_time += context_slot[1] - context_slot[0]

    context_with_time[context].sort()
    context_start_time = context_with_time[context][0][0]
    context_end_time = \
        context_with_time[context][len(context_with_time[context]) - 1][1]

    for target in target_total_time:
        datapoint = DataPoint(context_start_time, context_end_time, offset,
                              [float(format(target_total_time[target].seconds/60,'.3f')),
                               float(format((target_total_time[target].seconds/60)/(total_context_time.seconds/60)*60,'.3f'))])

        outputstream.append(datapoint)

    return outputstream


def output_stream(targetconstruct_with_time: dict, context_with_time: dict,
                  offset: int) -> tuple:
    """
    This function compares time intervals of typing with time
    intervals of office or office beacon, to find overlapping time windows to
    extract time intervals, in which typing occurs in office/around
    office beacon.

    :param dict targetconstruct_with_time: a dictionary of typing time intervals
    :param dict context_with_time: a dictionary of office/beacon time intervals
    :param int offset: for local time information
    :return: total time spent for typing,
            DataPoints for output stream
    :rtype: tuple(dict,List)
    """

    target_total_time = {}  # total time for typing
    outputstream = []  # list of DataPoints for output

    if targetconstruct_with_time and context_with_time:
        for target in targetconstruct_with_time:
            # keeps running total time for typing
            time_diff = timedelta(0)
            if target == 1:
                for time_slot in targetconstruct_with_time[target]:
                    for context in context_with_time:
                        if context == 'work' or context == '1':
                            for context_slot in context_with_time[context]:
                                start_time = max(time_slot[0], context_slot[0])
                                end_time = min(time_slot[1], context_slot[1])
                                if end_time > start_time:
                                    datapoint = DataPoint(start_time, end_time,
                                                          offset, context)

                                    time_diff += end_time - start_time
                                    outputstream.append(datapoint)

            if target == 1:
                target_total_time[target] = time_diff

    return target_total_time, outputstream


def process_data(data: List[DataPoint]) -> dict:
    """
     This function takes a list of data points of a stream.
     For each DataPoint, based on sample value(1,work,'1')
     creates a dictionary of start and end time.

    :param List[DataPoint] data: list of typing,office,office beacon datapoints
    :return: dictionaries denoting start and end times for typing, office and office beacon in a day
    :rtype: dict
    """
    dicts = {}

    if len(data) == 0:
        return None

    for v in data:
        time = []
        if v.sample != None:
            time.append(v.start_time)
            time.append(v.end_time)

            if type(v.sample) == list:
                if v.sample[0] in dicts:
                    dicts[v.sample[0]].append(time)
                else:
                    dicts[v.sample[0]] = []
                    dicts[v.sample[0]].append(time)

            elif type(v.sample) == str:
                if v.sample in dicts:
                    dicts[v.sample].append(time)
                else:
                    dicts[v.sample] = []
                    dicts[v.sample].append(time)

            elif type(v.sample) == np.str_:
                if v.sample in dicts:
                    dicts[v.sample].append(time)
                else:
                    dicts[v.sample] = []
                    dicts[v.sample].append(time)

            elif type(v.sample) == int:
                v.sample = str(v.sample)
                if v.sample in dicts:
                    dicts[v.sample].append(time)
                else:
                    dicts[v.sample] = []
                    dicts[v.sample].append(time)

    return dicts

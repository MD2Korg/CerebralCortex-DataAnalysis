# Copyright (c) 2018, MD2K Center of Excellence
# -Mithun Saha <msaha1@memphis.edu>
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

posture_stream_name = 'org.md2k.data_analysis.feature.body_posture.wrist.accel_only.10_second'
activity_stream_name = 'org.md2k.data_analysis.feature.activity.wrist.accel_only.10_seconds'
office_stream_name = 'org.md2k.data_analysis.gps_episodes_and_semantic_location_from_model'
beacon_stream_name = 'org.md2k.data_analysis.feature.v6.beacon.work_beacon_context'


def target_in_fraction_of_context(target_total_time: dict,
                                  context_with_time: dict,
                                  offset: int, context: str) -> List[DataPoint]:
    """
    This function calculates total context time(office, around office beacon) in
    a day and finds fraction of times spent in sitting,standing and walking in minutes
    per hour in office and around office beacon.

    :param dict target_total_time: a dictionary of posture/activity total time in a day
    :param dict context_with_time: a dictionary of office/beacon start and end times
                                    in a day
    :param int offset: offset for local time
    :param str context: office(work)/work beacon(1)
    :return: DataPoints denoting time spent in minutes per hour for standing,
            sitting,walking in office and around office beacon context
    :rtype: List(DataPoint)

    """
    outputstream = []  # list of DataPoints for output

    total_context_time = timedelta(0)

    # context_slot[0] = start time
    # context_slot[1] = end time
    for context_slot in context_with_time[context]:
        total_context_time += context_slot[1] - context_slot[0]

    context_with_time[context].sort()
    context_start_time = context_with_time[context][0][0]
    context_end_time = \
        context_with_time[context][len(context_with_time[context]) - 1][1]

    for target in target_total_time:
        # datapoint = DataPoint(context_start_time, context_end_time, offset,
        #                       [str(target),
        #                        float(format(target_total_time[target] / total_context_time * 60, '.3f'))])
        datapoint = DataPoint(context_start_time, context_end_time, offset,
                              [str(target),float(format(target_total_time[target].seconds/60,'.3f')),
                               float(format((target_total_time[target].seconds/60)/(total_context_time.seconds/60)*60,'.3f'))])

        outputstream.append(datapoint)

    return outputstream


def output_stream(targetconstruct_with_time: dict, context_with_time: dict,
                  offset: int) -> tuple:
    """
    This function compares time intervals of posture or activity with time
    intervals of office or beacon, to find overlapping time windows to
    extract time intervals, in which posture/activity occurs in office/around
    work beacon.

    :param dict targetconstruct_with_time: a dictionary of posture/activity time intervals
    :param dict context_with_time: a dictionary of office/beacon time intervals
    :param int offset: for local time information
    :return: total time spent for posture/activity,
            DataPoints for output stream
    :rtype: tuple(dict,List)
    """

    target_total_time = {}  # total time for posture/activity
    outputstream = []  # list of DataPoints for output

    if targetconstruct_with_time and context_with_time:
        for target in targetconstruct_with_time:
            # keeps running total time for posture/activity
            time_diff = timedelta(0)
            if target == 'sitting' or target == 'standing' or target == 'WALKING':
                for time_slot in targetconstruct_with_time[target]:
                    for context in context_with_time:
                        if context == 'work' or context == '1':
                            for context_slot in context_with_time[context]:
                                start_time = max(time_slot[0], context_slot[0])
                                end_time = min(time_slot[1], context_slot[1])
                                if end_time > start_time:
                                    datapoint = DataPoint(start_time, end_time,
                                                          offset, [target, context])

                                    time_diff += end_time - start_time
                                    outputstream.append(datapoint)

            if target == 'sitting' or target == 'standing' or target == 'WALKING':
                target_total_time[target] = time_diff

    return target_total_time, outputstream


def process_data(data: List[DataPoint]) -> dict:
    """
     This function takes a list of data points of a stream.
     For each DataPoint, based on sample value(sitting,standing,walking,work,1)
     creates a dictionary of start and end time.

    :param List[DataPoint] data: list of posture,activity,gps,beacon datapoints
    :return: dictionaries denoting start and end times for standing,
            sitting,walking, office and office beacon in a day
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

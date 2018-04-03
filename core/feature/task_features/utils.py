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
beacon_stream_name = 'org.md2k.data_analysis.feature.v4.beacon.work_beacon_context'


def target_in_fraction_of_context(target_total_time,
                                  context_with_time,
                                  offset, context):
    """
    This function total time of posture, activity with total time of
    office and beacon to find fraction of time spent in posture,
    activity per hour.
    :param target_total_time: a dictionary of posture/activity total time
    :param context_with_time: a dictionary of office/beacon intervals
    :param context_type: office/beacon
    :return: fraction of total time spent in posture/activity
             in office/around beacon

    """
    outputstream = []  # list of datapoints for output

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
        datapoint = DataPoint(context_start_time, context_end_time, offset,
                              [total_context_time,
                               target_total_time[target],
                               target_total_time[
                                   target] / total_context_time * 60])

        outputstream.append(datapoint)

    return outputstream

def output_stream(targetconstruct_with_time, context_with_time,
                  offset):
    """
    This function compares time intervals of posture or activity with time
    intervals of office or beacon, to find overlapping time windows to
    extract time intervals, in which posture/activity occurs in office/around
    work beacon.
    :param targetconstruct_with_time: a dictionary of posture/activity time intervals
    :param context_with_time: a dictionary of office/beacon time intervals
    :param offset: offset for time information
    :return: a dictionray of total time spent for posture/activity,
            a list of datapoints for output stream
    """

    target_total_time = {} #total time for posture/activity
    outputstream=[] #list of datapoints for output

    if targetconstruct_with_time and context_with_time:
        for target in targetconstruct_with_time:
            #keeps running total time for posture/activity
            time_diff = timedelta(0)
            if target == 'sitting' or target == 'standing' or target == 'WALKING':
                for time_slot in targetconstruct_with_time[target]:
                    for context in context_with_time:
                        if context == 'Work' or context == '1':
                            for context_slot in context_with_time[context]:
                                start_time = max(time_slot[0],context_slot[0])
                                end_time = min(time_slot[1],context_slot[1])
                                if end_time > start_time :
                                    datapoint = DataPoint(start_time, end_time,
                                                          offset, [target, context])

                                    time_diff += end_time-start_time
                                    outputstream.append(datapoint)

            if target == 'sitting'or target == 'standing'or target == 'WALKING':
                target_total_time[target] = time_diff

    return target_total_time,outputstream

def process_data(data: List[DataPoint]):
    """
     This function takes a list of data points of a stream.
     For each datapoint, based on sample value, creats a
     dictionary of start and end time.
    :param user_id:list of datapoints
    :return: a dictionary of start time and end time of sample value
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

    return dicts

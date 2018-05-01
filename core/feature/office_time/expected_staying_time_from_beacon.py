# Copyright (c) 2018, MD2K Center of Excellence
# - Alina Zaman <azaman@memphis.edu>
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

from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint
from datetime import datetime, timedelta
from core.computefeature import ComputeFeatureBase

from typing import List
import pprint as pp
import numpy as np
import pdb
import pickle
import uuid
import json
import traceback
import math

# TODO: Define constants
feature_class_name = 'ExpectedStayingTimesFromBeacon'
Working_Days_STREAM = "org.md2k.data_analysis.feature.working_days_from_beacon"
MEDIAN_ABSOLUTE_DEVIATION_MULTIPLIER = 1.4826
OUTLIER_DETECTION_MULTIPLIER = 3


class ExpectedStayingTimesFromBeacon(ComputeFeatureBase):
    """
     Produce feature from the days when a participant was around office beacon from stream
    "org.md2k.data_analysis.feature.working_days_from_beacon". The time extent between the first
    arrival time in beacon range and last time of leaving is taken as staying time. Usual staying
    time is calculated from these data. For example, if anyone's office staying time is the 7 hour
    30 min in usual then expected conservative staying time is 8 hour and expected liberal staying
    time is 7 hour.Each day's staying_time is marked as in_expected_conservative_time or
    more_than_expected_conservative_time or less_than_expected_conservative_time in one stream.
    And in another stream each day's staying_time is marked as in_expected_liberal_time or
    more_than_expected_liberal_time or less_than_expected_liberal_time
    """

    def listing_all_expected_staying_times_from_beacon(self, user_id: str, all_days: List[str]):
        """
        Produce and save the list of work_day's staying_time at office from
        "org.md2k.data_analysis.feature.working_days_from_beacon" stream and marked each day's
        staying_time as in_expected_conservative_time or more_than_expected_conservative_time
        or less_than_expected_conservative_time in one stream and in another stream each day's
        staying_time is marked as in_expected_liberal_time or more_than_expected_liberal_time
        or less_than_expected_liberal_time

        :param str user_id: UUID of the stream owner
        :param List(str) all_days: All days of the user in the format 'YYYYMMDD'
        :return:
        """

        self.CC.logging.log('%s started processing for user_id %s' %
                            (self.__class__.__name__, str(user_id)))

        stream_ids = self.CC.get_stream_id(user_id,
                                           Working_Days_STREAM)
        expected_conservative_staying_data = []
        expected_liberal_staying_data = []
        office_staying_times = list()
        for stream_id in stream_ids:
            for day in all_days:
                work_data_stream = \
                    self.CC.get_stream(stream_id["identifier"], user_id, day, localtime=True)

                for data in work_data_stream.data:
                    arrival_time = data.start_time.hour * 60 + data.start_time.minute
                    leave_time = data.end_time.hour * 60 + data.end_time.minute
                    staying_time = leave_time - arrival_time
                    office_staying_times.append(staying_time)
                    sample = []
                    temp = DataPoint(data.start_time, data.end_time, data.offset, sample)
                    temp.sample.append(staying_time)
                    expected_conservative_staying_data.append(temp)
        if not len(office_staying_times):
            return
        median = np.median(office_staying_times)
        mad_office_staying_times = []
        for staying_time in office_staying_times:
            # mad = median absolute deviation
            mad_office_staying_times.append(abs(staying_time - median))
        median2 = np.median(mad_office_staying_times)
        mad_value = median2 * MEDIAN_ABSOLUTE_DEVIATION_MULTIPLIER
        outlier_border = mad_value * OUTLIER_DETECTION_MULTIPLIER
        outlier_removed_office_staying_times = []
        for staying_time in office_staying_times:
            if (median - outlier_border) < staying_time < (median + outlier_border):
                outlier_removed_office_staying_times.append(staying_time)
        if not len(outlier_removed_office_staying_times):
            outlier_removed_office_staying_times = office_staying_times
        actual_staying_time = np.mean(outlier_removed_office_staying_times)
        actual_minute = int(actual_staying_time % 60)
        actual_hour = int(actual_staying_time / 60)
        conservative_hour = actual_hour
        liberal_hour = actual_hour
        if actual_minute < 30:
            conservative_minute = 30
            liberal_minute = 0
        else:
            conservative_minute = 0
            liberal_minute = 30
            conservative_hour += 1
        conservative_time = conservative_hour * 60 + conservative_minute
        liberal_time = liberal_hour * 60 + liberal_minute
        for data in expected_conservative_staying_data:
            sample = []
            temp = DataPoint(data.start_time, data.end_time, data.offset, sample)
            staying_time = data.sample[0]
            if staying_time > conservative_time:
                data.sample.append("more_than_expected_conservative_time")
                data.sample.append(math.ceil(staying_time - conservative_time))
                data.sample.append(1)
            elif staying_time < conservative_time:
                data.sample.append("less_than_expected_conservative_time")
                data.sample.append(math.ceil(conservative_time - staying_time))
                temp.sample.append(0)
            elif staying_time == conservative_time:
                data.sample.append("in_expected_conservative_time")
                data.sample.append(0)
                data.sample.append(1)
            temp.sample.append(staying_time)
            if staying_time > liberal_time:
                temp.sample.append("more_than_expected_liberal_time")
                temp.sample.append(math.ceil(staying_time - liberal_time))
                data.sample.append(1)
            elif staying_time < liberal_time:
                temp.sample.append("less_than_expected_liberal_time")
                temp.sample.append(math.ceil(liberal_time - staying_time))
                temp.sample.append(0)
            elif staying_time == liberal_time:
                temp.sample.append("in_expected_liberal_time")
                temp.sample.append(0)
                data.sample.append(1)
            expected_liberal_staying_data.append(temp)
        try:
            if len(expected_conservative_staying_data):
                streams = self.CC.get_user_streams(user_id)
                for stream_name, stream_metadata in streams.items():
                    if stream_name == Working_Days_STREAM:
                        self.store_stream(filepath="expected_conservative_staying_time_from_beacon.json",
                                          input_streams=[stream_metadata],
                                          user_id=user_id,
                                          data=expected_conservative_staying_data, localtime=True)
                        break
        except Exception as e:
            print("Exception:", str(e))
            print(traceback.format_exc())
        self.CC.logging.log('%s finished processing for user_id %s saved %d '
                            'data points' %
                            (self.__class__.__name__, str(user_id),
                             len(expected_conservative_staying_data)))
        try:
            if len(expected_liberal_staying_data):
                streams = self.CC.get_user_streams(user_id)
                for stream_name, stream_metadata in streams.items():
                    if stream_name == Working_Days_STREAM:
                        self.store_stream(filepath="expected_liberal_staying_time_from_beacon.json",
                                          input_streams=[stream_metadata],
                                          user_id=user_id,
                                          data=expected_liberal_staying_data, localtime=True)
                        break
        except Exception as e:
            print("Exception:", str(e))
            print(traceback.format_exc())
        self.CC.logging.log('%s finished processing for user_id %s saved %d '
                            'data points' %
                            (self.__class__.__name__, str(user_id),
                             len(expected_liberal_staying_data)))

    def process(self, user_id: str, all_days: List[str]):
        """
        Main processing function inherited from ComputerFeatureBase

        :param str user_id: UUID of the user
        :param List(str) all_days: List of days with format 'YYYYMMDD'
        :return:
        """
        if self.CC is not None:
            self.CC.logging.log("Processing Expected Staying Times From Beacon")
            self.listing_all_expected_staying_times_from_beacon(user_id, all_days)

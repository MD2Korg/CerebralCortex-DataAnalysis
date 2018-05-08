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
feature_class_name = 'ArrivalTimesFromBeacon'
Working_Days_STREAM = "org.md2k.data_analysis.feature.working_days_from_beacon"
MEDIAN_ABSOLUTE_DEVIATION_MULTIPLIER = 1.4826
OUTLIER_DETECTION_MULTIPLIER = 3


class ArrivalTimesFromBeacon(ComputeFeatureBase):
    """
    Produce feature from the days when a participant was in office from stream
    "org.md2k.data_analysis.feature.working_days_from_beacon". For office arrival time
    the first time of entering in his office's beacon range according to beacon data
    location is considered and only the hour and minute are taken for calculation. Usual
    arrival time is calculated from these data. And here usual time is a range of time.
    Each day's arrival_time is marked as usual or before_time or after_time
    """

    def listing_all_arrival_times_from_beacon(self, user_id: str, all_days: List[str]):
        """
        Produce and save the list of work_day's arrival_time at office's beacon
        from "org.md2k.data_analysis.feature.working_days_from_beacon" stream and
        marked each day's arrival_time as usual or before_time or after_time

        :param str user_id: UUID of the stream owner
        :param List(str) all_days: All days of the user in the format 'YYYYMMDD'
        """

        self.CC.logging.log('%s started processing for user_id %s' %
                            (self.__class__.__name__, str(user_id)))

        stream_ids = self.CC.get_stream_id(user_id,
                                           Working_Days_STREAM)
        arrival_data = []
        office_arrival_times = list()
        for stream_id in stream_ids:
            for day in all_days:
                work_data_stream = \
                    self.CC.get_stream(stream_id["identifier"], user_id, day, localtime=True)

                for data in work_data_stream.data:
                    print(data)
                    arrival_time = data.start_time.hour * 60 + data.start_time.minute
                    office_arrival_times.append(arrival_time)
                    sample = []
                    temp = DataPoint(data.start_time, data.end_time, data.offset, sample)
                    arrival_data.append(temp)
        if not len(office_arrival_times):
            return
        median = np.median(office_arrival_times)
        mad_arrival_times = []
        for arrival_time in office_arrival_times:
            # mad = median absolute deviation
            mad_arrival_times.append(abs(arrival_time - median))
        median2 = np.median(mad_arrival_times)
        mad_value = median2 * MEDIAN_ABSOLUTE_DEVIATION_MULTIPLIER
        outlier_border = mad_value * OUTLIER_DETECTION_MULTIPLIER
        outlier_removed_office_arrival_times = []
        for arrival_time in office_arrival_times:
            if (median - outlier_border) < arrival_time < (median + outlier_border):
                outlier_removed_office_arrival_times.append(arrival_time)
        if not len(outlier_removed_office_arrival_times):
            outlier_removed_office_arrival_times = office_arrival_times
        mean = np.mean(outlier_removed_office_arrival_times)
        standard_deviation = np.std(outlier_removed_office_arrival_times)
        for data in arrival_data:
            arrival_time = data.start_time.hour * 60 + data.start_time.minute
            data.sample.append(data.start_time.time())
            if arrival_time > mean + standard_deviation:
                data.sample.append("after_usual_time")
                data.sample.append(math.ceil(arrival_time - (mean + standard_deviation)))
                data.sample.append(0)
            elif arrival_time < mean - standard_deviation:
                data.sample.append("before_usual_time")
                data.sample.append(math.ceil(mean - standard_deviation - arrival_time))
                data.sample.append(1)
            else:
                data.sample.append("usual_time")
                data.sample.append(0)
                data.sample.append(1)
        try:
            if len(arrival_data):
                streams = self.CC.get_user_streams(user_id)
                for stream_name, stream_metadata in streams.items():
                    if stream_name == Working_Days_STREAM:
                        print("Going to pickle the file: ", arrival_data)

                        self.store_stream(filepath="arrival_time_from_beacon.json",
                                          input_streams=[stream_metadata],
                                          user_id=user_id,
                                          data=arrival_data, localtime=True)
                        break
        except Exception as e:
            print("Exception:", str(e))
            print(traceback.format_exc())
        self.CC.logging.log('%s finished processing for user_id %s saved %d '
                            'data points' %
                            (self.__class__.__name__, str(user_id),
                             len(arrival_data)))

    def process(self, user_id: str, all_days: List[str]):
        """
        Main processing function inherited from ComputerFeatureBase

        :param str user_id: UUID of the user
        :param List(str) all_days: List of days with format 'YYYYMMDD'
        :return:
        """
        if self.CC is not None:
            self.CC.logging.log("Processing Arrival Times From Beacon")
            self.listing_all_arrival_times_from_beacon(user_id, all_days)

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

# TODO: Comment and describe constants
feature_class_name = 'SleepDurationAnalysis'
Sleep_Durations_STREAM = 'org.md2k.data_analysis.feature.v2.sleep_durations'
MEDIAN_ABSOLUTE_DEVIATION_MULTIPLIER = 1.4826
OUTLIER_DETECTION_MULTIPLIER = 3


class SleepDurationAnalysis(ComputeFeatureBase):
    """
    Produce feature from these stream: 'org.md2k.data_analysis.feature.v2.sleep_durations. Sleep
    duration time is taken from the stream's data sample. And here usual sleep duration is a range
    of time. each day's sleep_duration is marked as usual_sleep_duration or more_than_usual or
    less_than_usual
    """

    def listing_all_sleep_duration_analysis(self, user_id: str, all_days: List[str]):
        """
        Produce and save the list of sleep duration acoording to day in one stream and marked
        each day's staying_time as Usual_sleep_duration or More_than_usual or Less_than_usual.
        Sleep duration is saved in hour. For each day's sleep duration the deviation from usual
        sleep duration is saved. All measure are in hour

        :param str user_id: UUID of the stream owner
        :param List(str) all_days: All days of the user in the format 'YYYYMMDD'
        :return:
        """
        self.CC.logging.log('%s started processing for user_id %s' %
                            (self.__class__.__name__, str(user_id)))

        stream_ids = self.CC.get_stream_id(user_id,
                                           Sleep_Durations_STREAM)
        sleep_duration_data = []
        sleep_durations = list()
        for stream_id in stream_ids:
            for day in all_days:
                sleep_duration_stream = \
                    self.CC.get_stream(stream_id["identifier"], user_id, day)

                for data in sleep_duration_stream.data:
                    sleep_duration = data.sample
                    sleep_durations.append(sleep_duration)
                    sample = []
                    sample.append(sleep_duration)
                    temp = DataPoint(data.start_time, data.end_time, data.offset, sample)
                    sleep_duration_data.append(temp)
        if not len(sleep_durations):
            return
        median = np.median(sleep_durations)
        mad_sleep_durations = []
        for sleep_duration in sleep_durations:
            # mad = median absolute deviation
            mad_sleep_durations.append(abs(sleep_duration - median))
        median2 = np.median(mad_sleep_durations)
        mad_value = median2 * MEDIAN_ABSOLUTE_DEVIATION_MULTIPLIER
        outlier_border = mad_value * OUTLIER_DETECTION_MULTIPLIER
        outlier_removed_sleep_durations = []
        for sleep_duration in sleep_durations:
            if sleep_duration > (median - outlier_border) and sleep_duration < (median + outlier_border):
                outlier_removed_sleep_durations.append(sleep_duration)

        if not len(outlier_removed_sleep_durations):
            outlier_removed_sleep_durations = sleep_durations
        mean = np.mean(outlier_removed_sleep_durations)
        standard_deviation = np.std(outlier_removed_sleep_durations)
        for data in sleep_duration_data:
            sleep_duration = data.sample[0]
            if sleep_duration > mean + standard_deviation:
                data.sample.append("more_than_usual")
                data.sample.append(sleep_duration - (mean + standard_deviation))
                data.sample.append(1)
            elif sleep_duration < mean-standard_deviation:
                data.sample.append("less_than_usual")
                data.sample.append(mean-standard_deviation - sleep_duration)
                data.sample.append(0)
            else:
                data.sample.append("usual_sleep_duration")
                data.sample.append(0)
                data.sample.append(1)

        try:
            if len(sleep_duration_data)>0:
                streams = self.CC.get_user_streams(user_id)
                if streams:
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == Sleep_Durations_STREAM:

                            self.store_stream(filepath="sleep_duration_analysis.json",
                                              input_streams=[stream_metadata],
                                              user_id=user_id,
                                              data=sleep_duration_data)
                            break
        except Exception as e:
            print("Exception:", str(e))
            print(traceback.format_exc())
        self.CC.logging.log('%s finished processing for user_id %s saved %d '
                            'data points' %
                            (self.__class__.__name__, str(user_id),
                             len(sleep_duration_data)))

    def process(self, user_id: str, all_days: List[str]):
        """
        Main processing function inherited from ComputerFeatureBase

        :param str user_id: UUID of the user
        :param List(str) all_days: List of days with format 'YYYYMMDD'
        :return:
        """
        if self.CC is not None:
            self.CC.logging.log("Processing Sleep Duration Analysis")
            self.listing_all_sleep_duration_analysis(user_id, all_days)

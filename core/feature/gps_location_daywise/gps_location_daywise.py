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
from datetime import datetime, timedelta, time
from core.computefeature import ComputeFeatureBase

from typing import List
import pprint as pp
import numpy as np
import pdb
import pickle
import uuid
import json
import traceback

feature_class_name = 'GpsLocationDaywise'
GPS_EPISODES_AND_SEMANTIC_lOCATION_STREAM = "org.md2k.data_analysis.gps_episodes_and_semantic_location_from_model"


class GpsLocationDaywise(ComputeFeatureBase):
    """
    Produce feature from gps location from
    "org.md2k.data_analysis.gps_episodes_and_semantic_location" data stream. One data
    point is split into two when it starts from one day and ends in other day. In that way,
    we are getting semantic location of daily data
    """

    def listing_all_gps_location_daywise(self, user_id: str, all_days: List[str]):
        """
        Produce and save the gps location of participant's in day basis

        :param str user_id: UUID of the stream owner
        :param List(str) all_days: All days of the user in the format 'YYYYMMDD'
        """

        self.CC.logging.log('%s started processing for user_id %s' %
                            (self.__class__.__name__, str(user_id)))
        gps_data = []
        stream_ids = self.CC.get_stream_id(user_id,
                                           GPS_EPISODES_AND_SEMANTIC_lOCATION_STREAM)
        for stream_id in stream_ids:

            for day in all_days:
                location_data_stream = \
                    self.CC.get_stream(stream_id["identifier"], user_id, day, localtime=False)

                for data in set(location_data_stream.data):

                    if data.start_time.date() != data.end_time.date():
                        temp = DataPoint(data.start_time, data.end_time, data.offset, data.sample)
                        start_day = data.start_time.date()
                        end_time = datetime.combine(start_day, time.max)
                        end_time = end_time.replace(tzinfo=data.start_time.tzinfo)
                        temp.end_time = end_time
                        gps_data.append(temp)

                        end_day = data.end_time.date()
                        start_day += timedelta(days=1)
                        while start_day != end_day:
                            temp = DataPoint(data.start_time, data.end_time, data.offset, data.sample)
                            start_time = datetime.combine(start_day, time.min)
                            start_time = start_time.replace(tzinfo=data.start_time.tzinfo)
                            temp.start_time = start_time
                            end_time = datetime.combine(start_day, time.max)
                            end_time = end_time.replace(tzinfo=data.start_time.tzinfo)
                            temp.end_time = end_time
                            gps_data.append(temp)
                            start_day += timedelta(days=1)
                        temp = DataPoint(data.start_time, data.end_time, data.offset, data.sample)
                        start_time = datetime.combine(start_day, time.min)
                        start_time = start_time.replace(tzinfo=data.start_time.tzinfo)
                        temp.start_time = start_time
                        gps_data.append(temp)
                    else:
                        gps_data.append(data)

        try:
            if len(gps_data):
                streams = self.CC.get_user_streams(user_id)
                for stream_name, stream_metadata in streams.items():
                    if stream_name == GPS_EPISODES_AND_SEMANTIC_lOCATION_STREAM:
                        self.store_stream(filepath="gps_location_daywise.json",
                                          input_streams=[stream_metadata],
                                          user_id=user_id,
                                          data=gps_data)
                        break
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(traceback.format_exc())

        self.CC.logging.log('%s finished processing for user_id %s saved %d '
                            'data points' %
                            (self.__class__.__name__, str(user_id),
                             len(gps_data)))

    def process(self, user_id: str, all_days: List[str]):
        """This is the main entry point for feature computation and is called by the main driver application

        Args:
            user_id: User identifier in UUID format
            all_days: List of all days to run this feature over

        """
        if self.CC is not None:
            self.CC.logging.log("Processing Working Days")
            self.listing_all_gps_location_daywise(user_id, all_days)

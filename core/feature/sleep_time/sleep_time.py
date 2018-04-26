# Copyright (c) 2018, MD2K Center of Excellence
# - Alina Zaman <azaman@memphis.edu>
# - Md Shiplu Hawlader <shiplu.cse.du@gmail.com; mhwlader@memphis.edu>
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
from core.feature.sleep_time.SleepDurationPrediction import SleepDurationPredictor
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
feature_class_name = 'SleepTime'
ACTIVITY_STREAM = 'ACTIVITY_TYPE--org.md2k.phonesensor--PHONE'
LIGHT_STREAM = 'AMBIENT_LIGHT--org.md2k.phonesensor--PHONE'
PHONE_SCREEN_STREAM = 'CU_IS_SCREEN_ON--edu.dartmouth.eureka'
AUDIO_ENERGY_STREAM = 'CU_AUDIO_ENERGY--edu.dartmouth.eureka'


class SleepTime(ComputeFeatureBase):
    """
    Produce feature from these four streams:
    1. CU_IS_SCREEN_ON--edu.dartmouth.eureka
    2. ACTIVITY_TYPE--org.md2k.phonesensor--PHONE
    3. AMBIENT_LIGHT--org.md2k.phonesensor--PHONE
    4. CU_AUDIO_ENERGY--edu.dartmouth.eureka

    Sleep time is calculated from these stream's data.
    """

    def listing_all_sleep_times(self, user_id: str, all_days: List[str]):
        """
        Produce and save the list of sleep time intervals according to day in one stream

        :param str user_id: UUID of the stream owner
        :param List(str) all_days: All days of the user in the format 'YYYYMMDD'
        """
        try:
            input_stream_names = [ACTIVITY_STREAM, LIGHT_STREAM, PHONE_SCREEN_STREAM, AUDIO_ENERGY_STREAM]
            input_streams = []

            streams = self.CC.get_user_streams(user_id)
            if streams:
                for stream_name, stream_metadata in streams.items():
                    if stream_name in input_stream_names:
                        input_streams.append(stream_metadata)
            if len(input_streams) != len(input_stream_names):
                return
            sleep_predictor = SleepDurationPredictor(self.CC)
            for day in all_days:
                sleep_date = datetime.strptime(day, "%Y%m%d")
                sleep_duration = sleep_predictor.get_sleep_time(user_id, sleep_date)

                if sleep_duration:
                    self.store_stream(filepath="sleep_time.json",
                                      input_streams=input_streams,
                                      user_id=user_id,
                                      data=[sleep_duration], localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

    def process(self, user_id: str, all_days: List[str]):
        """This is the main entry point for feature computation and is called by the main driver application

        Args:
            user_id: User identifier in UUID format
            all_days: List of all days to run this feature over

        """
        if self.CC is not None:
            self.CC.logging.log("Processing Sleep Times")
            self.listing_all_sleep_times(user_id, all_days)

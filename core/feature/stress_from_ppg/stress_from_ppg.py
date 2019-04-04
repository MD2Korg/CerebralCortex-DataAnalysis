# Copyright (c) 2018, MD2K Center of Excellence
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
import datetime
import traceback
from core.computefeature import ComputeFeatureBase

from typing import List, Callable, Any, Tuple
import numpy as np


# from ppg_to_stress import get_stress_time_series
# from decode import Preprc
from core.feature.stress_from_ppg.filtering import get_realigned_data
from core.feature.stress_from_ppg.ppg_to_stress import get_stress_time_series
import core.feature.stress_from_ppg.utils as utils

feature_class_name = 'StressFromPPG'

class StressFromPPG(ComputeFeatureBase):

    def process_day_data(self, user_id: str, day: str, streams: dict):

        raw_led_hrvp_lw = "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV_PLUS--LEFT_WRIST"
        raw_led_hrvp_rw = "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV_PLUS--RIGHT_WRIST"
        raw_led_hrv_lw = "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
        raw_led_hrv_rw = "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
        raw_hrv_lw = "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
        raw_hrv_wr = "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
        raw_stream_names = [raw_led_hrvp_lw, raw_led_hrvp_rw, raw_led_hrv_lw, raw_led_hrv_rw, raw_hrv_lw, raw_hrv_wr]
        ppg_data = None
        input_streams = []
        for rs in raw_stream_names:
            if rs not in streams:
                continue
            data = utils.get_raw_data_by_stream_name(rs, user_id, day, self.CC, localtime=False)
            data = get_realigned_data(data)
            input_streams.append(streams[rs])
            if ppg_data is None:
                ppg_data = data
            else:
                ppg_data = np.concatenate(ppg_data, data)

        if ppg_data is None:
            return

        try:
            ppg_data = sorted(ppg_data)
            offset = ppg_data[0][1]
            stress_data = get_stress_time_series(ppg_data)
            data = []
            for d in stress_data:
                data.append(DataPoint(start_time=datetime.datetime.fromtimestamp(d[0]/1000), offset=offset, sample=[d[1]]))
            self.store_stream(filepath="stress-from-wrist.json",
                              input_streams=input_streams, user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))


    def process_data(self, user_id: str, all_user_streams: dict, all_days: List[str]):

        streams = all_user_streams

        if not streams or not len(streams):
            self.CC.logging.log('No streams found for user %s for feature %s'
                                % (str(user_id), self.__class__.__name__))
            return

        for days in all_days:
            self.process_day_data(user_id, days, streams)



    def process(self, user_id: str, all_days: List[str]):
        """
        Main processing function inherited from ComputerFeatureBase

        :param str user_id: UUID of the user
        :param List(str) all_days: List of days with format 'YYYYMMDD'
        :return:
        """
        if self.CC is not None:
            self.CC.logging.log("Processing PPG data to detect stress")
            streams = self.CC.get_user_streams(user_id)
            self.process_data(user_id, streams, all_days)
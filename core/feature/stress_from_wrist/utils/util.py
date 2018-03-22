# Copyright (c) 2018, MD2K Center of Excellence
# All rights reserved.
# author: Md Azim Ullah
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
import uuid
from typing import List
from datetime import timedelta
import pickle
import core.computefeature

Fs = 25
led_decode_left_wrist = "org.md2k.signalprocessing.decodedLED.leftwrist"
led_decode_right_wrist = "org.md2k.signalprocessing.decodedLED.rightwrist"
window_size = 60
window_offset = 60
acceptable = .5
label_lab = 'LABEL--org.md2k.studymperflab'
motionsense_hrv_left_raw = \
    "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"

led_decode_left_wrist1 = "org.md2k.feature.decodedLED.leftwrist"
led_decode_right_wrist1 = "org.md2k.feature.decodedLED.rightwrist"
motionsense_hrv_right_raw = \
    "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
path_to_resources = 'core/resources/stress_files/'

def get_stream_days(stream_id: uuid, CC: CerebralCortex) -> List:
    """
    Returns a list of days (string format: YearMonthDay (e.g., 20171206)
    :param stream_id:
    """
    stream_dicts = CC.get_stream_duration(stream_id)
    stream_days = []
    days = stream_dicts["end_time"]-stream_dicts["start_time"]
    for day in range(days.days+1):
        stream_days.append(
            (stream_dicts["start_time"]+timedelta(days=day)).strftime('%Y%m%d'))
    return stream_days



def get_constants():
    int_RR_dist_obj = pickle.load(
        open(path_to_resources+'int_RR_dist_obj.p','rb'))
    H = pickle.load(
        open(path_to_resources+'H.p','rb'))
    w_l = pickle.load(
        open(path_to_resources+'w_l.p','rb'))
    w_r = pickle.load(
        open(path_to_resources+'w_r.p','rb'))
    fil_type = 'ppg'
    return int_RR_dist_obj,H,w_l,w_r,fil_type

# Copyright (c) 2018, MD2K Center of Excellence
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
from cerebralcortex.core.datatypes.datastream import DataPoint
from core.computefeature import ComputeFeatureBase
import numpy as np
from datetime import datetime
from core.feature.respirationstatistics.utils.peak_valley import \
    compute_peak_valley
from core.feature.respirationstatistics.utils.\
    rip_cycle_feature_computation import rip_cycle_feature_computation

from scipy.stats import skew,kurtosis
from copy import deepcopy
from scipy import stats
from core.feature.respirationstatistics.utils.get_store import get_stream_days
from core.feature.respirationstatistics.utils.util import *

CC = CerebralCortex()
users = CC.get_all_users("mperf-buder")
respiration_raw_autosenseble = "RESPIRATION--org.md2k.autosenseble--AUTOSENSE_BLE--CHEST"
respiration_baseline_autosenseble = "RESPIRATION_BASELINE--org.md2k.autosenseble--AUTOSENSE_BLE--CHEST"
for user in users:
    streams = CC.get_user_streams(user['identifier'])
    user_id = user["identifier"]
    if respiration_raw_autosenseble in streams:
        stream_days = get_stream_days(streams[respiration_raw_autosenseble]["identifier"],CC)

        if not stream_days:
            continue

        for day in stream_days:
            rip_raw = CC.get_stream(streams[respiration_raw_autosenseble]["identifier"], day=day,
                                    user_id=user_id)
            rip_baseline = CC.get_stream(streams[respiration_baseline_autosenseble]["identifier"],
                                         day=day, user_id=user_id)
            if not rip_raw:
                continue
            elif not rip_baseline.data:
                final_respiration = rip_raw.data
            else:
                final_respiration = get_recovery(rip_raw.data[1:1000],
                                                 rip_baseline.data[1:1000],25)
            print(final_respiration)

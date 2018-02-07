# Copyright (c) 2018, MD2K Center of Excellence
# - Nazir Saleheen <nazir.saleheen@gmail.com>
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


import uuid

from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from core.feature.puffmarker.wrist_features import compute_wrist_feature
from core.feature.puffmarker.metadata import update_metadata

def process_puffmarker(user_id: uuid, CC: CerebralCortex, config: dict,
                       accel_stream_left: DataStream,
                       gyro_stream_left: DataStream,
                       accel_stream_right: DataStream,
                       gyro_stream_right: DataStream):
    """
    1. generates puffmarker wrist feature vectors from accelerometer and gyroscope
    2. classifies each feature vector as either puff or non_puff
    3. constructs smoking episodes from this puffs
    :param accel_stream_left:
    :param gyro_stream_left:
    :param accel_stream_right:
    :param gyro_stream_right:
    """

    fast_size = config["thresholds"]["fast_size"]
    slow_size = config["thresholds"]["slow_size"]
    all_features_left = compute_wrist_feature(accel_stream_left, gyro_stream_left, 'leftwrist', fast_size, slow_size)
    all_features_left = update_metadata(all_features_left, user_id, CC, config, accel_stream_left, gyro_stream_left, 'leftwrist')
    CC.save_stream(all_features_left)

    all_features_right = compute_wrist_feature(accel_stream_right, gyro_stream_right, 'rightwrist', fast_size, slow_size)
    all_features_right = update_metadata(all_features_right , user_id, CC, config, accel_stream_right, gyro_stream_right, 'rightwrist')
    CC.save_stream(all_features_right)



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


import pandas as pd, numpy as np
import time
from math import radians, cos, sin, asin, sqrt
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry.multipoint import MultiPoint
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.core.datatypes.datapoint import DataPoint
import datetime


def process_data(user_id: object, CC: object) -> object:
    """

    :param user_id:
    :param CC:
    """
    streams = CC.get_user_streams(user_id)
    if streams and len(streams) > 0:
        gps_gt(streams, user_id, CC)


def gps_gt(streams: object, user_id: object, CC: object) -> object:
    """

    :rtype: object
    :param streams:
    :param user_id:
    :param CC:
    :return:
    """
    if "GEOFENCE--LIST--org.md2k.phonesensor--PHONE" in streams:
        gps_stream_id = streams["GEOFENCE--LIST--org.md2k.phonesensor--PHONE"]["identifier"]
        gps_stream_name = streams["GEOFENCE--LIST--org.md2k.phonesensor--PHONE"]["name"]
    else:
        gps_stream_id = None
    all_day_data = []
    if gps_stream_id:
        stream_end_days = CC.get_stream_duration(gps_stream_id)
        if stream_end_days["start_time"] and stream_end_days["end_time"]:
            days = stream_end_days["end_time"] - stream_end_days["start_time"]
            for day in range(days.days + 1):
                day = (stream_end_days["start_time"] + datetime.timedelta(days=day)).strftime('%Y%m%d')
                stream = CC.get_stream(gps_stream_id, day=day, data_type=DataSet.COMPLETE, user_id=user_id)
                only_data = stream.data
                all_day_data.append(only_data)
    all_gps = []
    cent_name = {}
    for a in all_day_data:
        for aa in a:
            all_gps.append(aa.sample.split('#'))
    for aa in all_gps:
        cen_gps = np.array([float(aa[1]), float(aa[2])])
        cent_name[aa[0]] = cen_gps
    return cent_name

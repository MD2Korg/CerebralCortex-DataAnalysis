import pandas as pd, numpy as np
import time
from math import radians, cos, sin, asin, sqrt
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry.multipoint import MultiPoint
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.core.datatypes.datapoint import DataPoint
import datetime


def process_data(user_id, CC):
    streams = CC.get_user_streams(user_id)
    if streams and len(streams) > 0:
        gps_gt(streams, user_id, CC)

def gps_gt(streams, user_id, CC):
    if "GEOFENCE--LIST--org.md2k.phonesensor--PHONE" in streams:
        gps_stream_id = streams["GEOFENCE--LIST--org.md2k.phonesensor--PHONE"][
            "identifier"]
        # gps_stream_id = 'a1402e7c-d761-3814-b989-bad282f8bec3'
        print(gps_stream_id)
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
                # if gps_stream_id =='4f2d6378-43fd-3c51-b418-d71beb72daa0':
                #     continue
                # else:
                stream = CC.get_stream(gps_stream_id, day=day, data_type=DataSet.COMPLETE)
                only_data = stream.data
                all_day_data.append(only_data)
    all_gps = []
    cent_name = {}
    for a in all_day_data:
        # print(len(a))
        for aa in a:
            all_gps.append(aa.sample.split('#'))
    for aa in all_gps:
        cen_gps = np.array([float(aa[1]),float(aa[2])])
        # print(cen_gps)
        cent_name[aa[0]] = cen_gps
    return cent_name
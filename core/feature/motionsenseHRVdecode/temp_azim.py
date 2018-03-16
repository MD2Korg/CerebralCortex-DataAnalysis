from cerebralcortex.cerebralcortex import CerebralCortex
from core.feature.motionsenseHRVdecode.util_get_store import get_stream_days
from core.feature.motionsenseHRVdecode.util_helper_functions import get_decoded_matrix
import numpy as np
from datetime import datetime

CC = CerebralCortex()
users = CC.get_all_users("mperf-alabsi")

motionsense_hrv_left_raw = "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
motionsense_hrv_right_raw = "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
both_stream_available = False
only_left_available = False
only_right_available = False

for user in users[1:2]:
    streams = CC.get_user_streams(user['identifier'])
    user_id = user["identifier"]
    user_data_collection = {}
    if motionsense_hrv_left_raw in streams:
        # print(1)
        user_data_collection['left']={}
        stream_days_left = get_stream_days(streams[motionsense_hrv_left_raw]["identifier"],
                                           CC)

        if len(stream_days_left)>0:
            for day in stream_days_left:
                motionsense_left_raw = CC.get_stream(streams[motionsense_hrv_left_raw]["identifier"],
                                        day=day,user_id=user_id)
                if len(motionsense_left_raw.data)>0:
                    user_data_collection['left'][day] = np.array(motionsense_left_raw.data)
    if motionsense_hrv_right_raw in streams:
        user_data_collection['right']={}
        stream_days_right = get_stream_days(streams[motionsense_hrv_right_raw]["identifier"],
                                           CC)
        if len(stream_days_right)>0:
            for day in stream_days_right:
                motionsense_right_raw = CC.get_stream(streams[motionsense_hrv_right_raw]["identifier"],
                                                     day=day,user_id=user_id)
                if len(motionsense_right_raw.data)>0:
                    user_data_collection['right'][day] = np.array(motionsense_right_raw.data)

    for day in user_data_collection['left'].keys():
        data = user_data_collection['left'][day]
        offset = data[0].offset
        decoded_sample = get_decoded_matrix(data)
    for day in user_data_collection['right'].keys():
        data = user_data_collection['right'][day]
        offset = data[0].offset
        decoded_sample = get_decoded_matrix(data)






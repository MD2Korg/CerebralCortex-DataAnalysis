# Copyright (c) 2018, MD2K Center of Excellence
# -
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
from pprint import pprint
from scipy.io import savemat
from datetime import timedelta, datetime
from collections import OrderedDict
from cerebralcortex.core.util.data_types import DataPoint
from collections import OrderedDict
from typing import List
from sklearn import ensemble
from collections import Counter
from keras.models import Sequential
from keras.layers import Conv2D, MaxPooling2D, LSTM, Dense, Dropout, Flatten
from keras.layers.core import Permute, Reshape
from keras import backend as K
from keras.models import load_model

import math
import datetime
import pandas as pd
import pytz
import numpy as np
import matplotlib.pyplot as plt
import scipy.io
import keras

from core.feature.typing.utils import *

feature_class_name = 'TypingMarker'

common_days = []

motionsense_hrv_accel_right = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"  # FIXME
motionsense_hrv_accel_left = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"  # FIXME
motionsense_hrv_gyro_right = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"  # FIXME
motionsense_hrv_gyro_left = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"  # FIXME


class TypingMarker(ComputeFeatureBase):
    """

    """

    def collect_data(self, dict, day, user_id):
        # this function colects user data of all stream ids for a common day
        all_data = []
        for stream_id in dict:
            if day in dict[stream_id]:
                data_stream = self.CC.get_stream(stream_id, user_id, day)
                if len(data_stream.data) == 0:
                    continue
                all_data.extend(data_stream.data)
        all_data.sort(key=lambda x: x.start_time)
        return all_data



    def process(self):
        CC = CerebralCortex("/home/md2k/cc_configuration.yml")
        # users = CC.get_all_users("mperf")[:20] # gets user list
        users = ['247d42cf-f81c-44d2-9db8-fea69f468d58']

        for user in users:

            #     user_id = user['identifier']
            user_id = user

            accel_right_stream_ids_with_date = {}
            gyro_right_stream_ids_with_date = {}
            accel_left_stream_ids_with_date = {}
            gyro_left_stream_ids_with_date = {}

            streams = CC.get_user_streams(user_id)  # gets all streams of one user
            stream_ids_with_date = {}

            if streams:
                for s in [motionsense_hrv_accel_right, motionsense_hrv_gyro_right, motionsense_hrv_accel_left,
                          motionsense_hrv_gyro_left]:

                    stream_id_all = CC.get_stream_id(user_id,
                                                     s)  # gets a list of dictionary of all stream ids of one stream

                    stream_ids = []

                    for stream_id in stream_id_all:
                        stream_ids.append(stream_id['identifier'])  # converts the dictionar to a list of stream ids

                    for stream_id in stream_ids:  # for each stream id gets all the days
                        stream_dicts = CC.get_stream_duration(stream_id)
                        stream_days = []
                        days = stream_dicts["end_time"] - stream_dicts["start_time"]

                        for day in range(days.days + 1):
                            stream_days.append((stream_dicts["start_time"] + timedelta(days=day)).strftime('%Y%m%d'))

                        # creates a dictionary of stream ids for each stream where each stream id contains all the dates

                        if s == motionsense_hrv_accel_right:
                            accel_right_stream_ids_with_date[stream_id] = stream_days
                        elif s == motionsense_hrv_gyro_right:
                            gyro_right_stream_ids_with_date[stream_id] = stream_days
                        elif s == motionsense_hrv_accel_left:
                            accel_left_stream_ids_with_date[stream_id] = stream_days
                        elif s == motionsense_hrv_gyro_left:
                            gyro_left_stream_ids_with_date[stream_id] = stream_days

            # creates unique days for the accelerometer left and accelerometer right

            accel_right_unique_days = unique_days_of_one_stream(accel_right_stream_ids_with_date)
            accel_left_unique_days = unique_days_of_one_stream(accel_left_stream_ids_with_date)

            # creates common days for the accelerometer left and accelerometer right

            common_days = list(accel_right_unique_days.intersection(accel_left_unique_days))
            common_days.sort()

            for day in common_days[:1]:
                get_all_data = self.collect_data(accel_right_stream_ids_with_date, day, user_id)
                acc_dataR = get_dataframe(get_all_data, ['time', 'arx', 'ary', 'arz'])

                get_all_data = self.collect_data(gyro_right_stream_ids_with_date, day, user_id)
                gyr_dataR = get_dataframe(get_all_data, ['time', 'grx', 'gry', 'grz'])

                get_all_data = self.collect_data(accel_left_stream_ids_with_date, day, user_id)
                acc_dataL = get_dataframe(get_all_data, ['time', 'alx', 'aly', 'alz'])

                get_all_data = self.collect_data(gyro_left_stream_ids_with_date, day, user_id)
                gyr_dataL = get_dataframe(get_all_data, ['time', 'glx', 'gly', 'glz'])

                dr = pd.concat((acc_dataR[acc_dataR.columns[0:4]], gyr_dataR[gyr_dataR.columns[1:4]]), axis=1)
                dl = pd.concat((acc_dataL[acc_dataL.columns[0:4]], gyr_dataL[gyr_dataL.columns[1:4]]), axis=1)

                dataset = sync_left_right_accel(dl, dr)
                print(dataset)
                offset = 0
                if len(get_all_data)>0:
                    offset = get_all_data[0].offset
                data = typing_episodes(dataset, offset)

    def process_user_day(self, user, all_days):
        if self.CC is None:
            return

        if not user:
            return

        streams = self.CC.get_user_streams(user)

        if not streams:
            self.CC.logging.log("Activity - no streams found for user: %s" %
                                (user))
            return

        for day in all_days:
            self.store_stream(filepath='json_file_path',
                              input_streams=[],
                              user_id=user,
                              data=[])

        self.CC.logging.log("Finished processing Activity for user: %s" % (user))

# Copyright (c) 2018, MD2K Center of Excellence
# - Anik Khan <aniknagato@gmail.com>
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
from core.computefeature import ComputeFeatureBase
import traceback
from datetime import datetime, timedelta
import pprint as pp
import numpy as np

from math import radians, cos, sin, asin, sqrt, ceil, floor

import pdb
import uuid

import json

feature_class_name = 'GPSDaily'


class GPSDaily(ComputeFeatureBase):

    def split_datapoint_array_by_day(self, data: object) -> object:
        """
        Returns DataPoint array splitted wth respect to days considering localtime.

        :param data: Input data (single DataPoint)
        :return: Splitted list of DataPoints
        :rtype: List(DataPoint)
        """
        data_by_day = []
        for dp in data:
            start_date = dp.start_time.date()
            end_date = dp.end_time.date()
            start_time = dp.start_time
            end_time = dp.end_time
            offset = dp.offset
            timezoneinfo = start_time.tzinfo

            if start_date == end_date:
                data_by_day.append(dp)
                continue

            while (start_date != end_date):
                new_end_time = start_time + timedelta(days=1)
                new_end_date = new_end_time.date()

                new_end_date_str = str(new_end_date).replace("-", "")
                new_end_datetime = datetime.strptime(new_end_date_str, "%Y%m%d")

                new_end_datetime = new_end_datetime.replace(tzinfo=timezoneinfo)

                new_datapoint = DataPoint(start_time, new_end_datetime, offset, dp.sample)

                data_by_day.append(new_datapoint)

                start_date = new_end_date

                start_date_str = str(start_date).replace("-", "")

                start_time = start_time + timedelta(days=1)

            new_start_str = str(start_date).replace("-", "")
            new_start_datetime = datetime.strptime(new_start_str, "%Y%m%d")
            new_start_datetime = new_start_datetime.replace(tzinfo=timezoneinfo)
            new_datapoint = DataPoint(new_start_datetime, end_time, offset, dp.sample)
            data_by_day.append(new_datapoint)

        return data_by_day

    def process(self, user_id: object, all_days: object):
        """

        :param user_id:
        :param all_days:
        """

        stream_name_gps_cluster = "org.md2k.data_analysis.gps_clustering_episode_generation"
        stream_name_semantic_location = "org.md2k.data_analysis.gps_episodes_and_semantic_location_from_model"
        stream_name_semantic_location_places =\
        "org.md2k.data_analysis.gps_episodes_and_semantic_location_from_places"
        stream_name_semantic_location_user_marked = "org.md2k.data_analysis.gps_episodes_and_semantic_location_user_marked"
        streams = self.CC.get_user_streams(user_id)

        for day in all_days:

            cluster_data_duplication = []
            semantic_data_duplication = []
            semantic_places_data_duplication = []
            semantic_user_data_duplication = []

            if stream_name_gps_cluster in streams:
                cluster_stream_ids = self.CC.get_stream_id(user_id, stream_name_gps_cluster)
                for cluster_stream_id in cluster_stream_ids:
                    cluster_data_duplication += self.CC.get_stream(cluster_stream_id['identifier'], user_id, day,
                                                                   localtime=True).data

            if stream_name_semantic_location in streams:
                semantic_stream_ids = self.CC.get_stream_id(user_id, stream_name_semantic_location)
                for semantic_stream_id in semantic_stream_ids:
                    semantic_data_duplication += self.CC.get_stream(semantic_stream_id['identifier'], user_id, day,
                                                                    localtime=True).data

            if stream_name_semantic_location_places in streams:
                semantic_stream_ids = self.CC.get_stream_id(user_id,
                                                            stream_name_semantic_location_places)
                for semantic_stream_id in semantic_stream_ids:
                    semantic_places_data_duplication += self.CC.get_stream(semantic_stream_id['identifier'], user_id, day,
                                                                    localtime=True).data

            if stream_name_semantic_location_user_marked in streams:
                user_marked_stream_ids = self.CC.get_stream_id(user_id, stream_name_semantic_location_user_marked)
                for user_marked_stream_id in user_marked_stream_ids:
                    semantic_user_data_duplication += self.CC.get_stream(user_marked_stream_id['identifier'], user_id,
                                                                         day, localtime=True).data

            cluster_data_unique = []
            cluster_data_start_time = []

            for dd in cluster_data_duplication:
                if dd.start_time in cluster_data_start_time:
                    continue
                else:
                    cluster_data_start_time.append(dd.start_time)
                    cluster_data_unique.append(dd)

            cluster_data_by_day = self.split_datapoint_array_by_day(cluster_data_unique)

            try:
                if len(cluster_data_by_day):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_gps_cluster:
                            self.store_stream(filepath="gps_data_clustering_episode_generation_daily.json",
                                              input_streams=[streams[stream_name]],
                                              user_id=user_id,
                                              data=cluster_data_by_day, localtime=True)
                            break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(cluster_data_by_day)))

            semantic_data_unique = []
            semantic_data_start_time = []

            for dd in semantic_data_duplication:
                if dd.start_time in semantic_data_start_time:
                    continue
                else:
                    semantic_data_start_time.append(dd.start_time)
                    semantic_data_unique.append(dd)

            semantic_data_by_day = self.split_datapoint_array_by_day(semantic_data_unique)

            try:
                if len(semantic_data_by_day):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_semantic_location:
                            self.store_stream(filepath="gps_episodes_and_semantic_location_from_model_daily.json",
                                              input_streams=[streams[stream_name]],
                                              user_id=user_id,
                                              data=semantic_data_by_day, localtime=True)
                            break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(semantic_data_by_day)))


            semantic_places_data_unique = []
            semantic_places_data_start_time = []

            for dd in semantic_places_data_duplication:
                if dd.start_time in semantic_places_data_start_time:
                    continue
                else:
                    semantic_places_data_start_time.append(dd.start_time)
                    semantic_places_data_unique.append(dd)

            semantic_places_data_by_day =\
            self.split_datapoint_array_by_day(semantic_places_data_unique)

            try:
                if len(semantic_places_data_by_day):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_semantic_location_places:
                            self.store_stream(filepath="gps_episodes_and_semantic_location_from_places_daily.json",
                                              input_streams=[streams[stream_name]],
                                              user_id=user_id,
                                              data=semantic_places_data_by_day, localtime=True)
                            break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(semantic_data_by_day)))

            if stream_name_semantic_location_user_marked in streams:

                semantic_user_data_unique = []
                semantic_user_data_start_time = []

                for dd in semantic_user_data_duplication:
                    if dd.start_time in semantic_user_data_start_time:
                        continue
                    else:
                        semantic_user_data_start_time.append(dd.start_time)
                        semantic_user_data_unique.append(dd)

                semantic_user_data_by_day = self.split_datapoint_array_by_day(semantic_user_data_unique)

                try:
                    if len(semantic_user_data_by_day):
                        streams = self.CC.get_user_streams(user_id)
                        for stream_name, stream_metadata in streams.items():
                            if stream_name == stream_name_semantic_location:
                                self.store_stream(filepath="gps_episodes_and_semantic_location_user_marked_daily.json",
                                                  input_streams=[streams[stream_name]],
                                                  user_id=user_id,
                                                  data=semantic_data_by_day, localtime=True)
                                break

                except Exception as e:
                    self.CC.logging.log("Exception:", str(e))
                    self.CC.logging.log(traceback.format_exc())
                self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                    'data points' %
                                    (self.__class__.__name__, str(user_id),
                                     len(semantic_user_data_by_day)))

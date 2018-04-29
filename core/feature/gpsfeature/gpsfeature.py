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

feature_class_name = 'GPSFeatures'
stream_name_gps_cluster = "org.md2k.data_analysis.v1.gps_clustering_episode_generation_daily"
stream_name_semantic_location = "org.md2k.data_analysis.v1.gps_episodes_and_semantic_location_daily"
stream_name_semantic_location_places = \
"org.md2k.data_analysis.v1.gps_episodes_and_semantic_location_from_places_daily"
stream_name_semantic_location_user_marked = "org.md2k.data_analysis.v1.gps_episodes_and_semantic_location_user_marked_daily"


class GPSFeatures(ComputeFeatureBase):
    def haversine(self, lon1: float, lat1: float, lon2: float, lat2: float) -> float:
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)

        :rtype: object
        :param lon1:
        :param lat1:
        :param lon2:
        :param lat2:
        :return:
        """

        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        km = 6373 * c
        return km

    def total_distance_covered(self, data: object) -> object:
        """
        Total distance covered in a day.

        :return:
        :param data: DataPoint array of centroid stream
        :return: total distance covered by participant in kilometers
        :rtype: List(DataPoint) with a single element.
        """

        total_distance = 0
        data_without_transit = []

        for dp in data:
            if (float(dp.sample[1]) != -1.0):
                data_without_transit.append(dp)

        if len(data_without_transit) == 0:
            return []

        i = 0
        while i <= len(data_without_transit) - 2:
            lattitude_pre = float(data_without_transit[i].sample[1])
            longitude_pre = float(data_without_transit[i].sample[2])

            lattitude_post = float(data_without_transit[i + 1].sample[1])
            longitude_post = float(data_without_transit[i + 1].sample[2])

            distance = self.haversine(longitude_pre, lattitude_pre, longitude_post, lattitude_post)
            total_distance = total_distance + distance
            i += 1

        start_time = data[0].start_time
        end_time = data[-1].end_time
        offset = data[0].offset

        total_distance_datapoint = DataPoint(start_time, end_time, offset, total_distance)

        return [total_distance_datapoint]

    def maximum_distance_between_two_locations(self, data: object) -> object:
        """
        Maximum distance between two locations covered by participant in kilometers in a day.

        :param data: DataPoint array of centroid stream
        :return: maximum distance between two locations covered by participant in kilometers
        :rtype: List(DataPoint) with a single element.
        """

        data_without_transit = []

        for dp in data:
            if (float(dp.sample[1]) != -1.0):
                data_without_transit.append(dp)

        if len(data_without_transit) == 0:
            return []

        max_dist_bet_two_locations = 0
        i = 0
        j = 0
        while i < len(data_without_transit):

            while j < len(data_without_transit):

                dist_bet_i_j = self.haversine(float(data_without_transit[i].sample[2]),
                                              float(data_without_transit[i].sample[1]),
                                              float(data_without_transit[j].sample[2]),
                                              float(data_without_transit[j].sample[1]))
                if (dist_bet_i_j > max_dist_bet_two_locations):
                    max_dist_bet_two_locations = dist_bet_i_j
                j = j + 1
            i = i + 1

        start_time = data[0].start_time
        end_time = data[-1].end_time
        offset = data[0].offset
        max_distance_datapoint = DataPoint(start_time, end_time, offset, max_dist_bet_two_locations)

        return [max_distance_datapoint]

    def number_of_different_places(self, data: object) -> object:
        """
        Number of different places the participant visited in a day.

        :param data: DataPoint array of centroid stream
        :return: number of different places the participant visited in a day
        :rtype: List(DataPoint) with a single element.
        """

        num_diff_places = 0

        loc_array = []

        ii = 0
        while ii < len(data):
            if float(data[ii].sample[1]) == -1.0:
                ii = ii + 1
                continue
            concat_string = str(data[ii].sample[1]) + str(data[ii].sample[2])
            loc_array.append(concat_string)
            ii = ii + 1

        if len(loc_array) == 0:
            return []

        loc_dict = {}
        i = 0
        same = 0
        while i < len(loc_array):
            if (loc_array[i] in loc_dict.keys()):
                same = same + 1

            else:
                num_diff_places = num_diff_places + 1
                loc_dict[loc_array[i]] = 1

            i = i + 1

        start_time = data[0].start_time
        end_time = data[-1].end_time
        offset = data[0].offset
        num_of_diff_pls_datapoint = DataPoint(start_time, end_time, offset, num_diff_places)

        return [num_of_diff_pls_datapoint]

    def standard_deviation_of_displacements(self, datawithtransit: object) -> object:
        """
        Standard deviation of displacements of a user in a day.

        :param datawithtransit: DataPoint array of centroid stream
        :return: standard deviation of displacements in a day
        :rtype: List(DataPoint) with a single element.
        """

        data = []
        ii = 0
        while ii < len(datawithtransit):
            if (float(datawithtransit[ii].sample[0]) != -1.0):
                data.append(datawithtransit[ii])
            ii = ii + 1

        if len(datawithtransit) == 0:
            return []

        mean_distance = 0
        i = 0
        while i < len(data) - 1:
            mean_distance = mean_distance + self.haversine(data[i].sample[2], data[i].sample[1],
                                                           data[i + 1].sample[2], data[i + 1].sample[1])
            i = i + 1

        if len(data) < 2:
            return []

        mean_distance = mean_distance / (len(data) - 1)
        var_distance = 0
        j = 0
        while j < len(data) - 1:
            var_distance = var_distance + (self.haversine(data[j].sample[2], data[j].sample[1],
                                                          data[j + 1].sample[2],
                                                          data[j + 1].sample[1]) - mean_distance) ** 2
            j = j + 1

        standard_deviation = sqrt(var_distance / (len(data) - 1))

        start_time = data[0].start_time
        end_time = data[-1].end_time
        offset = data[0].offset

        stan_dev_datapoint = DataPoint(start_time, end_time, offset, standard_deviation)

        return [stan_dev_datapoint]

    def cumulative_staying_time(self, semanticdata: object, user_id: str):
        """
        Cumulative staying time of one type of place.

        :param semanticdata: DataPoint array of semantic stream
        :return: cumulative staying time at different types of locations
        :rtype: List[DataPoint] with a single element (dictionary).
        """

        data = semanticdata
        time_dictionary = {}

        i = 0
        while i < len(data):
            get_time_datetime = data[i].end_time - data[i].start_time
            get_time = get_time_datetime.total_seconds()

            get_pre_time = 0
            if data[i].sample[0] in time_dictionary.keys():
                get_pre_time = time_dictionary[data[i].sample]
            new_time = get_pre_time + get_time
            time_dictionary[data[i].sample] = new_time
            i = i + 1

        start_time = data[0].start_time
        end_time = data[-1].end_time
        offset = data[0].offset

        if 'home' in time_dictionary:
            home_datapoint = DataPoint(start_time, end_time, offset,
                                   time_dictionary['home'])

            try:
                streams = self.CC.get_user_streams(user_id)
                for stream_name, stream_metadata in streams.items():
                    if stream_name == stream_name_semantic_location:
                        self.store_stream(filepath="cumulative_staying_time_home.json",
                                          input_streams=[stream_metadata],
                                          user_id=user_id,
                                          data=[home_datapoint])
                        break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())

        if 'work' in time_dictionary:
            work_datapoint = DataPoint(start_time, end_time, offset,
                                   time_dictionary['work'])

            try:
                streams = self.CC.get_user_streams(user_id)
                for stream_name, stream_metadata in streams.items():
                    if stream_name == stream_name_semantic_location:
                        self.store_stream(filepath="cumulative_staying_time_work.json",
                                          input_streams=[stream_metadata],
                                          user_id=user_id,
                                          data=[work_datapoint])
                        break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())


    def cumulative_staying_time_poi(self, semanticdata: object, user_id: str):
        """
        Cumulative staying time of one type of place at different places of
        interest.

        :param semanticdata: DataPoint array of semantic stream
        :return: cumulative staying time at different types of locations
        :rtype: List[DataPoint] with a single element (dictionary).
        """

        data = semanticdata
        time_dictionary = {}
        '''
        Datapoin sample is a list as follows
        0 rest and bar
        1 school
        2 place of worship
        3 entertainment
        4 store
        5 sport

        order of preference to use
        worship 
        school
        sport
        entertainment
        store
        restaurant and bar
    
        '''

        i = 0
        while i < len(data):
            get_time_datetime = data[i].end_time - data[i].start_time
            get_time = get_time_datetime.total_seconds()

            nearby_places = data[i].sample
            poi_touse = -1

            if nearby_places[2] == 'yes' or nearby_places[2] == 1: #worship
                poi_touse = 2
            elif nearby_places[1] == 'yes' or nearby_places[1] == 1: #school
                poi_touse = 1
            elif nearby_places[5] == 'yes' or nearby_places[5] == 1: #sport
                poi_touse = 5
            elif nearby_places[3] == 'yes' or nearby_places[3] == 1:#entertainment
                poi_touse = 3
            elif nearby_places[4] == 'yes' or nearby_places[4] == 1: #store
                poi_touse = 4
            elif nearby_places[0] == 'yes' or nearby_places[0] == 1: #rest&bar
                poi_touse = 0



            get_pre_time = 0
            if poi_touse in time_dictionary.keys():
                get_pre_time = time_dictionary[poi_touse]
            new_time = get_pre_time + get_time
            time_dictionary[poi_touse] = new_time
            i = i + 1

        start_time = data[0].start_time
        end_time = data[-1].end_time
        offset = data[0].offset

        if 0 in time_dictionary:
            restbar_datapoint = DataPoint(start_time, end_time, offset,
                                   time_dictionary[0])
            #print('restbar',restbar_datapoint)

            try:
                streams = self.CC.get_user_streams(user_id)
                for stream_name, stream_metadata in streams.items():
                    if stream_name == stream_name_semantic_location_places:
                        self.store_stream(filepath="cumulative_staying_time_restbar.json",
                                          input_streams=[stream_metadata],
                                          user_id=user_id,
                                          data=[restbar_datapoint])
                        break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
        if 1 in time_dictionary:
            school_datapoint = DataPoint(start_time, end_time, offset,
                                   time_dictionary[1])
            #print('school',school_datapoint)

            try:
                streams = self.CC.get_user_streams(user_id)
                for stream_name, stream_metadata in streams.items():
                    if stream_name == stream_name_semantic_location_places:
                        self.store_stream(filepath="cumulative_staying_time_school.json",
                                          input_streams=[stream_metadata],
                                          user_id=user_id,
                                          data=[school_datapoint])
                        break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
        if 2 in time_dictionary:
            worship_datapoint = DataPoint(start_time, end_time, offset,
                                   time_dictionary[2])
            #print('worship',worship_datapoint)

            try:
                streams = self.CC.get_user_streams(user_id)
                for stream_name, stream_metadata in streams.items():
                    if stream_name == stream_name_semantic_location_places:
                        self.store_stream(filepath="cumulative_staying_time_worship.json",
                                          input_streams=[stream_metadata],
                                          user_id=user_id,
                                          data=[worship_datapoint])
                        break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
        if 3 in time_dictionary:
            entertainment_datapoint = DataPoint(start_time, end_time, offset,
                                   time_dictionary[3])
            #print('entertainment',entertainment_datapoint)

            try:
                streams = self.CC.get_user_streams(user_id)
                for stream_name, stream_metadata in streams.items():
                    if stream_name == stream_name_semantic_location_places:
                        self.store_stream(filepath="cumulative_staying_time_entertainment.json",
                                          input_streams=[stream_metadata],
                                          user_id=user_id,
                                          data=[entertainment_datapoint])
                        break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
        if 4 in time_dictionary:
            store_datapoint = DataPoint(start_time, end_time, offset,
                                   time_dictionary[4])
            #print('store',store_datapoint)

            try:
                streams = self.CC.get_user_streams(user_id)
                for stream_name, stream_metadata in streams.items():
                    if stream_name == stream_name_semantic_location_places:
                        self.store_stream(filepath="cumulative_staying_time_store.json",
                                          input_streams=[stream_metadata],
                                          user_id=user_id,
                                          data=[store_datapoint])
                        break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
        if 5 in time_dictionary:
            sport_datapoint = DataPoint(start_time, end_time, offset,
                                   time_dictionary[5])
            #print('sport',sport_datapoint)

            try:
                streams = self.CC.get_user_streams(user_id)
                for stream_name, stream_metadata in streams.items():
                    if stream_name == stream_name_semantic_location_places:
                        self.store_stream(filepath="cumulative_staying_time_sport.json",
                                          input_streams=[stream_metadata],
                                          user_id=user_id,
                                          data=[sport_datapoint])
                        break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())



    def transition_counter(self, semanticdata: object) -> object:
        """
        Number of transitions from one type of place to another.

        :param semanticdata: DataPoint array of semantic stream
        :return: number of transitions from one type of location to another
        :rtype: List(DataPoint) with a single element (dictionary).
        """

        semanticwithouttransit = []
        jj = 0
        while jj < len(semanticdata):
            if (str(semanticdata[jj].sample) != "transit"):
                semanticwithouttransit.append(semanticdata[jj])
            jj = jj + 1

        number_of_trans_dict = {}
        i = 0
        to_work_transitions = 0
        to_home_transitions = 0

        while i < len(semanticwithouttransit) - 1:

            pre_loc = semanticwithouttransit[i]
            post_loc = semanticwithouttransit[i + 1]
            if pre_loc.sample != post_loc.sample:
                key_string = pre_loc.sample + " " + post_loc.sample
                get_pre_num = 0

                if post_loc.sample.lower() == 'work':
                    to_work_transitions += 1
                if post_loc.sample.lower() == 'home':
                    to_home_transitions += 1


                if (key_string in number_of_trans_dict.keys()):
                    get_pre_num = number_of_trans_dict[key_string]
                new_num = get_pre_num + 1
                # print (new_num)
                number_of_trans_dict[key_string] = new_num
            i = i + 1

        start_time = semanticdata[0].start_time
        end_time = semanticdata[-1].end_time
        offset = semanticdata[0].offset

        output_datapoint = DataPoint(start_time, end_time, offset, number_of_trans_dict)
        to_work_transitions_datapoint = DataPoint(start_time, end_time, offset,
                                                 to_work_transitions)
        to_home_transitions_datapoint = DataPoint(start_time, end_time, offset,
                                                 to_home_transitions)
        toreturn = []
        toreturn.append(output_datapoint)
        toreturn.append(to_work_transitions_datapoint)
        toreturn.append(to_home_transitions_datapoint)
        return toreturn

    def maximum_distance_from_home(self, home_lattitude: object, home_longitude: object,
                                   centroiddata: object) -> object:
        """
        Maximum distance from home.

        :return:
        :param home_lattitude: lattitude of home's location
        :param home_longitude: longitude of home's location
        :param centroiddata: list of centroid datapoints.
        :rtype: List(DataPoint) with a single element.
        """

        max = 0

        jj = 0
        centroidwithouttransit = []
        while jj < len(centroiddata):
            if float(centroiddata[jj].sample[1]) != -1.0:
                centroidwithouttransit.append(centroiddata[jj])
            jj = jj + 1

        i = 0

        if len(centroidwithouttransit) == 0:
            return []

        while i < len(centroidwithouttransit):
            lattitude = centroidwithouttransit[i].sample[1]
            longitude = centroidwithouttransit[i].sample[2]
            distance = self.haversine(float(home_longitude), float(home_lattitude), float(longitude), float(lattitude))
            # print (distance)
            if (max < distance):
                max = distance
            i = i + 1

        start_time = centroiddata[0].start_time
        end_time = centroiddata[-1].end_time
        offset = centroiddata[0].offset

        max_datapoint = DataPoint(start_time, end_time, offset, max)

        return [max_datapoint]

    def radius_of_gyration(self, centroiddatapoints: object) -> object:
        """
        Radius of gyration of a participant in a day.

        :param data: DataPoint array of centroid stream
        :return: radius_of_gyration
        :rtype: List(DataPoint) with a single element.
        """
        data = []

        for dp in centroiddatapoints:
            if (float(dp.sample[0]) != -1.0):
                data.append(dp)

        if len(data) == 0:
            return []

        summed_lattitude = 0
        summed_longitude = 0

        for dp in data:
            summed_lattitude = summed_lattitude + float(dp.sample[1])
            summed_longitude = summed_longitude + float(dp.sample[2])

        mean_lattitude = 0
        mean_longitude = 0

        if len(data) > 0:
            mean_lattitude = summed_lattitude / len(data)
            mean_longitude = summed_longitude / len(data)
        total_time = 0
        time_distance = 0

        for dp in data:
            total_time = total_time + (dp.end_time - dp.start_time).total_seconds()
            distance = self.haversine(float(dp.sample[2]), float(dp.sample[1]), mean_longitude, mean_lattitude)
            time_distance = time_distance + ((dp.end_time - dp.start_time).total_seconds()) * distance * distance

        rad_of_gyration = 0

        if total_time > 0:
            rad_of_gyration = sqrt(time_distance / total_time)

        start_time = centroiddatapoints[0].start_time
        end_time = centroiddatapoints[-1].end_time
        offset = centroiddatapoints[0].offset

        rad_gyr_datapoint = DataPoint(start_time, end_time, offset, rad_of_gyration)

        return [rad_gyr_datapoint]

    def mobility_places(self, data: object) -> object:
        """
        Returns list of lists places visited by the user in whole study. 
        Each element lists the places visited by the user in a day.

        :param data: DataPoint array of centroid stream
        :return: Mobility places of one participant in a day ( with interval 15 minutes ).
        :rtype: List(DataPoint).
        """

        mob_places = []
        # window size 15 minutes
        data_window_min = 15
        hours_in_day = 24

        i = 0
        while i < hours_in_day * 60 / data_window_min:
            mob_places.append("MISSING")
            i = i + 1

        for dp in data:

            start_hour = dp.start_time.hour
            start_minute = dp.start_time.minute
            start_index = ceil((start_hour * 60 + start_minute) / data_window_min)
            end_hour = dp.end_time.hour
            end_minute = dp.end_time.minute
            end_index = floor((end_hour * 60 + end_minute) / data_window_min)

            index = start_index

            while index <= end_index:
                if (float(dp.sample[0]) != -1.0):
                    mob_places[index] = str(dp.sample[1]) + str(dp.sample[2])
                index = index + 1

        start_time = data[0].start_time
        end_time = data[-1].end_time
        offset = data[0].offset

        mob_pls_datapoint = DataPoint(start_time, end_time, offset, mob_places)

        return mob_pls_datapoint

    def average_difference(self, datapoint1: object, datapoint2: object) -> object:
        """
        Average difference of the places visited by user in two different days.

        :param datapoint1:
        :param datapoint2:
        :return: averaged different places in two days
        :rtype: number(float)
        """
        data1 = datapoint1.sample
        data2 = datapoint2.sample

        i = 0
        difference = 0
        while i < min(len(data1), len(data2)):
            if (data1[i] != "MISSING" and data2[i] != "MISSING" and data1[i] != data2[i]):
                difference = difference + 1

            i = i + 1

        average_diff = difference / min(len(data1), len(data2))

        return average_diff

    def routine_index(self, places: object) -> object:
        """
        Returns Routine Index for all days of the participant.

        :param places:
        :return: total distance covered by participant in kilometers
        :rtype: List(DataPoint) with a single element.

        """

        if len(places) <= 1:
            return []

        routine_ind_datapoints = []
        i = 0
        while i < len(places):

            j = 0
            summed_diff = 0

            while j < len(places):
                if (i != j):
                    summed_diff = summed_diff + self.average_difference(places[i], places[j])
                j = j + 1
            routine_index_value = summed_diff / (len(places) - 1)
            start_time = places[i].start_time
            end_time = places[i].end_time
            offset = places[i].offset

            routine_ind_datapoint = DataPoint(start_time, end_time, offset, routine_index_value)
            routine_ind_datapoints.append(routine_ind_datapoint)

            i = i + 1

        return routine_ind_datapoints

    def available_data_in_time(self, data: object) -> object:
        """
        Available data of a participant in seconds.

        :param data: DataPoint array of centroid stream
        :return: available data in a day in seconds
        :rtype: List(DataPoint) with a single element.
        """
        total_time = 0
        for dp in data:
            total_time += (dp.end_time - dp.start_time).total_seconds()
        if total_time < 0:
            return []

        start_time = data[0].start_time
        end_time = data[-1].end_time
        offset = data[0].offset

        datapoint = DataPoint(start_time, end_time, offset, total_time)

        return [datapoint]

    def process(self, user_id: object, all_days: object):
        """

        :param user_id:
        :param all_days:
        """

        streams = self.CC.get_user_streams(user_id)

        # this loop is for calculating home's lattitude and longitude
        home_present = False

        for day in all_days:

            if not stream_name_gps_cluster in streams:
                continue

            if stream_name_gps_cluster in streams:
                cluster_stream_ids = self.CC.get_stream_id(user_id, stream_name_gps_cluster)
                cluster_data = []
                for cluster_stream_id in cluster_stream_ids:
                    cluster_data += self.CC.get_stream(cluster_stream_id['identifier'], user_id, day).data

            if stream_name_semantic_location in streams:
                semantic_stream_ids = self.CC.get_stream_id(user_id, stream_name_semantic_location)
                semantic_data = []
                for semantic_stream_id in semantic_stream_ids:
                    semantic_data += self.CC.get_stream(semantic_stream_id['identifier'], user_id, day).data

            if stream_name_semantic_location_user_marked in streams:
                user_marked_stream_ids = self.CC.get_stream_id(user_id, stream_name_semantic_location_user_marked)
                user_marked_data = []
                for user_marked_stream_id in user_marked_stream_ids:
                    user_marked_data += self.CC.get_stream(user_marked_stream_id['identifier'], user_id, day).data

                j = 0
                while j < len(user_marked_data):
                    if (user_marked_data[j].sample[0].lower() == "home"):
                        home_lattitude = cluster_data[j].sample[1]
                        home_longitude = cluster_data[j].sample[2]
                        home_present = True
                        break
                    j = j + 1

            if home_present == False and stream_name_semantic_location in streams:
                i = 0
                while i < len(semantic_data):
                    if  isinstance(semantic_data[i].sample, str) and semantic_data[i].sample.lower() == "home":
                        home_lattitude = cluster_data[i].sample[1]
                        home_longitude = cluster_data[i].sample[2]
                        home_present = True
                        break
                    i = i + 1

                if i == len(semantic_data):
                    home_present = False

        day_places = []
        for day in all_days:

            if not stream_name_gps_cluster in streams:
                continue

            if stream_name_gps_cluster in streams:
                cluster_stream_ids = self.CC.get_stream_id(user_id, stream_name_gps_cluster)
                cluster_data = []
                for cluster_stream_id in cluster_stream_ids:
                    cluster_data += self.CC.get_stream(cluster_stream_id['identifier'], user_id, day).data

            if len(cluster_data) == 0:
                continue
            day_places.append(self.mobility_places(cluster_data))

        rout_ind = self.routine_index(day_places)

        try:
            if len(rout_ind):
                streams = self.CC.get_user_streams(user_id)
                for stream_name, stream_metadata in streams.items():
                    if stream_name == stream_name_gps_cluster:
                        # print("---->",stream_metadata)

                        self.store_stream(filepath="routine_index.json",
                                          input_streams=[stream_metadata],
                                          user_id=user_id,
                                          data=rout_ind)
                        break
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(traceback.format_exc())
        self.CC.logging.log('%s finished processing for user_id %s saved %d '
                            'data points' %
                            (self.__class__.__name__, str(user_id),
                             len(rout_ind)))

        for day in all_days:
            if not stream_name_gps_cluster in streams:
                continue

            if stream_name_gps_cluster in streams:
                cluster_stream_ids = self.CC.get_stream_id(user_id, stream_name_gps_cluster)
                cluster_data = []
                for cluster_stream_id in cluster_stream_ids:
                    cluster_data += self.CC.get_stream(cluster_stream_id['identifier'], user_id, day).data

            if stream_name_semantic_location in streams:
                semantic_stream_ids = self.CC.get_stream_id(user_id, stream_name_semantic_location)
                semantic_data = []
                for semantic_stream_id in semantic_stream_ids:
                    semantic_data += self.CC.get_stream(semantic_stream_id['identifier'], user_id, day).data

            if stream_name_semantic_location_places in streams:
                semantic_stream_ids = self.CC.get_stream_id(user_id,
                                                            stream_name_semantic_location_places)
                semantic_data_places = []
                for semantic_stream_id in semantic_stream_ids:
                    semantic_data_places += self.CC.get_stream(semantic_stream_id['identifier'], user_id, day).data

            if len(cluster_data) == 0:
                continue

            available_data = self.available_data_in_time(cluster_data)

            try:
                if len(available_data):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_gps_cluster:
                            self.store_stream(filepath="gpsfeature_data_available.json",
                                              input_streams=[stream_metadata],
                                              user_id=user_id,
                                              data=available_data)
                            break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(available_data)))

            rad_gyr = self.radius_of_gyration(cluster_data)

            try:
                if len(rad_gyr):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_gps_cluster:
                            # print("---->",stream_metadata)

                            self.store_stream(filepath="radius_of_gyration.json",
                                              input_streams=[stream_metadata],
                                              user_id=user_id,
                                              data=rad_gyr)
                            break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(rad_gyr)))

            tot_dist = self.total_distance_covered(cluster_data)

            try:
                if len(tot_dist):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_gps_cluster:
                            self.store_stream(filepath="total_distance_covered.json",
                                              input_streams=[streams[stream_name]],
                                              user_id=user_id,
                                              data=tot_dist)
                            break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(tot_dist)))

            max_dist = self.maximum_distance_between_two_locations(cluster_data)

            try:
                if len(max_dist):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_gps_cluster:
                            # print("---->",stream_metadata)

                            self.store_stream(filepath="maximum_distance_covered.json",
                                              input_streams=[stream_metadata],
                                              user_id=user_id,
                                              data=max_dist)
                            break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(max_dist)))

            num_of_diff_pls = self.number_of_different_places(cluster_data)

            try:
                if len(num_of_diff_pls):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_gps_cluster:
                            self.store_stream(filepath="number_of_different_places.json",
                                              input_streams=[stream_metadata],
                                              user_id=user_id,
                                              data=num_of_diff_pls)
                            break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(num_of_diff_pls)))

            standard_dev = self.standard_deviation_of_displacements(cluster_data)

            try:
                if len(standard_dev):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_gps_cluster:
                            self.store_stream(filepath="standard_deviation_of_displacements.json",
                                              input_streams=[stream_metadata],
                                              user_id=user_id,
                                              data=standard_dev)
                            break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(standard_dev)))

            self.cumulative_staying_time(semantic_data,user_id)

            self.cumulative_staying_time_poi(semantic_data_places,user_id)


            trans_fre = self.transition_counter(semantic_data)
            try:
                if len(trans_fre):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_semantic_location:
                            self.store_stream(filepath="transition_counter.json",
                                              input_streams=[stream_metadata],
                                              user_id=user_id,
                                              data=[trans_fre[0]])
                            self.store_stream(filepath="to_work_transitions.json",
                                              input_streams=[stream_metadata],
                                              user_id=user_id,
                                              data=[trans_fre[1]])
                            self.store_stream(filepath="to_home_transitions.json",
                                              input_streams=[stream_metadata],
                                              user_id=user_id,
                                              data=[trans_fre[2]])
                            break
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(trans_fre)))

            if home_present:

                max_dis_home = self.maximum_distance_from_home(home_lattitude, home_longitude, cluster_data)

                try:
                    input_stream_names = [stream_name_semantic_location, stream_name_gps_cluster,
                                          stream_name_semantic_location_user_marked]
                    input_streams = []
                    if len(max_dis_home):
                        streams = self.CC.get_user_streams(user_id)
                        for stream_name, stream_metadata in streams.items():
                            if stream_name in input_stream_names:
                                input_streams.append(stream_metadata)

                        self.store_stream(filepath="maximum_distance_from_home.json",
                                          input_streams=input_streams,
                                          user_id=user_id,
                                          data=max_dis_home)
                except Exception as e:
                    self.CC.logging.log("Exception:", str(e))
                    self.CC.logging.log(traceback.format_exc())
                self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                    'data points' %
                                    (self.__class__.__name__, str(user_id),
                                     len(max_dis_home)))

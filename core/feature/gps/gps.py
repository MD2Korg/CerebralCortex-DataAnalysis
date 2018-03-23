import uuid
import pickle
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from pprint import pprint
from scipy.io import savemat
import datetime
import numpy as np, pandas as pd
import time, os
from math import radians, cos, sin, asin, sqrt
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry.multipoint import MultiPoint
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.core.datatypes.datapoint import DataPoint
import datetime
from core.computefeature import ComputeFeatureBase
from googleplaces import GooglePlaces, types, lang
import googleplaces


class GPSClusteringEpochComputation(ComputeFeatureBase):
    INTERPOLATION_TIME = 1.0
    KM_PER_RADIAN = 6371.0088
    EPSILON_CONSTANT = 1000
    GPS_ACCURACY_THRESHOLD = 41.0
    GEO_FENCE_DISTANCE = 2
    MINIMUM_POINTS_IN_CLUSTER = 500
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    GEOFENCE_ASSIGNING_CENTROID = 1000
    EPOCH_THRESHOLD = 0.5
    LATITUDE = 0
    LONGITUDE = 1
    ACCURACY = -1
    GPS_DATA_THRESHOLD = 100
    OFFSET = -1
    GPS_SAMPLE_LENGTH = 6
    IN_SECONDS = 60
    SEMANTIC_NAMES_INDEX = 5
    CENTROID_LATITUDE = 3
    CENTROID_LONGITUDE = 4
    OFFSET_INDEX = 6
    GROUND_STRING_LENGTH = 3
    MODEL_FILE_PATH = 'core/resources/models/gps/model.pkl'
    UNDEFINED = 'UNDEFINED'
    RESTAURANT = ['restaurant', 'bar']
    SCHOOL = ['school', 'book_store', 'library']
    PLACE_OF_WORSHIP = ['church', 'hindu_temple', 'mosque']
    ENTERTAINMENT = ['zoo', 'amusement_park', 'aquarium', 'art_gallery',
                     'bowling_alley', 'movie_theater', 'museum',
                     'night_club', 'casino']
    STORE = ['jewelry_store', 'store', 'bicycle_store', 'movie_rental',
             'car_rental', 'pet_store', 'clothing_store',
             'convenience_store', 'department_store', 'shoe_store',
             'shopping_mall', 'furniture_store', 'supermarket',
             'home_goods_store']
    SPORTS = ['bowling_alley', 'gym']
    #TODO no API KEY
    CENTROID_INDEX = 7
    FIVE_MINUTE_SECONDS = 300.0
    NOT_HOME_OR_WORK = 'other'

    def process(self, user, all_days):
        """
        Process GPS data
        """
        location_stream = 'LOCATION--org.md2k.phonesensor--PHONE'
        geofence_list_stream = 'GEOFENCE--LIST--org.md2k.phonesensor--PHONE'
        if self.CC is not None:
            streams = self.CC.get_user_streams(user)
            gps_data_all_streams = []
            gps_groundtruth_data = {}

            for stream_name in streams:
                if geofence_list_stream in stream_name:
                    stream_ids = self.CC.get_stream_id(user, stream_name)
                    for stream_id in stream_ids:
                        geofence_stream_id = stream_id["identifier"]
                        gps_groundtruth_d = self.gps_groundtruth(
                            geofence_stream_id, user, all_days)
                        gps_groundtruth_data.update(gps_groundtruth_d)
                if location_stream in stream_name:
                    stream_ids = self.CC.get_stream_id(user, stream_name)
                    for stream_id in stream_ids:
                        gps_stream_id = stream_id["identifier"]
                        data = self.get_gps(gps_stream_id, user, all_days)
                        if not data:
                            continue
                        gps_data_all_streams.extend(data)

            if len(gps_data_all_streams) < self.GPS_DATA_THRESHOLD:
                self.CC.logging.log('if not gps_data_all_streams and not '
                                    'gps_groundtruth_data ' + str(user))
                return
            else:
                gps_data_admission_controlled = self.gps_admission_control(
                    gps_data_all_streams)
                interpolated_gps_data = self.gps_interpolation(
                    gps_data=gps_data_admission_controlled)
                epoch_id, epoch_centroid, epoch_semantic, epoch_semantic_model, \
                epoch_place_annotation = \
                    self.get_gps_data_format(interpolated_gps_data,
                                             geo_fence_distance=
                                             self.GEO_FENCE_DISTANCE,
                                             min_points_in_cluster
                                             =self.MINIMUM_POINTS_IN_CLUSTER,
                                             max_dist_assign_centroid=
                                             self.GEOFENCE_ASSIGNING_CENTROID,
                                             centroid_name_dict=
                                             gps_groundtruth_data)
                # if gps_groundtruth_data:
                #     self.store_stream(filepath="gps_episodes_and_semantic_location.json",
                #                       input_streams=[streams[location_stream], streams[geofence_list_stream]],
                #                       user_id=user, data=epoch_semantic)
                #     self.store_data("metadata/gps_data_clustering_episode_generation.json", [streams[location_stream],
                #                                                                              streams[geofence_list_stream]],
                #                     user['identifier'],epoch_centroid)
                # else:
                #     self.store_data("metadata/gps_episodes_and_semantic_location.json", [streams[location_stream]],
                #                     user['identifier'],epoch_semantic)
                #     self.store_data("metadata/gps_data_clustering_episode_generation.json", [streams[location_stream]],
                #                     user['identifier'],epoch_centroid)

    def find_interesting_places(self, latitude, longitude, api_key,
                                geofence_radius):
        """
        Obtain the list of interesting places near a coordinate
        :param latitude: float
        :param longitude: float
        :param api_key: string
        :param geofence_radius: float
        :return: 
        """
        google_places = GooglePlaces(api_key)
        places_type_list = [self.RESTAURANT, self.SCHOOL, self.PLACE_OF_WORSHIP,
                            self.ENTERTAINMENT, self.STORE,
                            self.SPORTS]
        return_list = []
        for places_list in places_type_list:
            place_list_length = 0
            for a_place in places_list:
                query_res = google_places.nearby_search(
                    lat_lng={'lat': latitude, 'lng': longitude},
                    keyword=a_place,
                    radius=geofence_radius)
                for place in query_res.places:
                    place_list_length += 1
            if place_list_length:
                return_list.append('yes')
            else:
                return_list.append('no')
        return return_list

    def get_gps(self, gps_stream_id, user_id, all_days):
        """
        Extract all gps data of a given user
        :param all_days:
        :param gps_stream_id: String
        :param user_id: String
        :return: List (of gps datapoints for the that gps_stream_id for that
        user_id)
        """
        data_for_all_days = []
        if gps_stream_id:
            for day in all_days:
                stream = self.CC.get_stream(gps_stream_id, user_id=user_id,
                                            day=day, data_type=DataSet.COMPLETE)
                data_for_a_day = stream.data
                data_for_all_days.append(data_for_a_day)
        extracted_gps_data = []
        if not data_for_all_days:
            extracted_gps_data = []
        else:
            for all_data in data_for_all_days:
                for data in all_data:
                    extracted_gps_data.append(data)
        return extracted_gps_data

    def gps_admission_control(self, gps_data):
        """
         Filter out spurious data
        :param gps_data: List
        :return: List
        """
        gps_data_control = []
        for gps in gps_data:
            if isinstance(gps.sample, list) and len(gps.sample) == 6:
                gps_data_control.append(gps)
            else:
                continue
        return gps_data_control

    def gps_groundtruth(self, geofence_stream_id, user_id, all_days):
        """
         Obtain gps locations marked by users
        :param geofence_stream_id: String
        :param user_id: String
        :return: Dictionary (key - semantic names, values - Corresponding
        co-ordinates)
        """
        data_for_all_days = []
        if geofence_stream_id:
            for day in all_days:
                stream = self.CC.get_stream(geofence_stream_id, user_id=user_id,
                                            day=day, data_type=DataSet.COMPLETE)
                data_for_a_day = stream.data
                data_for_all_days.append(data_for_a_day)

        extracted_semantic_name = {}
        if not data_for_all_days:
            extracted_semantic_name = {}
        else:
            for all_data in data_for_all_days:
                for data in all_data:
                    if isinstance(data.sample, str) and '#' in data.sample:
                        for i in range(1, len(data.sample.split('#')),
                                       self.GROUND_STRING_LENGTH):
                            if len(data.sample.split(
                                    '#')) % self.GROUND_STRING_LENGTH != 0:
                                extracted_semantic_name = {}
                                continue
                            semantic_gps_data = np.array(
                                [float(data.sample.split('#')[i]),
                                 float(data.sample.split('#')[i + 1])])
                            extracted_semantic_name[data.sample.split('#')[
                                i - 1]] = semantic_gps_data
                    else:
                        continue
        return extracted_semantic_name

    def gps_interpolation(self, gps_data):
        """
        Interpolate raw gps data
        :param gps_data: List
        :return: list
        """
        interpolated_data = []
        for i in range(len(gps_data) - 1):
            curr_time_point = gps_data[i].start_time
            curr_sample = gps_data[i].sample
            curr_offset = gps_data[i].offset
            next_time_point = gps_data[i + 1].start_time
            dp = DataPoint(curr_time_point, None, curr_offset, curr_sample)
            interpolated_data.append(dp)
            while ((
                           next_time_point - curr_time_point).total_seconds()
                   / self.IN_SECONDS) > self.INTERPOLATION_TIME:
                new_start_time = curr_time_point + datetime.timedelta(
                    seconds=self.IN_SECONDS)
                new_sample = curr_sample
                new_offset = curr_offset
                dp = DataPoint(new_start_time, None, new_offset, new_sample)
                curr_time_point = new_start_time
                interpolated_data.append(dp)
        return interpolated_data

    def haversine(self, lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        :param lon1: Float
        :param lat1: Float
        :param lon2: Float
        :param lat2: Float
        :return: Float (Distance in km)
        """
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        km = self.KM_PER_RADIAN * c
        return km

    @staticmethod
    def get_centermost_point(cluster):
        centroid = (
        MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
        centermost_point = min(cluster, key=lambda point: great_circle(point,
                                                                       centroid).m)
        return tuple(centermost_point)

    def get_gps_clusters(self, interpolated_gps_data, geo_fence_distance,
                         min_points_in_cluster):
        """
         Computes the clusters
        :param interpolated_gps_data: List
        :param geo_fence_distance: Constant
        :param min_points_in_cluster: Constant
        :return: List of cluster-centroids coordinates
        """
        arr_latitude = []
        arr_longitude = []
        for gps_info in interpolated_gps_data:
            if gps_info.sample[self.ACCURACY] < self.GPS_ACCURACY_THRESHOLD:
                arr_latitude.append(gps_info.sample[self.LATITUDE])
                arr_longitude.append(gps_info.sample[self.LONGITUDE])
            else:
                continue
        dataframe = pd.DataFrame(
            {'Latitude': arr_latitude, 'Longitude': arr_longitude})
        coords = dataframe.as_matrix(columns=['Latitude', 'Longitude'])
        epsilon = geo_fence_distance / (
                self.EPSILON_CONSTANT * self.KM_PER_RADIAN)
        db = DBSCAN(eps=epsilon, min_samples=min_points_in_cluster,
                    algorithm='ball_tree', metric='haversine').fit(
            np.radians(coords))
        cluster_labels = db.labels_
        num_clusters = len(set(cluster_labels))
        clusters = pd.Series(
            [coords[cluster_labels == n] for n in range(-1, num_clusters)])
        clusters = clusters.apply(lambda y: np.nan if len(y) == 0 else y)
        clusters.dropna(how='any', inplace=True)
        centermost_points = clusters.map(self.get_centermost_point)
        centermost_points = np.array(centermost_points)
        all_centroid = []
        for cols in centermost_points:
            cols = np.array(cols)
            cols.flatten()
            cols = ([cols[self.LATITUDE], cols[self.LONGITUDE]])
            all_centroid.append(cols)
        return all_centroid

    def gps_semantic_locations(self, semantic_groundtruth, centorids_coord):
        """
        Assigns semantic name to a centroid
        :param semantic_groundtruth: Dictionary
        :param centorids_coord: List
        :return: List of centroids with their corresponding semantic names, if
        found
        """
        candidate_names = []
        semantic_name_list = []
        centroid_index = 0
        for key, value in semantic_groundtruth.items():
            candidate_names.append(key)
        for i in centorids_coord:
            semantic_name_list.append(
                [i[self.LATITUDE], i[self.LONGITUDE], self.UNDEFINED])
            dist = []
            all_index = []
            for key, value in semantic_groundtruth.items():
                dist.append(
                    self.haversine(i[self.LONGITUDE], i[self.LATITUDE],
                                   value[self.LONGITUDE], value[self.LATITUDE]))
            for j in dist:
                if j < self.GEOFENCE_ASSIGNING_CENTROID / (
                        2 * self.GEOFENCE_ASSIGNING_CENTROID):
                    all_index.append(dist.index(j))
            for index in all_index:
                semantic_name_list[centroid_index][2] = candidate_names[index]
            centroid_index += 1
        return semantic_name_list

    def get_centroid(self, centroids, c_name, lat, long,
                     max_dist_assign_centroid):
        """
         Obtain the nearest centroid and semantic name for a given location
         co-ordinate
        :param centroids: List
        :param c_name: List
        :param lat: Float
        :param long: Float
        :param max_dist_assign_centroid: Constant
        :return: List of centroid and semantic name assigned to the given GPS
        datapoint
        """
        dist = []
        gps_centroids = [(c[self.LATITUDE], c[self.LONGITUDE]) for c in
                         centroids]
        for i in gps_centroids:
            dist.append(
                self.haversine(long, lat, i[self.LONGITUDE], i[self.LATITUDE]))
        min_dist = np.min(dist)
        if min_dist < max_dist_assign_centroid / \
                self.GEOFENCE_ASSIGNING_CENTROID:
            index = dist.index(min_dist)
            gps_pt_name = [gps_centroids[index], c_name[index], index]
            return list(gps_pt_name)
        else:
            return list([(-1, -1), self.UNDEFINED, -1])

    def utc_unix_time(self, strinddate):
        s = strinddate[:19]
        d = strinddate[-5:-3]
        utc = datetime.datetime.strptime(s, self.DATE_FORMAT)
        unix_t = utc.timestamp() * 1000 - int(d) * 60 * 60000
        return unix_t

    def getHourOfDay(self, timestamp):
        """
        Get Time of day
        :param timestamp: unixtimestamp
        :return:
        """
        tm = time.localtime(timestamp / 1000)
        hourOfDay = tm.tm_hour + tm.tm_min / 60.0 + tm.tm_sec / 3600.0
        return hourOfDay

    def getDayOfWeek(self, timestamp):
        """
        Get Day of Week
        :param timestamp: unixtimestamp
        :return:
        """
        # """day of week, range [0, 6], Monday is 0"""
        tm = time.localtime(timestamp / 1000)
        return tm.tm_wday

    def getFeatures(self, timestampEntry, timestampExit):
        """
        Compute features for semantic assignment model
        :param timestampEntry: unixtimestamp
        :param timestampExit: unixtimestamp
        :return:
        """
        featuresM = []
        hodEntry = self.getHourOfDay(timestampEntry)
        hodExit = self.getHourOfDay(timestampExit)
        dow = self.getDayOfWeek(timestampEntry)
        featuresM.append(dow)
        featuresM.append(hodEntry)
        featuresM.append(hodExit)
        featuresM.append((timestampExit - timestampEntry) / (60 * 60000))
        featuresM.append((timestampExit - timestampEntry) / (2 * 60 * 60000))
        return featuresM

    def predictLabel(self, timestampEntry, timestampExit):
        """
        Predict label from model
        :param timestampEntry: unixtimestamp
        :param timestampExit: unixtimestamp
        :return:
        """
        modelFilePath = self.MODEL_FILE_PATH
        model = pickle.load(open(modelFilePath, 'rb'))
        featuresM = self.getFeatures(timestampEntry, timestampExit)
        featuresM = np.vstack((featuresM, featuresM))
        result = model.predict(featuresM)
        result = result[0]
        return result

    def get_gps_data_format(self, interpolated_gps_data, geo_fence_distance,
                            min_points_in_cluster,
                            max_dist_assign_centroid,
                            centroid_name_dict):
        """

        :param interpolated_gps_data: List
        :param geo_fence_distance: Constant
        :param min_points_in_cluster: Constant
        :param max_dist_assign_centroid: Constant
        :param centroid_name_dict: Dictionary
        :return: None
        """
        gps_data = []
        gps_epoch_with_centroid = []
        gps_epoch_with_centroid_index = []
        gps_epoch_with_semantic_location = []
        gps_epoch_with_place_annotation = []
        gps_epoch_model_semantic_location = []

        centroid_location = self.get_gps_clusters(interpolated_gps_data,
                                                  geo_fence_distance,
                                                  min_points_in_cluster)
        gps_datapoints = [(dp.sample[self.LATITUDE], dp.sample[self.LONGITUDE],
                           dp.start_time, dp.offset) for dp in
                          interpolated_gps_data
                          if dp.sample[
                              self.ACCURACY] < self.GPS_ACCURACY_THRESHOLD]

        sem_names = self.gps_semantic_locations(centroid_name_dict,
                                                centroid_location)
        semantic_names_arr = (
        [sem_names[i][2] for i in range(len(centroid_location))])

        for i in gps_datapoints:
            assign_centroid = self.get_centroid(centroid_location,
                                                semantic_names_arr,
                                                i[self.LATITUDE],
                                                i[self.LONGITUDE],
                                                max_dist_assign_centroid)
            gps_data.append([i[2], i[self.LATITUDE], i[self.LONGITUDE],
                             assign_centroid[0][self.LATITUDE],
                             assign_centroid[0][self.LONGITUDE],
                             assign_centroid[1], i[self.OFFSET],
                             assign_centroid[2]])

        # If we have user marked location
        if centroid_name_dict:
            start_date = gps_data[0][0]
            for i in range(len(gps_data) - 1):
                if self.haversine(gps_data[i][self.CENTROID_LONGITUDE],
                                  gps_data[i][self.CENTROID_LATITUDE],
                                  gps_data[i + 1][self.CENTROID_LONGITUDE],
                                  gps_data[i + 1][
                                      self.CENTROID_LATITUDE]) <= \
                        self.EPOCH_THRESHOLD:
                    continue
                else:
                    end_date = gps_data[i][0]
                    sample_semantic_names = [
                        gps_data[i][self.SEMANTIC_NAMES_INDEX]]
                    dp_semantic_location = DataPoint(start_date, end_date,
                                                     gps_data[i][
                                                         self.OFFSET_INDEX],
                                                     sample_semantic_names)
                    gps_epoch_with_semantic_location.append(
                        dp_semantic_location)
                    start_date = gps_data[i + 1][0]

        # Semantic location from model
        m_start_date = gps_data[0][0]
        for i in range(len(gps_data) - 1):
            if self.haversine(gps_data[i][self.CENTROID_LONGITUDE],
                              gps_data[i][self.CENTROID_LATITUDE],
                              gps_data[i + 1][self.CENTROID_LONGITUDE],
                              gps_data[i + 1][
                                  self.CENTROID_LATITUDE]) <= \
                    self.EPOCH_THRESHOLD:
                continue
            else:
                m_end_date = gps_data[i][0]
                sample_centroid_index = [gps_data[i][self.CENTROID_INDEX]]
                sample_centroid = [gps_data[i][self.CENTROID_INDEX],
                                   gps_data[i][self.CENTROID_LATITUDE],
                                   gps_data[i][self.CENTROID_LONGITUDE]]
                dp_centroid = DataPoint(m_start_date, m_end_date,
                                        gps_data[i][self.OFFSET_INDEX],
                                        sample_centroid)
                dp_centroid_index = DataPoint(m_start_date, m_end_date,
                                              gps_data[i][self.OFFSET_INDEX],
                                              sample_centroid_index)
                gps_epoch_with_centroid_index.append(dp_centroid_index)
                gps_epoch_with_centroid.append(dp_centroid)
                if (
                        m_end_date - m_start_date).total_seconds() > \
                        self.FIVE_MINUTE_SECONDS:
                    if gps_data[i][self.CENTROID_LATITUDE] == -1 and \
                            gps_data[i][self.CENTROID_LONGITUDE] == -1:
                        sample_semantic_names = self.UNDEFINED
                    else:
                        sample_semantic_names = self.predictLabel(
                            self.utc_unix_time(str(m_start_date)),
                            self.utc_unix_time(str(m_end_date)))
                        if sample_semantic_names == self.NOT_HOME_OR_WORK:
                            sample_semantic_names = self.UNDEFINED
                    dp_semantic_location = DataPoint(m_start_date, m_end_date,
                                                     gps_data[i][
                                                         self.OFFSET_INDEX],
                                                     sample_semantic_names)
                    gps_epoch_model_semantic_location.append(
                        dp_semantic_location)
                    m_start_date = gps_data[i + 1][0]
                else:
                    m_start_date = gps_data[i + 1][0]

        n_start_date = gps_datapoints[0][2]
        for i in range(len(gps_datapoints[:1000]) - 1):
            if self.haversine(gps_datapoints[i][self.LONGITUDE],
                              gps_datapoints[i][self.LATITUDE],
                              gps_datapoints[i + 1][self.LONGITUDE],
                              gps_datapoints[i + 1][
                                  self.LATITUDE]) <= self.EPOCH_THRESHOLD:
                continue
            else:
                n_end_date = gps_datapoints[i][2]
                if (
                        n_end_date - n_start_date).total_seconds() > \
                        self.FIVE_MINUTE_SECONDS:
                    sample_centroid = self.find_interesting_places(
                        gps_datapoints[i][self.LATITUDE],
                        gps_datapoints[i][self.LONGITUDE], self.API_KEY,
                        self.MINIMUM_POINTS_IN_CLUSTER)
                    dp_centroid = DataPoint(n_start_date, n_end_date,
                                            gps_datapoints[i][3],
                                            sample_centroid)
                    gps_epoch_with_place_annotation.append(dp_centroid)
                    n_start_date = gps_datapoints[i + 1][2]
                else:
                    n_start_date = gps_datapoints[i + 1][2]
        print(gps_epoch_with_centroid_index[:50])
        print(gps_epoch_with_centroid[:50])
        print(gps_epoch_with_semantic_location[:50])
        print(gps_epoch_model_semantic_location[:50])
        print(gps_epoch_with_place_annotation[:50])
        return gps_epoch_with_centroid_index, gps_epoch_with_centroid, \
               gps_epoch_with_semantic_location, \
               gps_epoch_model_semantic_location, \
               gps_epoch_with_place_annotation

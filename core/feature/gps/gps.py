import uuid
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
import numpy as np, pandas as pd
import os
from math import radians, cos, sin, asin, sqrt
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry.multipoint import MultiPoint
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.core.datatypes.datapoint import DataPoint
import datetime
from core.computefeature import ComputeFeatureBase

feature_class_name = 'GPSClusteringEpochComputation'

class GPSClusteringEpochComputation(ComputeFeatureBase):
    INTERPOLATION_TIME = 1.0
    KM_PER_RADIAN = 6371.0088
    EPSILON_CONSTANT = 1000
    GPS_ACCURACY_THRESHOLD = 41
    GEO_FENCE_DISTANCE = 2
    MINIMUM_POINTS_IN_CLUSTER = 500
    GEOFENCE_ASSIGNING_CENTROID = 1000
    LATITUDE = 0
    LONGITUDE = 1
    ACCURACY = -1
    OFFSET = -1
    GPS_SAMPLE_LENGTH = 6
    IN_SECONDS = 60
    SEMANTIC_NAMES_INDEX = 5
    CENTROID_LATITUDE = 3
    CENTROID_LONGITUDE = 4
    OFFSET_INDEX = 6
    GROUND_STRING_LENGTH = 3
    UNDEFINED = 'UNDEFINED'

    def process(self, user, all_days):
        """
        Process GPS data
        """
        location_stream = 'LOCATION--org.md2k.phonesensor--PHONE'
        geofence_list_stream = 'GEOFENCE--LIST--org.md2k.phonesensor--PHONE'

        if self.CC is not None:
            #self.CC.logging.log('GPS processing %s ' %(str(user)))
            #users = [{'username': 'mperf_9040', 'identifier': '397c6457-0954-4cd2-995c-2fbeb6c72097'}]
            streams = self.CC.get_user_streams(user)
            gps_data_all_streams = []
            gps_groundtruth_data = {}

            for stream_name in streams:
                if geofence_list_stream in stream_name:
                    stream_ids = self.CC.get_stream_id(user, stream_name)
                    for stream_id in stream_ids:
                        geofence_stream_id = stream_id["identifier"]
                        gps_groundtruth_d = self.gps_groundtruth(geofence_stream_id, user, all_days)
                        gps_groundtruth_data.update(gps_groundtruth_d)
                if location_stream in stream_name:
                    stream_ids = self.CC.get_stream_id(user, stream_name)
                    for stream_id in stream_ids:
                        gps_stream_id = stream_id["identifier"]
                        data = self.get_gps(gps_stream_id, user, all_days)
                        gps_data_all_streams.extend(data)

            if not gps_groundtruth_data:
                return
            print("Ground Truth Data Found",user)

            gps_data_admission_controlled = self.gps_admission_control(gps_data_all_streams)
            interpolated_gps_data = self.gps_interpolation(gps_data=gps_data_admission_controlled)
            epoch_centroid, epoch_semantic = \
                self.get_gps_data_format(interpolated_gps_data,
                                         geo_fence_distance=self.GEO_FENCE_DISTANCE,
                                         min_points_in_cluster=self.MINIMUM_POINTS_IN_CLUSTER,
                                         max_dist_assign_centroid=self.GEOFENCE_ASSIGNING_CENTROID,
                                         centroid_name_dict=gps_groundtruth_data)

            print("gps_data_clustering:",len(epoch_centroid))
            print("gps_semantic:",len(epoch_semantic))

            self.store_data("metadata/gps_data_clustering_episode_generation.json", [streams[location_stream],
                             streams[geofence_list_stream]], user, epoch_centroid)
            self.store_data("metadata/gps_episodes_and_semantic_location.json", [streams[location_stream],
                             streams[geofence_list_stream]], user, epoch_semantic)


    def store_data(self, filepath, input_streams, user_id, data):
        output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(filepath + user_id + "GPS Clustering")))
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        newfilepath = os.path.join(cur_dir, filepath)
        with open(newfilepath, "r") as f:
            metadata = f.read()
            metadata = metadata.replace("CC_INPUT_STREAM_ID_CC", input_streams[0]["identifier"])
            metadata = metadata.replace("CC_INPUT_STREAM_NAME_CC", input_streams[0]["name"])
            metadata = metadata.replace("CC_OUTPUT_STREAM_IDENTIFIER_CC", output_stream_id)
            metadata = metadata.replace("CC_OWNER_CC", user_id)
            metadata = pd.json.loads(metadata)
            if len(data) > 0:
                self.store(identifier=output_stream_id, owner=user_id, name=metadata["name"],
                           data_descriptor=metadata["data_descriptor"],
                           execution_context=metadata["execution_context"], annotations=metadata["annotations"],
                           stream_type="datastream", data=data)

    def get_gps(self, gps_stream_id, user_id, all_days):
        """
        Extract all gps data of a given user
        :param gps_stream_id: String
        :param user_id: String
        :return: List (of gps datapoints for the that gps_stream_id for that user_id)
        """
        data_for_all_days = []
        if gps_stream_id:
            for day in all_days:
                stream = self.CC.get_stream(gps_stream_id, user_id=user_id, day=day, data_type=DataSet.COMPLETE)
                data_for_a_day = stream.data
                data_for_all_days.append(data_for_a_day)
        extracted_gps_data = []
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
        for gps in gps_data:
            if not isinstance(gps.sample, list):
                gps_data.remove(gps)
            else:
                continue
        return gps_data

    def gps_groundtruth(self, geofence_stream_id, user_id, all_days):
        """
         Obtain gps locations marked by users
        :param geofence_stream_id: String
        :param user_id: String
        :return: Dictionary (key - semantic names, values - Corresponding co-ordinates)
        """
        data_for_all_days = []
        if geofence_stream_id:
            for day in all_days:
                stream = self.CC.get_stream(geofence_stream_id, 
                                                user_id=user_id, day=day,
                                                data_type=DataSet.COMPLETE)
                data_for_a_day = stream.data
                data_for_all_days.append(data_for_a_day)
        extracted_semantic_name = {}
        for all_data in data_for_all_days:
            for data in all_data:
                if isinstance(data.sample, str) and '#' in data.sample:
                    for i in range(1, len(data.sample.split('#')), self.GROUND_STRING_LENGTH):
                        semantic_gps_data = np.array(
                            [float(data.sample.split('#')[i]), 
                             float(data.sample.split('#')[i + 1])])
                        extracted_semantic_name[data.sample.split('#')[i - 1]] = semantic_gps_data
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
            while ((next_time_point - curr_time_point).total_seconds() / self.IN_SECONDS) > self.INTERPOLATION_TIME:
                new_start_time = curr_time_point + datetime.timedelta(seconds=self.IN_SECONDS)
                new_sample = curr_sample
                new_offset = curr_offset
                dp = DataPoint(new_start_time, None, new_offset, new_sample)
                curr_time_point = new_start_time
                interpolated_data.append(dp)
        return interpolated_data

    def haversine(self, first_longitude, first_latitude, second_longitude, second_latitude):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        :param first_longitude: Float
        :param first_latitude: Float
        :param second_longitude: Float
        :param second_latitude: Float
        :return: Float (Distance in km)
        """
        # convert decimal degrees to radians
        first_longitude, first_latitude, second_longitude, second_latitude = \
            map(radians, [first_longitude, first_latitude, second_longitude, second_latitude])
        # haversine formula
        dlon = second_longitude - first_longitude
        dlat = second_latitude - first_latitude
        a = sin(dlat / 2) ** 2 + cos(first_latitude) * cos(second_latitude) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        km = self.KM_PER_RADIAN * c
        return km

    @staticmethod
    def get_centermost_point(cluster):
        centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
        centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
        return tuple(centermost_point)

    def get_gps_clusters(self, interpolated_gps_data, geo_fence_distance, min_points_in_cluster):
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
            if gps_info.sample[-1] < self.GPS_ACCURACY_THRESHOLD:
                arr_latitude.append(gps_info.sample[0])
                arr_longitude.append(gps_info.sample[1])
            else:
                continue
        dataframe = pd.DataFrame({'Latitude': arr_latitude, 'Longitude': arr_longitude})
        coords = dataframe.as_matrix(columns=['Latitude', 'Longitude'])
        epsilon = geo_fence_distance / (self.EPSILON_CONSTANT * self.KM_PER_RADIAN)
        db = DBSCAN(eps=epsilon, min_samples=min_points_in_cluster, algorithm='ball_tree', metric='haversine').fit(
            np.radians(coords))
        cluster_labels = db.labels_
        num_clusters = len(set(cluster_labels))
        clusters = pd.Series([coords[cluster_labels == n] for n in range(-1, num_clusters)])
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
        :return: List of centroids with their corresponding semantic names, if found
        """
        candidate_names = []
        semantic_name_list = []
        centroid_index = 0
        for key, value in semantic_groundtruth.items():
            candidate_names.append(key)
        for i in centorids_coord:
            semantic_name_list.append([i[self.LATITUDE], i[self.LONGITUDE], self.UNDEFINED])
            dist = []
            all_index = []
            for key, value in semantic_groundtruth.items():
                dist.append(
                    self.haversine(i[self.LONGITUDE], i[self.LATITUDE], value[self.LONGITUDE], value[self.LATITUDE]))
            for j in dist:
                if j < self.GEOFENCE_ASSIGNING_CENTROID / (2 * self.GEOFENCE_ASSIGNING_CENTROID):
                    all_index.append(dist.index(j))
            for index in all_index:
                semantic_name_list[centroid_index][2] = candidate_names[index]
            centroid_index += 1
        return semantic_name_list

    def get_centroid(self, centroids, c_name, lat, long, max_dist_assign_centroid):
        """
         Obtain the nearest centroid and semantic name for a given location co-ordinate
        :param centroids: List
        :param c_name: List
        :param lat: Float
        :param long: Float
        :param max_dist_assign_centroid: Constant
        :return: List of centroid and semantic name assigned to the given GPS datapoint
        """
        dist = []
        gps_centroids = [(c[self.LATITUDE], c[self.LONGITUDE]) for c in centroids]
        for i in gps_centroids:
            dist.append(self.haversine(long, lat, i[self.LONGITUDE], i[self.LATITUDE]))
        min_dist = np.min(dist)
        if min_dist < max_dist_assign_centroid / self.GEOFENCE_ASSIGNING_CENTROID:
            index = dist.index(min_dist)
            gps_pt_name = [gps_centroids[index], c_name[index]]
            return list(gps_pt_name)
        else:
            return list([(-1, -1), self.UNDEFINED])

    def get_gps_data_format(self, interpolated_gps_data, geo_fence_distance, min_points_in_cluster,
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
        gps_epoch_with_semantic_location = []
        centroid_location = self.get_gps_clusters(interpolated_gps_data, geo_fence_distance, min_points_in_cluster)
        gps_datapoints = [(dp.sample[self.LATITUDE], dp.sample[self.LONGITUDE], dp.start_time, dp.offset) for dp in
                          interpolated_gps_data
                          if dp.sample[self.ACCURACY] < self.GPS_ACCURACY_THRESHOLD]
        sem_names = self.gps_semantic_locations(centroid_name_dict, centroid_location)
        semantic_names_arr = ([sem_names[i][2] for i in range(len(centroid_location))])
        for i in gps_datapoints:
            assign_centroid = self.get_centroid(centroid_location, semantic_names_arr, i[self.LATITUDE],
                                                i[self.LONGITUDE],
                                                max_dist_assign_centroid)
            gps_data.append([i[2], i[self.LATITUDE], i[self.LONGITUDE], assign_centroid[0][self.LATITUDE],
                             assign_centroid[0][self.LONGITUDE],
                             assign_centroid[1], i[self.OFFSET]])
        start_date = gps_data[0][0]
        gps_epoch_with_centroid.append(DataPoint(start_date, None, gps_data[0][self.OFFSET_INDEX],
                                                 [gps_data[0][self.CENTROID_LATITUDE],
                                                  gps_data[0][self.CENTROID_LONGITUDE]]))
        gps_epoch_with_semantic_location.append(DataPoint(start_date, None, gps_data[0][self.OFFSET_INDEX],
                                                          [gps_data[0][self.SEMANTIC_NAMES_INDEX]]))
        for i in range(len(gps_data) - 1):
            if (gps_data[i + 1][self.CENTROID_LATITUDE] - gps_data[i][self.CENTROID_LATITUDE] == 0.0 and
                    gps_data[i + 1][self.CENTROID_LONGITUDE] - gps_data[i][self.CENTROID_LONGITUDE] == 0.0):
                continue
            else:
                end_date = gps_data[i][0]
                sample_centroid = [gps_data[i][self.CENTROID_LATITUDE], gps_data[i][self.CENTROID_LONGITUDE]]
                sample_semantic_names = [gps_data[i][self.SEMANTIC_NAMES_INDEX]]
                dp_centroid = DataPoint(start_date, end_date, gps_data[i][self.OFFSET_INDEX], sample_centroid)
                gps_epoch_with_centroid.append(dp_centroid)
                dp_semantic_location = DataPoint(start_date, end_date, gps_data[i][self.OFFSET_INDEX],
                                                 sample_semantic_names)
                gps_epoch_with_semantic_location.append(dp_semantic_location)
                start_date = gps_data[i + 1][0]
        return gps_epoch_with_centroid, gps_epoch_with_semantic_location

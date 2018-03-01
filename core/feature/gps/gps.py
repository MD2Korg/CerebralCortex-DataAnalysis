import pandas as pd, numpy as np
import time
import os
import json
import uuid
from math import radians, cos, sin, asin, sqrt
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry.multipoint import MultiPoint
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.core.datatypes.datapoint import DataPoint
import datetime
# from core.feature.gps.computefeature import ComputeFeatureBase
from core.computefeature import ComputeFeatureBase
from core.feature.gps.gps_groundtruth import gps_gt

feature_class_name = 'GPSClusteringEpochComputation'


class GPSClusteringEpochComputation(ComputeFeatureBase):
    def process(self):
        if self.CC is not None:
            print("Processing PhoneFeatures")
            self.all_users_data("mperf")

    def all_users_data(self, study_name, CC, spark_context):
        users = CC.get_all_users(study_name)
        for user in users:
            self.process_data(user["identifier"], CC)

    def process_data(self, user_id, CC):
        streams = CC.get_user_streams(user_id)
        if streams and len(streams) > 0:
            # print (streams)
            self.analyze_gps(streams, user_id, CC)

    def analyze_gps(self, streams, user_id, CC):
        if "LOCATION--org.md2k.phonesensor--PHONE" in streams:
            gps_stream_id = streams["LOCATION--org.md2k.phonesensor--PHONE"][
                "identifier"]
            # gps_stream_id = 'a1402e7c-d761-3814-b989-bad282f8bec3'
            gps_stream_name = streams["LOCATION--org.md2k.phonesensor--PHONE"]["name"]
        else:
            gps_stream_id = None
        all_day_data = []
        if gps_stream_id:
            stream_end_days = CC.get_stream_duration(gps_stream_id)
            if stream_end_days["start_time"] and stream_end_days["end_time"]:
                days = stream_end_days["end_time"] - stream_end_days["start_time"]
                for day in range(days.days + 1):
                    day = (stream_end_days["start_time"] + datetime.timedelta(days=day)).strftime('%Y%m%d')
                    stream = CC.get_stream(gps_stream_id, user_id=user_id, day=day, data_type=DataSet.COMPLETE)
                    only_data = stream.data
                    all_day_data.append(only_data)
        all_gps = []
        for a in all_day_data:
            for aa in a:
                all_gps.append(aa)

        cent_name = gps_gt(streams, user_id, CC)
        self.get_gps_data_format(all_gps, geo_fence_dist=100, min_points_in_cluster=500, max_dist_assign_centroid=1000,
                                 centroid_name_dict=cent_name)

    def haversine(lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
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

    def get_gps_points(c_lat, c_long):
        lat = c_lat
        long = c_long
        gps_pt = zip(lat, long)
        return list(gps_pt)

    def get_gps_triplet(in_lat, in_long, in_time, in_offset):
        lat = in_lat
        long = in_long
        gps_time = in_time
        time_offset = in_offset
        gps_pt = zip(gps_time, lat, long, time_offset)
        return list(gps_pt)

    def get_centroid(self, c_lat, c_long, c_name, lat, long, max_dist_assign_centroid):
        dist = []
        gps_c = self.get_gps_points(c_lat, c_long)

        for i in gps_c:
            dist.append(self.haversine(long, lat, i[1], i[0]))

        min_dist = np.min(dist)
        if min_dist < max_dist_assign_centroid/1000:
            index = dist.index(min_dist)
            gps_pt_name = [gps_c[index], c_name[index]]
            return list(gps_pt_name)
        else:
            return list([(-1, -1), '-1'])

    def get_centermost_point(self, cluster):
        centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
        centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
        return tuple(centermost_point)

    def gps_interpolation(self, all_data):
        interpolated_data = []
        for i in range(len(all_data) - 1):
            curr_time_point = all_data[i].start_time
            curr_sample = all_data[i].sample
            curr_offset = all_data[i].offset
            next_time_point = all_data[i + 1].start_time
            dp = DataPoint(curr_time_point, None, curr_offset, curr_sample)
            interpolated_data.append(dp)
            while ((next_time_point - curr_time_point).total_seconds() / 60) > 1.0:
                new_start_time = curr_time_point + datetime.timedelta(seconds=60)
                new_sample = curr_sample
                new_offset = curr_offset
                dp = DataPoint(new_start_time, None, new_offset, new_sample)
                curr_time_point = new_start_time
                interpolated_data.append(dp)
        return interpolated_data

    def get_gps_clusters(self, all_data, geo_fence_dist, min_points_in_cluster):
        kms_per_radian = 6371.0088
        interpolated_data = self.gps_interpolation(all_data)
        arr_lat = []
        arr_long = []

        count = 0
        for gps_info in interpolated_data:
            if gps_info.sample[-1] < 41:
                count = count + 1
                arr_lat.append(gps_info.sample[0])
                arr_long.append(gps_info.sample[1])
            else:
                count = count + 1
        df = pd.DataFrame({'Latitude': arr_lat, 'Longitude': arr_long})
        coords = df.as_matrix(columns=['Latitude', 'Longitude'])
        epsilon = geo_fence_dist / (1000 * kms_per_radian)
        db = DBSCAN(eps=epsilon, min_samples=min_points_in_cluster, algorithm='ball_tree', metric='haversine').fit(
            np.radians(coords))
        cluster_labels = db.labels_
        num_clusters = len(set(cluster_labels))
        clusters = pd.Series([coords[cluster_labels == n] for n in range(-1, num_clusters)])
        clusters = clusters.apply(lambda y: np.nan if len(y) == 0 else y)
        clusters.dropna(how='any', inplace=True)

        centermost_points = clusters.map(self.get_centermost_point)

        centermost_points = np.array(centermost_points)
        allc = []
        for cols in centermost_points:
            cols = np.array(cols)
            cols.flatten()
            cols = ([cols[0], cols[1]])
            allc.append(cols)
        return allc

    def gps_semantic_locations(self, semantic_groundtruth, centorids_coord):
        cen_all_gps = []
        cen_name = []
        semantic_name_list = []
        all_index = []
        for i in centorids_coord:
            semantic_name_list.append([i[0], i[1], '-1'])

        for key, value in semantic_groundtruth.items():
            cen_all_gps.append([value[0], value[1]])
            cen_name.append(key)

        for key, value in semantic_groundtruth.items():
            dist = []
            for i in centorids_coord:
                dist.append(self.haversine(i[1], i[0], value[1], value[0]))
            for i in dist:
                if i < 0.5:
                    all_index.append(dist.index(i))
            for aa in all_index:
                semantic_name_list[aa][2] = key
        return semantic_name_list

    def store_data(self, filepath, input_streams, user_id, data):
        output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(filepath + user_id + "GPS Clustering")))
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        newfilepath = os.path.join(cur_dir, filepath)
        with open(newfilepath, "r") as f:
            metadata = f.read()
            metadata = metadata.replace("CC_INPUT_STREAM_ID_CC", input_streams[0]["id"])
            metadata = metadata.replace("CC_INPUT_STREAM_NAME_CC", input_streams[0]["name"])
            metadata = metadata.replace("CC_OUTPUT_STREAM_IDENTIFIER_CC", output_stream_id)
            metadata = metadata.replace("CC_OWNER_CC", user_id)
            metadata = json.loads(metadata)

            self.CC.store(identifier=output_stream_id, owner=user_id, name=metadata["name"],
                          data_descriptor=metadata["data_descriptor"],
                          execution_context=metadata["execution_context"], annotations=metadata["annotations"],
                          stream_type="datastream", data=data)

    def get_gps_data_format(self, all_data, geo_fence_dist, min_points_in_cluster, max_dist_assign_centroid,
                            centroid_name_dict):
        interpolated_data = self.gps_interpolation(all_data)
        loc_c = self.get_gps_clusters(all_data, geo_fence_dist, min_points_in_cluster)  # th1==500, th2==500
        arr_lat = []
        arr_long = []
        arr_time = []
        arr_offset = []
        gps_data = []
        c_arr_lat = []
        c_arr_long = []
        c_arr_names = []
        data = []
        count = 0
        for gps_info in interpolated_data:
            if gps_info.sample[-1] < 41:
                count = count + 1
                arr_lat.append(gps_info.sample[0])
                arr_long.append(gps_info.sample[1])
                arr_time.append(gps_info.start_time)
                arr_offset.append(gps_info.offset)
            else:
                count = count + 1
        gps_p = self.get_gps_triplet(arr_lat, arr_long, arr_time, arr_offset)
        sem_names = self.gps_semantic_locations(centroid_name_dict, loc_c)
        for i in range(len(loc_c)):
            c_arr_lat.append(loc_c[i][0])
            c_arr_long.append(loc_c[i][1])
            c_arr_names.append(sem_names[i][2])

        for i in gps_p:
            assign_centroid = self.get_centroid(c_arr_lat, c_arr_long, c_arr_names, i[1], i[2],
                                                max_dist_assign_centroid)  # th3==1
            gps_data.append([i[0], i[1], i[2], assign_centroid[0][0], assign_centroid[0][1], assign_centroid[1], i[-1]])
        start_date = gps_data[0][0]
        for i in range(len(gps_data) - 1):
            if gps_data[i + 1][3] - gps_data[i][3] == 0.0 and gps_data[i + 1][4] - gps_data[i][4] == 0.0:
                continue
            else:
                end_date = gps_data[i][0]
                sample = [gps_data[i][5]]
                dp = DataPoint(start_date, end_date, gps_data[i][4], gps_data[i][5], gps_data[i][6], sample)
                data.append(dp)
                start_date = gps_data[i + 1][0]

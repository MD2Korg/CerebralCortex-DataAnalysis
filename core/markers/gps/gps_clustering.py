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
        analyze_gps(streams, user_id, CC)


def analyze_gps(streams, user_id, CC):
    if "LOCATION--org.md2k.phonesensor--PHONE" in streams:
        # gps_stream_id = streams["LOCATION--org.md2k.phonesensor--PHONE"][
        #     "identifier"]
        gps_stream_id = 'a1402e7c-d761-3814-b989-bad282f8bec3'
        print(gps_stream_id)
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
                stream = CC.get_stream(gps_stream_id, day=day, data_type=DataSet.COMPLETE)
                only_data = stream.data
                all_day_data.append(only_data)
    all_gps = []
    for a in all_day_data:
        for aa in a:
            all_gps.append(aa)
    get_gps_data_format(all_gps, geo_fence_dist=500, min_points_in_cluster=500, max_dist_assign_centroid=1)


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


def get_centroid(c_lat, c_long, lat, long, max_dist_assign_centroid):
    dist = []
    gps_c = get_gps_points(c_lat, c_long)

    for i in gps_c:
        dist.append(haversine(long, lat, i[1], i[0]))

    min_dist = np.min(dist)
    if min_dist < max_dist_assign_centroid:
        index = dist.index(min_dist)
        return gps_c[index]
    else:
        return -1, -1


def get_centermost_point(cluster):
    centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
    centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
    return tuple(centermost_point)


def gps_interpolation(all_data):
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


def get_gps_clusters(all_data, geo_fence_dist, min_points_in_cluster):
    kms_per_radian = 6371.0088
    interpolated_data = gps_interpolation(all_data)
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

    start_time = time.time()
    db = DBSCAN(eps=epsilon, min_samples=min_points_in_cluster, algorithm='ball_tree', metric='haversine').fit(
        np.radians(coords))
    cluster_labels = db.labels_

    num_clusters = len(set(cluster_labels))
    clusters = pd.Series([coords[cluster_labels == n] for n in range(-1, num_clusters)])
    clusters = clusters.apply(lambda y: np.nan if len(y) == 0 else y)
    clusters.dropna(how='any', inplace=True)

    centermost_points = clusters.map(get_centermost_point)

    centermost_points = np.array(centermost_points)
    allc = []
    for cols in centermost_points:
        cols = np.array(cols)
        cols.flatten()
        cols = ([cols[0], cols[1]])
        allc.append(cols)
    return allc


def get_gps_data_format(all_data, geo_fence_dist, min_points_in_cluster, max_dist_assign_centroid):
    interpolated_data = gps_interpolation(all_data)
    loc_c = get_gps_clusters(all_data, geo_fence_dist, min_points_in_cluster)  # th1==500, th2==500
    arr_lat = []
    arr_long = []
    arr_time = []
    arr_offset = []
    gps_data = []
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
    gps_p = get_gps_triplet(arr_lat, arr_long, arr_time, arr_offset)
    c_arr_lat = []
    c_arr_long = []

    for i in loc_c:
        c_arr_lat.append(i[0])
        c_arr_long.append(i[1])
    for i in gps_p:
        assign_centroid = get_centroid(c_arr_lat, c_arr_long, i[1], i[2], max_dist_assign_centroid)  # th3==1
        gps_data.append([i[0], i[1], i[2], assign_centroid[0], assign_centroid[1], i[-1]])
    start_date = gps_data[0][0]
    for i in range(len(gps_data) - 1):
        if gps_data[i + 1][3] - gps_data[i][3] == 0.0 and gps_data[i + 1][4] - gps_data[i][4] == 0.0:
            continue
        else:
            end_date = gps_data[i][0]
            sample = [gps_data[i][3], gps_data[i][4]]
            dp = DataPoint(start_date, end_date, gps_data[i][5], sample)
            data.append(dp)
            start_date = gps_data[i + 1][0]
    print(data)
    return {"metadata": "gps_data_clustering_episode_generation", "data": data}



from core.signalprocessing.vector import magnitude, normalize, window_std_dev
from core.signalprocessing.window import window_sliding


def activity_marker(attachment_marker_stream_id: uuid, mshrv_accel_id: uuid, mshrv_gyro_id: uuid, wrist: str,
                    owner_id: uuid, dd_stream_name, CC: CerebralCortex, config: dict):

    mshrv_accel_stream = CC.get_stream(mshrv_accel_id, day,
                                       start_time=marker_window.start_time,
                                       end_time=marker_window.end_time, data_type=DataSet.ONLY_DATA)

    accel_features = accelerometer_features(mshrv_accel_stream, 10)
    feature_vector = generate_activity_feature_vector(accel_features)

def accelerometer_features(accel: DataStream,
                           window_size: float = 10.0) -> Tuple[DataStream, DataStream, DataStream]:

    accelerometer_magnitude = magnitude(accel)

    # perform windowing of datastream
    window_data = window_sliding(accelerometer_magnitude.data, window_size, window_offset)

    variance_data = []
    mean_data = []
    median_data = []
    quartile_deviation_data = []

    for key, value in window_data.items():
        starttime, endtime = key

        reference_data = np.array([i.sample for i in value])
        variance_data.append(DataPoint.from_tuple(start_time=starttime,
                                                     end_time=endtime,
                                                     sample=np.var(reference_data)))

        mean_data.append(
            DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=np.mean(reference_data)))
        median_data.append(
            DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=np.median(reference_data)))
        quartile_deviation_data.append(DataPoint.from_tuple(start_time=starttime,
                                                               end_time=endtime,
                                                               sample=(0.5 * (
                                                                       np.percentile(reference_data, 75) - np.percentile(
                                                                   reference_data,
                                                                   25)))))

    variance = DataStream.from_datastream([accel])
    variance.data = variance_data
    mean = DataStream.from_datastream([datastream])
    mean.data = mean_data
    median = DataStream.from_datastream([datastream])
    median.data = median_data
    quartile = DataStream.from_datastream([datastream])
    quartile.data = quartile_deviation_data

    return variance, mean, median, quartile

def generate_activity_feature_vector(accel_features: List[DataStream]):
    """
    :param accel_features: DataStream
    :return feature vector DataStream
    """

    final_feature_vector = []
    for i in range(len(accel_features[0].data)):
        feature_vector = []
        for ef in accel_features:
            if ef.data[i].sample is None:
                continue
            feature_vector.append(ef.data[i].sample)

        final_feature_vector.append(DataPoint.from_tuple(start_time=ef.data[i].start_time,
                                                         end_time=ef.data[i].end_time,
                                                         sample=feature_vector))

    feature_vector_ds = DataStream.from_datastream(accel_features)
    feature_vector_ds.data = final_feature_vector

    return feature_vector_ds

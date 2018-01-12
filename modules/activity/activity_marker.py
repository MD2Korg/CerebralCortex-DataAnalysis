from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream
from modules.activity.activity_feature import compute_accelerometer_features
from modules.activity.do_classification import classify_posture, classify_activity


def do_activity_marker(accel_stream: DataStream):

    accel_features = compute_accelerometer_features(accel_stream, window_size=10)

    posture_label = classify_posture(accel_features)
    activity_label = classify_activity(accel_features)

    return posture_label, activity_label


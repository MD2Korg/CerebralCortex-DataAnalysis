from cerebralcortex.core.datatypes.datastream import DataStream
from core.markers.activity.activity_feature import compute_accelerometer_features
from core.markers.activity.do_classification import classify_posture, classify_activity
from core.signalprocessing.gravity_filter.gravityFilter import gravityFilter_function

def do_activity_marker(accel_stream: DataStream, gyro_stream: DataStream):

    acc_sync_filtered = gravityFilter_function(accel_stream, gyro_stream, 25.0)

    accel_features = compute_accelerometer_features(acc_sync_filtered, window_size=10)

    posture_label = classify_posture(accel_features)
    activity_label = classify_activity(accel_features)

    return posture_label, activity_label
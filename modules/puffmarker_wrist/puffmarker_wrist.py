import uuid

from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from modules.puffmarker_wrist.wrist_features import compute_wrist_feature
from modules.puffmarker_wrist.metadata import update_metadata

def process_puffmarker(user_id: uuid, CC: CerebralCortex, config: dict,
                       accel_stream_left: DataStream,
                       gyro_stream_left: DataStream,
                       accel_stream_right: DataStream,
                       gyro_stream_right: DataStream):
    """
    1. generates puffmarker wrist feature vectors from accelerometer and gyroscope
    2. classifies each feature vector as either puff or non_puff
    3. constructs smoking episodes from this puffs
    :param accel_stream_left:
    :param gyro_stream_left:
    :param accel_stream_right:
    :param gyro_stream_right:
    """

    fast_size = config["thresholds"]["fast_size"]
    slow_size = config["thresholds"]["slow_size"]
    all_features_left = compute_wrist_feature(accel_stream_left, gyro_stream_left, 'leftwrist', fast_size, slow_size)

    all_features_left = update_metadata(all_features_left, user_id, CC, config, accel_stream_left, gyro_stream_left, 'leftwrist')
    CC.save_datastream(all_features_left, "datastream")

    all_features_right = compute_wrist_feature(accel_stream_right, gyro_stream_right, 'rightwrist')
    all_features_right = update_metadata(all_features_right , user_id, CC, config, accel_stream_right, gyro_stream_right, 'rightwrist')
    CC.save_datastream(all_features_right, "datastream")



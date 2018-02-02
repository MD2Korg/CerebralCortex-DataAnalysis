import uuid
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.metadata_manager.metadata import Metadata

def update_metadata(all_features_left: DataStream, user_id, CC: CerebralCortex, config: dict, accel_stream: DataStream, gyro_stream: DataStream, wrist):
    owner = user_id
    if wrist == 'leftwrist':
        dd_stream_name = config["stream_names"]["puffmarker_wrist_feature_left"]
        input_param = {"motionsense_hrv_accel_left": config["stream_names"]["motionsense_hrv_accel_left"],
                       "motionsense_hrv_gyro_left": config["stream_names"]["motionsense_hrv_gyro_left"]}
    else:
        dd_stream_name = config["stream_names"]["puffmarker_wrist_feature_right"]
        input_param = {"motionsense_hrv_accel_left": config["stream_names"]["motionsense_hrv_accel_right"],
                       "motionsense_hrv_gyro_left": config["stream_names"]["motionsense_hrv_gyro_right"]}

    feature_vector_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(dd_stream_name + owner + "PUFFMARKER WRIST FEATURES"))
    dd_stream_id = feature_vector_stream_id
    stream_type = "ds"

    data_descriptor = config["description"]["puffmarker_wrist_feature_vector"]
    algo_description = config["description"]["puffmarker_wrist"]

    input_streams = [{"owner_id": owner, "id": str(accel_stream.identifier), "name": accel_stream.name}
        , {"owner_id": owner, "id": str(gyro_stream.identifier), "name": gyro_stream.name}]

    method = 'core.feature.puffmarker_features.wrist_features.py'

    execution_context = get_execution_context(dd_stream_name, input_param, input_streams, method,
                                              algo_description, config)

    annotations = get_annotations()

    ds = DataStream(identifier=dd_stream_id, owner=owner, name=dd_stream_name, data_descriptor=data_descriptor,
                    execution_context=execution_context, annotations=annotations,
                    stream_type=stream_type, data=all_features_left.data)

    return ds

def get_execution_context(name: str, input_param: dict, input_streams: dict, method: str,
                          algo_description: str, config: dict) -> dict:
    """
    :param name:
    :param input_param:
    :param input_streams:
    :param method:
    :param algo_description:
    :param config:
    :return:
    """

    author = [{"name": "Nazir Saleheen", "email": "nazir.saleheen@gmail.com"}]
    version = '0.0.1'
    ref = {"url": "https://dl.acm.org/citation.cfm?id=2806897"}
    metadata = Metadata()
    processing_module = metadata.processing_module_schema(name, config["description"]["puffmarker_wrist"],
                                                          input_param, input_streams)
    algorithm = metadata.algorithm_schema(method, algo_description, author, version, ref)
    ec = metadata.get_execution_context(processing_module, algorithm)

    return ec


def get_annotations() -> dict:
    """
    :return:
    """
    annotations = []
    return annotations

# Copyright (c) 2018, MD2K Center of Excellence
# - Nazir Saleheen <nazir.saleheen@gmail.com>
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


import uuid
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.metadata_manager.metadata import Metadata
import os
import json
import uuid

def store_data(filepath, input_streams, user_id, data, instance):
    output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(filepath + user_id+"PUFFMARKER")))
    # TODO FIXME XXX : This is a hack, Please fixme
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    newfilepath = os.path.join(cur_dir,filepath)
    with open(newfilepath,"r") as f:
        metadata = f.read()
        metadata = metadata.replace("CC_INPUT_STREAM_ID_CC",input_streams[0]["id"])
        metadata = metadata.replace("CC_INPUT_STREAM_NAME_CC",input_streams[0]["name"])
        metadata = metadata.replace("CC_OUTPUT_STREAM_IDENTIFIER_CC",output_stream_id)
        metadata = metadata.replace("CC_OWNER_CC",user_id)
        metadata = json.loads(metadata)

        instance.store(identifier=output_stream_id, owner=user_id, name=metadata["name"], data_descriptor=metadata["data_descriptor"],
                       execution_context=metadata["execution_context"], annotations=metadata["annotations"],
                       stream_type="datastream", data=data)



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

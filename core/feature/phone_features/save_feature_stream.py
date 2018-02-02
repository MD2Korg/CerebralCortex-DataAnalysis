import json
import uuid
from cerebralcortex.core.datatypes.datastream import DataStream


def store_data(filepath, input_streams, user_id, data, CC):
    output_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(filepath + user_id+"PHONE FEATURES"))
    with open(filepath,"r") as f:
        metadata = f.read()
        metadata = metadata.replace("CC_INPUT_STREAM_ID_CC",input_streams["id"])
        metadata = metadata.replace("CC_INPUT_STREAM_NAME_CC",input_streams["name"])
        metadata = metadata.replace("CC_OUTPUT_STREAM_IDENTIFIER_CC",output_stream_id)
        metadata = metadata.replace("CC_OWNER_CC",user_id)
        metadata = json.loads(metadata)

        ds = DataStream(identifier=output_stream_id, owner=user_id, name=metadata["name"], data_descriptor=metadata["data_descriptor"],
                        execution_context=metadata["execution_context"], annotations=metadata["annotations"],
                        stream_type="datastream", data=data)
        try:
            CC.save_datastream(ds, "datastream")
        except Exception as exp:
            print(exp)
import os
import json
import uuid


def store_data(filepath, input_streams, user_id, data, instance):
    output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(filepath + user_id+"PHONE FEATURES")))
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


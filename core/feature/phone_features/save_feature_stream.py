import json
def store_data(filepath, input_streams, user_id, data):
    with open(filepath,"r") as f:
        metadata = f.read()
        metadata = metadata.replace("CC_INPUT_STREAM_ID_CC",input_streams["id"])
        metadata = metadata.replace("CC_INPUT_STREAM_NAME_CC",input_streams["name"])
        metadata = metadata.replace("CC_OUTPUT_STREAM_IDENTIFIER_CC",output_stream_id)
        metadata = metadata.replace("CC_OWNER_CC",user_id)
        metadata = json.loads(metadata)
        return metadata


def store(data, stream_name, CC):
    metadata = read_metadata_json("metadata/"+stream_name+".json")

    if data:
        # basic output stream info
        owner = input_streams[0]["owner_id"]
        dd_stream_id = output_streams["id"]
        dd_stream_name = output_streams["name"]
        stream_type = "ds"

        data_descriptor = metadata["dd"]
        execution_context = metadata["ec"]
        annotations = metadata["anno"]

        ds = DataStream(identifier=dd_stream_id, owner=owner, name=dd_stream_name, data_descriptor=data_descriptor,
                        execution_context=execution_context, annotations=annotations,
                        stream_type=stream_type, data=data)

        CC_obj.save_datastream(ds, "datastream")

read_metadata_json("metadata/average_inter_phone_call_sms_time_daily.json")
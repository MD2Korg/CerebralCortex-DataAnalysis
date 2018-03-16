from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint

from datetime import datetime, timedelta
import pprint as pp
import numpy as np

import pdb
import uuid

import json

CC = CerebralCortex('/home/md2k/cc_configuration.yml')
def get_data_by_stream_name(stream_name, user_id, day):
    stream_ids = CC.get_stream_id(user_id, stream_name)
    data = []
    for stream in stream_ids:
        data += CC.get_stream(stream['identifier'], user_id = user_id, day=day).data
    return data

# storing the streams

def store(identifier,owner,name,data_descriptor,execution_context,annotations,stream_type,data):
    ds = DataStream(identifier=identifier, owner=owner, name=name, data_descriptor=data_descriptor,
                    execution_context=execution_context, annotations=annotations,
                    stream_type=stream_type, data=data)
    try:
        print("Saving Stream",ds)
        CC.save_stream(ds)
    except Exception as exp:
        print("Error", str(exp))

def store_data(filepath, input_stream_id, input_stream_name, user_id, data):
    output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(filepath + user_id+"WORK FEATURES")))
    # TODO FIXME XXX : This is a hack, Please fixme
    cur_dir = os.getcwd() #os.path.dirname(os.path.abspath(__file__))
    newfilepath = os.path.join(cur_dir,filepath)
    with open(newfilepath,"r") as f:
        metadata = f.read()
        metadata = metadata.replace("CC_INPUT_STREAM_ID_CC",input_stream_id)
        metadata = metadata.replace("CC_INPUT_STREAM_NAME_CC",input_stream_name)
        metadata = metadata.replace("CC_OUTPUT_STREAM_IDENTIFIER_CC",output_stream_id)
        metadata = metadata.replace("CC_OWNER_CC",user_id)
        json_metadata = json.loads(metadata)

        store(identifier=output_stream_id, owner=user_id, name=json_metadata["name"], data_descriptor=json_metadata["data_descriptor"],
              execution_context=json_metadata["execution_context"], annotations=json_metadata["annotations"],
              stream_type="datastream", data=data)

# string methods end here


users = CC.get_all_users("mperf")
for user in users:
    work_data = []
    stream_ids = CC.get_stream_id(user["identifier"], "org.md2k.data_analysis.gps_episodes_and_semantic_location")
    for stream_id in stream_ids:
        duration = CC.get_stream_duration(stream_id["identifier"])
        print (user, stream_id, duration)
        start_day = duration['start_time'].date()
        end_day = duration['end_time'].date()
        current_day= None
        while start_day < end_day:
            location_data_stream = CC.get_stream(stream_id["identifier"], user["identifier"], start_day.strftime("%Y%m%d"))
            #pp.pprint(location_data_stream.data)

            for data in location_data_stream.data:
                if data.sample[0] != "Work":
                    continue
                d = DataPoint(data.start_time, data.end_time, data.offset, data.sample)
                if d.offset:
                    d.start_time += timedelta(milliseconds=d.offset)
                    if d.end_time:
                        d.end_time += timedelta(milliseconds=d.offset)
                    else:
                        continue
                if d.start_time.date() != current_day:
                    if current_day:
                        temp = DataPoint(data.start_time, data.end_time)
                        temp.start_time = work_start_time
                        temp.end_time = work_end_time
                        temp.sample = 'office'
                        work_data.append(temp)
                    #   pdb.set_trace()
                    work_start_time = d.start_time
                    current_day = d.start_time.date()

                work_end_time = d.end_time
            # work_data.append(d)
            #             temp = DataPoint(data.start_time, data.end_time)
            #             temp.start_time = work_start_time
            #             temp.end_time = work_end_time
            #             temp.sample = 'office'
            #             work_data.append(temp)

            start_day += timedelta(days = 1)
    if len(work_data)>0:
        print(work_data)
        store_data('metadata/working_days.json',  CC.get_stream_id(user["identifier"], "org.md2k.data_analysis.gps_episodes_and_semantic_location")[0]['identifier'], "org.md2k.data_analysis.gps_episodes_and_semantic_location", user["identifier"], work_data)




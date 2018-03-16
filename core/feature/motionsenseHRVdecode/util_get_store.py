# -*- coding: utf-8 -*-
"""
Created on Sat Mar 10 19:01:26 2018

@author: Md Azim Ullah
"""
import os
import json
import uuid
from datetime import timedelta
from typing import List
from cerebralcortex.cerebralcortex import CerebralCortex

def get_stream_days(stream_id: uuid, CC: CerebralCortex) -> List:
    """
    Returns a list of days (string format: YearMonthDay (e.g., 20171206)
    :param stream_id:
    """
    stream_dicts = CC.get_stream_duration(stream_id)
    stream_days = []
    days = stream_dicts["end_time"]-stream_dicts["start_time"]
    for day in range(days.days+1):
        stream_days.append((stream_dicts["start_time"]+timedelta(days=day)).strftime('%Y%m%d'))
    return stream_days

def store_data(filepath, input_streams, user_id, data, instance):
    output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(filepath + user_id+"RECOVERED RESPIRATION")))
    # TODO FIXME XXX : This is a hack, Please fixme
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    newfilepath = os.path.join(cur_dir,filepath)
    with open(newfilepath,"r") as f:
        metadata = f.read()
        metadata = metadata.replace("CC_INPUT_STREAM_ID_CC",input_streams[0]["identifier"])
        metadata = metadata.replace("CC_INPUT_STREAM_NAME_CC",input_streams[0]["name"])
        metadata = metadata.replace("CC_OUTPUT_STREAM_IDENTIFIER_CC",output_stream_id)
        metadata = metadata.replace("CC_OWNER_CC",user_id)
        metadata = json.loads(metadata)
        if len(data) > 0:
            instance.store(identifier=output_stream_id, owner=user_id, name=metadata["name"], data_descriptor=metadata["data_descriptor"],
                           execution_context=metadata["execution_context"], annotations=metadata["annotations"],
                           stream_type="datastream", data=data)
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint
from typing import List, Callable, Any

import ast
from distutils.version import StrictVersion

def print_data(data):
    print(len(data))
    for d in data:
        print(d)
        
def get_filtered_data(data: List[DataPoint],
                          admission_control: Callable[[Any], bool] = None) -> List[DataPoint]:
    """
    Return the filtered list of DataPoints according to the admission control provided

    :param List(DataPoint) data: Input data list
    :param Callable[[Any], bool] admission_control: Admission control lambda function, which accepts the sample and
            returns a bool based on the data sample validity
    :return: Filtered list of DataPoints
    :rtype: List(DataPoint)
    """
    if admission_control is None:
        return data
    filtered_data = []
    for d in data:
        if admission_control(d.sample):
            filtered_data.append(d)
        elif type(d.sample) is list and len(d.sample) == 1 and admission_control(d.sample[0]):
            d.sample = d.sample[0]
            filtered_data.append(d)
            
    return filtered_data # [d for d in data if admission_control(d.sample)]

def get_latest_stream_id(CC, user_id, stream_name):
        streamids = CC.get_stream_id(user_id, stream_name)
        latest_stream_id = []
        latest_stream_version = None
        for stream in streamids:
            stream_metadata = CC.get_stream_metadata(stream['identifier'])
            execution_context = stream_metadata[0]['execution_context']
            execution_context = ast.literal_eval(execution_context)
            stream_version = execution_context['algorithm']['version']
            try:
                stream_version = int(stream_version)
                stream_version = str(stream_version) + '.0'
            except:
                pass
            stream_version = StrictVersion(stream_version)
            if not latest_stream_version:
                latest_stream_id.append(stream)
                latest_stream_version = stream_version
            else:
                if stream_version > latest_stream_version:
                    latest_stream_id = [stream]
                    latest_stream_version = stream_version
                elif stream_version == latest_stream_version:
                    latest_stream_id.append(stream)

        return latest_stream_id
        
def get_derived_data_by_stream_name(stream_name, user_id, day, CC, localtime = False, start_time = None, end_time = None):
    stream_ids = get_latest_stream_id(CC, user_id, stream_name)
    data = []
    for stream in stream_ids:
        data += CC.get_stream(stream['identifier'], user_id = user_id, day=day, start_time=start_time, end_time=end_time, localtime = localtime).data
    
    if stream_ids and len(stream_ids)>1:
        data.sort(key=lambda x:x.start_time)
    return data


def get_raw_data_by_stream_name(stream_name, user_id, day, CC, localtime = False, start_time = None, end_time = None):
    stream_ids = CC.get_stream_id(user_id, stream_name)
    data = []
    for stream in stream_ids:
        data += CC.get_stream(stream['identifier'], user_id = user_id, day=day, start_time=start_time, end_time=end_time, localtime = localtime).data
    
    if stream_ids and len(stream_ids)>1:
        data.sort(key=lambda x:x.start_time)
    return data

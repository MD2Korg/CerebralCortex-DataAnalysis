# 1. Total / Average screen tap count
#       input: phone touch screen stream
#       output: total / average
#
# 2. Average inter phone call time
#       input: phone call log stream
#       output: time in second
#
# 3. Average inter text / sms time
#       input: phone sms log stream
#       output time in second
#
# 4. total notification count
#       input: phone notification stream
#       output: integer
#
# 5. Average proximity
#       input: phone proximity sensor stream
#       output: float
#
# 6. Average ambient light
#       input: phone ambient light sensor stream
#       output: float
#
# 7. Average pressure sensed in phone
#       input: phone pressure sensor stream
#       output: float
#
# the methods written below might not be in order described above

import numpy as np
import uuid
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream


def average_inter_phone_call_sms_time_minute(phonedatastream: DataStream, smsdatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :param smsdatastream: sms length stream
    :return:
    """
    if len(phonedatastream.data)+len(smsdatastream) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (CALL & SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call and sms) in minutes within the given period"}]
    for i in len(smsdatastream.data):
        smsdatastream.data[i].end_time = smsdatastream.data[i].start_time

    combined_data = phonedatastream.data + smsdatastream.data
    sorted(combined_data, key=lambda x:x.start_time)

    total_inter_event_time = 0
    last_end = combined_data[0].end_time
    for i in range(1, len(combined_data)):
        total_inter_event_time += max(0, combined_data[i] - last_end)
        last_end = max(last_end, combined_data[i].end_time)

    total_inter_event_time /= 60000.0

    data = [DataPoint(combined_data[0].start_time, last_end, total_inter_event_time / (len(combined_data)-1))]
    start_time = data[0].start_time
    end_time = data[-1].end_time

    return DataStream(identifier, phonedatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def variance_inter_phone_call_sms_time_minute(phonedatastream: DataStream, smsdatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :param smsdatastream: sms length stream
    :return:
    """
    if len(phonedatastream.data)+len(smsdatastream) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'VAR. INTER EVENT TIME (CALL & SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Variance of inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Variance of inter event time (call and sms) in minutes within the given period"}]
    for i in len(smsdatastream.data):
        smsdatastream.data[i].end_time = smsdatastream.data[i].start_time

    combined_data = phonedatastream.data + smsdatastream.data
    sorted(combined_data, key=lambda x:x.start_time)

    total_inter_event_time = 0
    last_end = combined_data[0].end_time
    gaps = []
    for i in range(1, len(combined_data)):
        gaps.append(max(0, combined_data[i] - last_end))
        last_end = max(last_end, combined_data[i].end_time)

    gaps = list(map(lambda x:x/60000.0, gaps))

    data = [DataPoint(combined_data[0].start_time, last_end, np.var(gaps))]
    start_time = data[0].start_time
    end_time = data[-1].end_time

    return DataStream(identifier, phonedatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def average_inter_phone_call_time_minute(datastream: DataStream):
    """

    :param datastream: call duration stream
    :return:
    """
    if len(datastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (CALL)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call) in minutes within the given period"}]

    total_inter_event_time = 0
    for i in range(1, len(datastream.data)):
        total_inter_event_time += datastream.data[i].start_time - datastream.data[i-1].end_time
    total_inter_event_time /= 60000.0

    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].end_time, total_inter_event_time / (len(datastream.data)-1))]
    start_time = data[0].start_time
    end_time = data[-1].end_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def average_inter_sms_time_minute(datastream: DataStream):
    """

    :param datastream: sms length stream
    :return:
    """
    if len(datastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (sms) in minutes within the given period"}]

    total_inter_event_time = 0
    for i in range(1, len(datastream.data)):
        total_inter_event_time += datastream.data[i].start_time - datastream.data[i-1].start_time
    total_inter_event_time /= 60000.0

    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].start_time, total_inter_event_time / (len(datastream.data)-1))]
    start_time = data[0].start_time
    end_time = data[-1].end_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def variance_inter_phone_call_time_minute(datastream: DataStream):
    """

    :param datastream: call duration stream
    :return:
    """
    if len(datastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'VAR. INTER EVENT TIME (CALL)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Variance inter event time (call)", "DATA_TYPE":"float", "DESCRIPTION": "Variance of inter event time (call) in minutes within the given period"}]

    gaps = []
    for i in range(1, len(datastream.data)):
        gaps.append((datastream.data[i].start_time - datastream.data[i-1].end_time)/60000.0)

    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].end_time, np.var(gaps))]
    start_time = data[0].start_time
    end_time = data[-1].end_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def variance_inter_sms_time_minute(datastream: DataStream):
    """

    :param datastream: sms length stream
    :return:
    """
    if len(datastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'VAR. INTER EVENT TIME (SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Variance inter event time (sms)", "DATA_TYPE":"float", "DESCRIPTION": "Variance of inter event time (sms) in minutes within the given period"}]

    gaps = []
    for i in range(1, len(datastream.data)):
        gaps.append( (datastream.data[i].start_time - datastream.data[i-1].start_time) / 60000.0)

    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].start_time, np.var(gaps))]
    start_time = data[0].start_time
    end_time = data[-1].end_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def average_call_duration_second(datastream: DataStream):

    """

    :param datastream: call duration stream
    :return:
    """
    identifier = uuid.uuid1()
    name = 'AVG. CALL DURATION'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average call duration", "DATA_TYPE":"float", "DESCRIPTION": "Average call duration within the given period"}]

    total_call_duration = 0
    for d in datastream.data:
        total_call_duration += float(d.sample)
    total_call_duration /= 1000.0

    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].end_time, total_call_duration / len(datastream.data))]
    start_time = data[0].start_time
    end_time = data[-1].end_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def average_sms_length(datastream: DataStream):

    """

    :param datastream: sms length stream
    :return:
    """
    identifier = uuid.uuid1()
    name = 'AVG. SMS LENGTH'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average sms length", "DATA_TYPE":"float", "DESCRIPTION": "Average sms length within the given period"}]

    total_sms_length = 0
    for d in datastream.data:
        total_sms_length += int(d.sample)

    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].start_time, total_sms_length / len(datastream.data))]
    start_time = data[0].start_time
    end_time = data[-1].end_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def total_phone_screen_tap_count(datastream: DataStream):

    """

    :param datastream: phone screen touch stream
    :return:
    """
    identifier = uuid.uuid1()
    name = 'TOTAL PHONE SCREEN TAP COUNT'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Screen Touch Count", "DATA_TYPE":"integer", "DESCRIPTION": "Total screen touch count within the time period"}]
    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].start_time, len(datastream.data))]
    start_time = data[0].start_time
    end_time = data[-1].end_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                  execution_context,
                  annotations,
                  "1",
                  start_time,
                  end_time,
                  data)

def average_phone_screen_tap_per_minute(datastream: DataStream):


    """

    :param datastream: phone screen touch stream
    :return:
    """
    identifier = uuid.uuid1()
    name = 'AVG. PHONE SCREEN TAP COUNT'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Screen Touch Count per minute", "DATA_TYPE":"float", "DESCRIPTION": "Average total screen touch count within the time period per minute"}]
    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].start_time, len(datastream.data)/60000.0)]
    start_time = data[0].start_time
    end_time = data[-1].end_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


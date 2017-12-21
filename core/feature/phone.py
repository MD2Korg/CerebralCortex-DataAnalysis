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

import math
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
    data_descriptor = [{"NAME":"Screen Touch Count", "DATA_TYPE":"int", "DESCRIPTION": "Total screen touch count within the time period"}]
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


def entropy_phone_call(datastream: DataStream):

    """

    :param datastream: CU_CALL_NUMBER--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'ENTROPY PHONE CALL'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Entropy of phone call", "DATA_TYPE":"float", "DESCRIPTION": "Entropy of phone call within the given period"}]

    number = {}
    for x in datastream.data:
        if x.sample not in number:
            number[x.sample] = 0
        number[x.sample] += 1

    entropy = 0.0
    for key, value in number.items():
        entropy = entropy - value * math.log(value)

    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].start_time, entropy)]
    start_time = data[0].start_time
    end_time = data[-1].start_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)

def entropy_phone_sms(datastream: DataStream):

    """

    :param datastream: CU_SMS_NUMBER--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'ENTROPY PHONE SMS'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Entropy of phone SMS", "DATA_TYPE":"float", "DESCRIPTION": "Entropy of phone SMS within the given period"}]

    number = {}
    for x in datastream.data:
        if x.sample not in number:
            number[x.sample] = 0
        number[x.sample] += 1

    entropy = 0.0
    for key, value in number.items():
        entropy = entropy - value * math.log(value)

    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].start_time, entropy)]
    start_time = data[0].start_time
    end_time = data[-1].end_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def entropy_phone_call_sms(calldatastream: DataStream, smsdatastream: DataStream):

    """

    :param calldatastream: CU_CALL_NUMBER--edu.dartmouth.eureka
    :param smsdatastream: CU_SMS_NUMBER--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'ENTROPY PHONE CALL & SMS'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Entropy of phone call and SMS", "DATA_TYPE":"float", "DESCRIPTION": "Entropy of phone call and SMS within the given period"}]

    mergeddata = calldatastream.data + smsdatastream.data
    number = {}
    for x in mergeddata:
        if x.sample not in number:
            number[x.sample] = 0
        number[x.sample] += 1

    entropy = 0.0
    for key, value in number.items():
        entropy = entropy - value * math.log(value)

    start_time = min(calldatastream.data[0].start_time, smsdatastream.data[0].start_time)
    end_time = max(calldatastream.data[0].start_time, smsdatastream.data[0].start_time)

    data = [DataPoint(start_time, end_time, entropy)]


    return DataStream(identifier, calldatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def unique_contacts_phone_call(datastream: DataStream):

    """

    :param datastream: CU_CALL_NUMBER--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'UNIQUE CONTACT -- PHONE CALL'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Unique contacts (phone call)", "DATA_TYPE":"int", "DESCRIPTION": "Phone call from unique numbers within the given period"}]

    numbers = set([x.sample for x in datastream.data])

    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].start_time, len(numbers))]
    start_time = data[0].start_time
    end_time = data[-1].start_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def unique_contact_sms(datastream: DataStream):

    """

    :param datastream: CU_SMS_NUMBER--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'UNIQUE CONTACT -- SMS'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Unique contact(SMS)", "DATA_TYPE":"int", "DESCRIPTION": "Number of unique contacts (sms) within the given period"}]

    numbers = set([x.sample for x in datastream.data])

    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].start_time, len(numbers))]
    start_time = data[0].start_time
    end_time = data[-1].end_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def unique_contact_call_sms(calldatastream: DataStream, smsdatastream: DataStream):

    """

    :param calldatastream: CU_CALL_NUMBER--edu.dartmouth.eureka
    :param smsdatastream: CU_SMS_NUMBER--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'UNIQUE CONTACT -- PHONE CALL & SMS'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Unique contact (phone call and SMS)", "DATA_TYPE":"int", "DESCRIPTION": "Unique contacts (Call and SMS) within the given period"}]

    mergeddata = calldatastream.data + smsdatastream.data
    numbers = set([x.sample for x in mergeddata])

    start_time = min(calldatastream.data[0].start_time, smsdatastream.data[0].start_time)
    end_time = max(calldatastream.data[0].start_time, smsdatastream.data[0].start_time)

    data = [DataPoint(start_time, end_time, len(numbers))]


    return DataStream(identifier, calldatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def contact_to_interaction_ratio_call(datastream: DataStream):

    """

    :param datastream: CU_CALL_NUMBER--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'CONTACT TO INTERACTION RATIO -- PHONE CALL'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Contact to interaction ratio (phone call)", "DATA_TYPE":"float", "DESCRIPTION": "Contact to interactoin ratio (Phone call) within the given period"}]

    numbers = set([x.sample for x in datastream.data])

    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].start_time, len(datastream.data) / len(numbers))]
    start_time = data[0].start_time
    end_time = data[-1].start_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def contact_to_interaction_ratio_sms(datastream: DataStream):

    """

    :param datastream: CU_SMS_NUMBER--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'CONTACT TO INTERACTION RATIO -- SMS'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Contact to interaction ratio(SMS)", "DATA_TYPE":"float", "DESCRIPTION": "Contact to interaction ratio (sms) within the given period"}]

    numbers = set([x.sample for x in datastream.data])

    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].start_time, len(datastream.data) / len(numbers))]
    start_time = data[0].start_time
    end_time = data[-1].end_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def contact_to_interaction_ratio_call_sms(calldatastream: DataStream, smsdatastream: DataStream):

    """

    :param calldatastream: CU_CALL_NUMBER--edu.dartmouth.eureka
    :param smsdatastream: CU_SMS_NUMBER--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'CONTACT TO INTERACTION RATIO -- PHONE CALL & SMS'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Contact to interation ratio (phone call and SMS)", "DATA_TYPE":"float", "DESCRIPTION": "Contact to interaction ratio (Call and SMS) within the given period"}]

    mergeddata = calldatastream.data + smsdatastream.data
    numbers = set([x.sample for x in mergeddata])

    start_time = min(calldatastream.data[0].start_time, smsdatastream.data[0].start_time)
    end_time = max(calldatastream.data[0].start_time, smsdatastream.data[0].start_time)

    data = [DataPoint(start_time, end_time, len(mergeddata) / len(numbers))]


    return DataStream(identifier, calldatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def number_of_interaction_call(datastream: DataStream):

    """

    :param datastream: CU_CALL_NUMBER--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'NUMBER OF INTERACTION -- PHONE CALL'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Number of interaction (phone call)", "DATA_TYPE":"int", "DESCRIPTION": "Number of interaction (Phone call) within the given period"}]


    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].start_time, len(datastream.data))]
    start_time = data[0].start_time
    end_time = data[-1].start_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def number_of_interaction_sms(datastream: DataStream):

    """

    :param datastream: CU_SMS_NUMBER--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'NUMBER OF INTERACTION -- SMS'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Number of interaction (SMS)", "DATA_TYPE":"int", "DESCRIPTION": "Number of interaction (sms) within the given period"}]

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


def number_of_interaction_call_sms(calldatastream: DataStream, smsdatastream: DataStream):

    """

    :param calldatastream: CU_CALL_NUMBER--edu.dartmouth.eureka
    :param smsdatastream: CU_SMS_NUMBER--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'NUMBER OF INTERACTION -- PHONE CALL & SMS'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Number of interaction (phone call and SMS)", "DATA_TYPE":"int", "DESCRIPTION": "Number of interaction (Call and SMS) within the given period"}]

    mergeddata = calldatastream.data + smsdatastream.data

    start_time = min(calldatastream.data[0].start_time, smsdatastream.data[0].start_time)
    end_time = max(calldatastream.data[0].start_time, smsdatastream.data[0].start_time)

    data = [DataPoint(start_time, end_time, len(mergeddata))]

    return DataStream(identifier, calldatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def call_initiated_percent(calldatastream: DataStream):

    """

    :param calldatastream: CU_CALL_TYPE--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'PERCENT INITIATED -- PHONE CALL'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Percent initiated (phone call)", "DATA_TYPE":"float", "DESCRIPTION": "Percent initiated (Phone call) within the given period"}]

    outgoing = 0
    for x in calldatastream.data:
        if int(x.sample) == 2:
            outgoing += 1

    data = [DataPoint(calldatastream.data[0].start_time, calldatastream.data[-1].start_time, outgoing * 100 / len(calldatastream.data))]
    start_time = data[0].start_time
    end_time = data[-1].start_time

    return DataStream(identifier, calldatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def sms_initiated_percent(smsdatastream: DataStream):

    """

    :param smsdatastream: CU_SMS_TYPE--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'PERCENT INITIATED -- SMS'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Percent initiated (SMS)", "DATA_TYPE":"float", "DESCRIPTION": "Percent initiated (SMS) within the given period"}]

    outgoing = 0
    for x in smsdatastream.data:
        if int(x.sample) == 2:
            outgoing += 1

    data = [DataPoint(smsdatastream.data[0].start_time, smsdatastream.data[-1].start_time, outgoing * 100 / len(smsdatastream.data))]
    start_time = data[0].start_time
    end_time = data[-1].start_time

    return DataStream(identifier, smsdatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def call_sms_initiated_percent(calldatastream: DataStream, smsdatastream: DataStream):

    """

    :param calldatastream: CU_CALL_TYPE--edu.dartmouth.eureka
    :param smsdatastream: CU_SMS_TYPE--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'PERCENT INITIATED -- CALL & SMS'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Percent initiated (CALL & SMS)", "DATA_TYPE":"float", "DESCRIPTION": "Percent initiated (CALL & SMS) within the given period"}]

    mergeddata = calldatastream.data + smsdatastream.data
    outgoing = 0
    for x in mergeddata:
        if int(x.sample) == 2:
            outgoing += 1


    start_time = min(calldatastream.data[0].start_time, smsdatastream.data[0].start_time)
    end_time = max(calldatastream.data[0].start_time, smsdatastream.data[0].start_time)

    data = [DataPoint(start_time, end_time, outgoing * 100 / len(mergeddata))]

    return DataStream(identifier, smsdatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def total_screen_on_time_second(screendatastream: DataStream):

    """

    :param screendatastream: CU_IS_SCREEN_ON--edu.dartmouth.eureka
    :return:
    """
    identifier = uuid.uuid1()
    name = 'TOTAL SCREEN ON TIME -- SECOND'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Total screen on time in seconds", "DATA_TYPE":"float", "DESCRIPTION": "Total screen on time in seconds within the given period"}]

    total = 0
    if len(screendatastream.data)>0:
        if screendatastream.data[0].sample == "false":
            total += screendatastream.data[0].start_time - screendatastream.start_time

        for i in range(1, len(screendatastream.data)):
            if screendatastream.data[i].sample == "false":
                total += screendatastream.data[i].start_time - screendatastream.data[i-1].start_time

        if screendatastream.data[-1].sample == "true":
            total += screendatastream.end_time - screendatastream.data[-1].start_time

        total /= 1000.0

    data = [DataPoint(screendatastream.start_time, screendatastream.end_time, total)]
    start_time = screendatastream.start_time
    end_time = screendatastream.end_time

    return DataStream(identifier, screendatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def average_pressure(pressuredatastream: DataStream):

    """

    :param pressuredatastream: PRESSURE--org.md2k.phonesensor--PHONE
    :return:
    """
    identifier = uuid.uuid1()
    name = 'AVERAGE PRESSURE'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average pressure", "DATA_TYPE":"float", "DESCRIPTION": "Average pressure sensed by phone's barometer within the given period"}]

    totalpressure = 0
    for x in pressuredatastream.data:
        totalpressure += float(x.sample)

    data = [DataPoint(pressuredatastream.data[0].start_time, pressuredatastream.data[-1].start_time, totalpressure / len(pressuredatastream.data))]
    start_time = data[0].start_time
    end_time = data[-1].start_time

    return DataStream(identifier, pressuredatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def pressure_variance(pressuredatastream: DataStream):

    """

    :param pressuredatastream: PRESSURE--org.md2k.phonesensor--PHONE
    :return:
    """
    identifier = uuid.uuid1()
    name = 'VARIANCE OF PRESSURE'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Variance of pressure", "DATA_TYPE":"float", "DESCRIPTION": "Variance of pressure sensed by phone's barometer within the given period"}]

    l = list(map(lambda x: float(x.sample), pressuredatastream.data))

    data = [DataPoint(pressuredatastream.data[0].start_time, pressuredatastream.data[-1].start_time, np.var(l))]
    start_time = data[0].start_time
    end_time = data[-1].start_time

    return DataStream(identifier, pressuredatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def average_ambient_temperature(temperaturedatastream: DataStream):

    """

    :param temperaturedatastream: AMBIENT_TEMPERATURE--org.md2k.phonesensor--PHONE
    :return:
    """
    identifier = uuid.uuid1()
    name = 'AVERAGE TEMPERATURE'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average temperature", "DATA_TYPE":"float", "DESCRIPTION": "Average ambient temperature sensed by phone's barometer within the given period"}]

    total = 0
    for x in temperaturedatastream.data:
        total += float(x.sample)

    data = [DataPoint(temperaturedatastream.data[0].start_time, temperaturedatastream.data[-1].start_time, total / len(temperaturedatastream.data))]
    start_time = data[0].start_time
    end_time = data[-1].start_time

    return DataStream(identifier, temperaturedatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)


def average_ambient_light(lightdatastream: DataStream):

    """

    :param lightdatastream: AMBIENT_LIGHT--org.md2k.phonesensor--PHONE
    :return:
    """
    identifier = uuid.uuid1()
    name = 'AVERAGE AMBIENT LIGHT'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average ambient light", "DATA_TYPE":"float", "DESCRIPTION": "Average ambient light sensed by phone's barometer within the given period"}]

    total = 0
    for x in lightdatastream.data:
        total += float(x.sample)

    data = [DataPoint(lightdatastream.data[0].start_time, lightdatastream.data[-1].start_time, total / len(lightdatastream.data))]
    start_time = data[0].start_time
    end_time = data[-1].start_time

    return DataStream(identifier, lightdatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      data)
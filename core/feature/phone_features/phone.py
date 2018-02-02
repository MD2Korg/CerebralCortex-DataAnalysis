from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint
import  uuid
import datetime
import time
import numpy as np

def to_unixtime(d: datetime):
    return time.mktime(d.timetuple())


def inter_event_time_list(data):
    if len(data)==0:
        return None

    last_end = to_unixtime(data[0].end_time)

    ret = []
    flag = False
    for cd in data:
        if flag == False:
            flag = True
            continue
        current_start_time = to_unixtime(cd.start_time)
        ret.append(max(0, current_start_time - last_end))
        last_end = max(last_end, to_unixtime(cd.end_time))

    return list(map(lambda x: x/60.0, ret))


def average_inter_phone_call_sms_time_hourly(phonedatastream: DataStream, smsdatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :param smsdatastream: sms length stream
    :return:
    """
    if len(phonedatastream.data)+len(smsdatastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVGERAGE-INTER-EVENT-TIME-CALL-SMS'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call and sms) in minutes within the given period"}]
    tmpphonestream = phonedatastream
    tmpsmsstream = smsdatastream
    for s in tmpphonestream.data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
    for s in tmpsmsstream.data:
        s.end_time = s.start_time

    combined_data = phonedatastream.data + smsdatastream.data

    combined_data.sort(key=lambda x:x.start_time)

    new_data = []
    for h in range(0, 24):
        datalist = []
        start = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day, hour=h)
        end = start + datetime.timedelta(minutes=59)
        for d in combined_data:
            if start<=d.start_time<=end or start<=d.end_time<=end:
                datalist.append(d)
        if len(datalist) <=1:
            continue
        new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=sum(inter_event_time_list(datalist))/(len(datalist)-1)))



    start_time = new_data[0].start_time
    end_time = new_data[-1].end_time

    return DataStream(identifier, phonedatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      new_data)


def average_inter_phone_call_sms_time_four_hourly(phonedatastream: DataStream, smsdatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :param smsdatastream: sms length stream
    :return:
    """
    if len(phonedatastream.data)+len(smsdatastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (CALL & SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call and sms) in minutes within the given period"}]
    tmpphonestream = phonedatastream
    tmpsmsstream = smsdatastream
    for s in tmpphonestream.data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
    for s in tmpsmsstream.data:
        s.end_time = s.start_time

    combined_data = phonedatastream.data + smsdatastream.data

    combined_data.sort(key=lambda x:x.start_time)

    new_data = []
    for h in range(0, 24, 4):
        datalist = []
        start = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day, hour=h)
        end = start + datetime.timedelta(hours=3, minutes=59)
        for d in combined_data:
            if start<=d.start_time<=end or start<=d.end_time<=end:
                datalist.append(d)
        if len(datalist) <=1:
            continue
        new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=sum(inter_event_time_list(datalist))/(len(datalist)-1)))



    start_time = new_data[0].start_time
    end_time = new_data[-1].end_time

    return DataStream(identifier, phonedatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      new_data)

def average_inter_phone_call_sms_time_daily(phonedatastream: DataStream, smsdatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :param smsdatastream: sms length stream
    :return:
    """
    if len(phonedatastream.data)+len(smsdatastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (CALL & SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call and sms) in minutes within the given period"}]
    tmpphonestream = phonedatastream
    tmpsmsstream = smsdatastream
    for s in tmpphonestream.data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
    for s in tmpsmsstream.data:
        s.end_time = s.start_time

    combined_data = phonedatastream.data + smsdatastream.data

    combined_data.sort(key=lambda x:x.start_time)
    start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day)
    end_time = start_time + datetime.timedelta(hours=23, minutes=59)
    new_data = [DataPoint(start_time=start_time, end_time=end_time, sample= sum(inter_event_time_list(combined_data)) / (len(combined_data)-1))]


    return DataStream(identifier, phonedatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      new_data)

def variance_inter_phone_call_sms_time_daily(phonedatastream: DataStream, smsdatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :param smsdatastream: sms length stream
    :return:
    """
    if len(phonedatastream.data)+len(smsdatastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (CALL & SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call and sms) in minutes within the given period"}]
    tmpphonestream = phonedatastream
    tmpsmsstream = smsdatastream
    for s in tmpphonestream.data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
    for s in tmpsmsstream.data:
        s.end_time = s.start_time

    combined_data = phonedatastream.data + smsdatastream.data

    combined_data.sort(key=lambda x:x.start_time)
    start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day)
    end_time = start_time + datetime.timedelta(hours=23, minutes=59)

    new_data = [DataPoint(start_time=start_time, end_time=end_time, sample= np.var(inter_event_time_list(combined_data)) )]


    return DataStream(identifier, phonedatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      new_data)

def variance_inter_phone_call_sms_time_hourly(phonedatastream: DataStream, smsdatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :param smsdatastream: sms length stream
    :return:
    """
    if len(phonedatastream.data)+len(smsdatastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (CALL & SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call and sms) in minutes within the given period"}]
    tmpphonestream = phonedatastream
    tmpsmsstream = smsdatastream
    for s in tmpphonestream.data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
    for s in tmpsmsstream.data:
        s.end_time = s.start_time

    combined_data = phonedatastream.data + smsdatastream.data

    combined_data.sort(key=lambda x:x.start_time)

    new_data = []
    for h in range(0, 24):
        datalist = []
        start = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day, hour=h)
        end = start + datetime.timedelta(minutes=59)
        for d in combined_data:
            if start<=d.start_time<=end or start<=d.end_time<=end:
                datalist.append(d)
        if len(datalist) <=1:
            continue
        new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=np.var(inter_event_time_list(datalist))))



    start_time = new_data[0].start_time
    end_time = new_data[-1].end_time

    return DataStream(identifier, phonedatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      new_data)

def variance_inter_phone_call_sms_time_four_hourly(phonedatastream: DataStream, smsdatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :param smsdatastream: sms length stream
    :return:
    """
    if len(phonedatastream.data)+len(smsdatastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (CALL & SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call and sms) in minutes within the given period"}]
    tmpphonestream = phonedatastream
    tmpsmsstream = smsdatastream
    for s in tmpphonestream.data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
    for s in tmpsmsstream.data:
        s.end_time = s.start_time

    combined_data = phonedatastream.data + smsdatastream.data

    combined_data.sort(key=lambda x:x.start_time)

    new_data = []
    for h in range(0, 24, 4):
        datalist = []
        start = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day, hour=h)
        end = start + datetime.timedelta(hours=3, minutes=59)
        for d in combined_data:
            if start<=d.start_time<=end or start<=d.end_time<=end:
                datalist.append(d)
        if len(datalist) <=1:
            continue
        new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=np.var(inter_event_time_list(datalist))))



    start_time = new_data[0].start_time
    end_time = new_data[-1].end_time

    return DataStream(identifier, phonedatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      new_data)



def average_inter_phone_call_time_hourly(phonedatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :return:
    """
    if len(phonedatastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (CALL & SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call and sms) in minutes within the given period"}]
    combined_data = phonedatastream.data

    for s in combined_data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

    new_data = []
    for h in range(0, 24):
        datalist = []
        start = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day, hour=h)
        end = start + datetime.timedelta(minutes=59)
        for d in combined_data:
            if start<=d.start_time<=end or start<=d.end_time<=end:
                datalist.append(d)
        if len(datalist) <=1:
            continue
        new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=sum(inter_event_time_list(datalist))/(len(datalist)-1)))



    start_time = new_data[0].start_time
    end_time = new_data[-1].end_time

    return DataStream(identifier, phonedatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      new_data)


def average_inter_phone_call_time_four_hourly(phonedatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :return:
    """
    if len(phonedatastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (CALL & SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call and sms) in minutes within the given period"}]
    combined_data = phonedatastream.data

    for s in combined_data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)


    new_data = []
    for h in range(0, 24, 4):
        datalist = []
        start = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day, hour=h)
        end = start + datetime.timedelta(hours=3, minutes=59)
        for d in combined_data:
            if start<=d.start_time<=end or start<=d.end_time<=end:
                datalist.append(d)
        if len(datalist) <=1:
            continue
        new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=sum(inter_event_time_list(datalist))/(len(datalist)-1)))



    start_time = new_data[0].start_time
    end_time = new_data[-1].end_time

    return DataStream(identifier, phonedatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      new_data)

def average_inter_phone_call_time_daily(phonedatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :return:
    """
    if len(phonedatastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (CALL & SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call and sms) in minutes within the given period"}]
    combined_data = phonedatastream.data

    for s in combined_data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

    start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day)
    end_time = start_time + datetime.timedelta(hours=23, minutes=59)
    new_data = [DataPoint(start_time=start_time, end_time=end_time, sample= sum(inter_event_time_list(combined_data)) / (len(combined_data)-1))]


    return DataStream(identifier, phonedatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      new_data)



def average_inter_sms_time_hourly(smsdatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :return:
    """
    if len(smsdatastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (CALL & SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call and sms) in minutes within the given period"}]
    combined_data = smsdatastream.data

    for s in combined_data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

    new_data = []
    for h in range(0, 24):
        datalist = []
        start = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day, hour=h)
        end = start + datetime.timedelta(minutes=59)
        for d in combined_data:
            if start<=d.start_time<=end or start<=d.end_time<=end:
                datalist.append(d)
        if len(datalist) <=1:
            continue
        new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=sum(inter_event_time_list(datalist))/(len(datalist)-1)))



    start_time = new_data[0].start_time
    end_time = new_data[-1].end_time

    return DataStream(identifier, smsdatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      new_data)


def average_inter_sms_time_four_hourly(smsdatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :return:
    """
    if len(smsdatastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (CALL & SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call and sms) in minutes within the given period"}]
    combined_data = smsdatastream.data

    for s in combined_data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)


    new_data = []
    for h in range(0, 24, 4):
        datalist = []
        start = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day, hour=h)
        end = start + datetime.timedelta(hours=3, minutes=59)
        for d in combined_data:
            if start<=d.start_time<=end or start<=d.end_time<=end:
                datalist.append(d)
        if len(datalist) <=1:
            continue
        new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=sum(inter_event_time_list(datalist))/(len(datalist)-1)))



    start_time = new_data[0].start_time
    end_time = new_data[-1].end_time

    return DataStream(identifier, smsdatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      new_data)

def average_inter_sms_time_daily(smsdatastream: DataStream):
    """

    :param phonedatastream: phone call duration stream
    :return:
    """
    if len(smsdatastream.data) <=1:
        return None

    identifier = uuid.uuid1()
    name = 'AVG. INTER EVENT TIME (CALL & SMS)'
    execution_context = {}
    annotations = {}
    data_descriptor = [{"NAME":"Average inter event time (call and sms)", "DATA_TYPE":"float", "DESCRIPTION": "Average inter event time (call and sms) in minutes within the given period"}]
    combined_data = smsdatastream.data

    for s in combined_data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

    start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day)
    end_time = start_time + datetime.timedelta(hours=23, minutes=59)
    new_data = [DataPoint(start_time=start_time, end_time=end_time, sample= sum(inter_event_time_list(combined_data)) / (len(combined_data)-1))]


    return DataStream(identifier, smsdatastream.owner, name, data_descriptor,
                      execution_context,
                      annotations,
                      "1",
                      start_time,
                      end_time,
                      new_data)
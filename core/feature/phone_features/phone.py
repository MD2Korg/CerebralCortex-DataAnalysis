from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint
from computefeature import ComputeFeatureBase
from save_feature_stream import store_data
#import argparse

import  uuid
import datetime
import time
import numpy as np


feature_class_name='PhoneFeatures'

def inter_event_time_list(data):
    if len(data)==0:
        return None

    last_end = data[0].end_time

    ret = []
    flag = False
    for cd in data:
        if flag == False:
            flag = True
            continue
        dif = cd.start_time - last_end
        ret.append(max(0, dif.total_seconds()))
        last_end = max(last_end, cd.end_time)

    return list(map(lambda x: x/60.0, ret))


def average_inter_phone_call_sms_time_hourly(phonedatastream: DataStream, smsdatastream: DataStream):

    if len(phonedatastream.data)+len(smsdatastream.data) <=1:
        return None

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

    return new_data


def average_inter_phone_call_sms_time_four_hourly(phonedatastream: DataStream, smsdatastream: DataStream):

    if len(phonedatastream.data)+len(smsdatastream.data) <=1:
        return None

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

    return new_data



def average_inter_phone_call_sms_time_daily(phonedatastream: DataStream, smsdatastream: DataStream):

    if len(phonedatastream.data)+len(smsdatastream.data) <=1:
        return None

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
    new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset, sample= sum(inter_event_time_list(combined_data)) / (len(combined_data)-1))]

    return new_data


def variance_inter_phone_call_sms_time_daily(phonedatastream: DataStream, smsdatastream: DataStream):

    if len(phonedatastream.data)+len(smsdatastream.data) <=1:
        return None

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

    new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset, sample= np.var(inter_event_time_list(combined_data)) )]

    return new_data


def variance_inter_phone_call_sms_time_hourly(phonedatastream: DataStream, smsdatastream: DataStream):

    if len(phonedatastream.data)+len(smsdatastream.data) <=1:
        return None

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

    return new_data


def variance_inter_phone_call_sms_time_four_hourly(phonedatastream: DataStream, smsdatastream: DataStream):

    if len(phonedatastream.data)+len(smsdatastream.data) <=1:
        return None

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

    return new_data



def average_inter_phone_call_time_hourly(phonedatastream: DataStream):

    if len(phonedatastream.data) <=1:
        return None

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

    return new_data


def average_inter_phone_call_time_four_hourly(phonedatastream: DataStream):

    if len(phonedatastream.data) <=1:
        return None

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

    return new_data


def average_inter_phone_call_time_daily(phonedatastream: DataStream):

    if len(phonedatastream.data) <=1:
        return None

    combined_data = phonedatastream.data

    for s in combined_data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

    start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day)
    end_time = start_time + datetime.timedelta(hours=23, minutes=59)
    new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset, sample= sum(inter_event_time_list(combined_data)) / (len(combined_data)-1))]

    return new_data



def average_inter_sms_time_hourly(smsdatastream: DataStream):

    if len(smsdatastream.data) <=1:
        return None

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

    return new_data


def average_inter_sms_time_four_hourly(smsdatastream: DataStream):

    if len(smsdatastream.data) <=1:
        return None

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

    return new_data


def average_inter_sms_time_daily(smsdatastream: DataStream):

    if len(smsdatastream.data) <=1:
        return None

    combined_data = smsdatastream.data

    for s in combined_data:
        s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

    start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day)
    end_time = start_time + datetime.timedelta(hours=23, minutes=59)
    new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset, sample= sum(inter_event_time_list(combined_data)) / (len(combined_data)-1))]

    return new_data


def all_users_data(study_name: str, CC):

    all_users = CC.get_all_users(study_name)

    if all_users:
        for user in all_users:
            streams = CC.get_user_streams(user["identifier"])
            process_data(user["identifier"], streams, CC)
    else:
        print(study_name, "- study has 0 users.")

#['CU_CALL_DURATION--edu.dartmouth.eureka', 'CU_SMS_LENGTH--edu.dartmouth.eureka']

def process_data(user_id, stream_names, CC):

    input_stream1 = {}
    input_stream2 = {}

    streams = stream_names
    for stream_name,stream_metadata in streams.items():
        if stream_name=='CU_CALL_DURATION--edu.dartmouth.eureka':
            callstream = CC.get_stream(stream_metadata["identifier"], day="20180102", data_type=DataSet.COMPLETE)
            input_stream1["id"] = stream_metadata["identifier"]
            input_stream1["name"] = stream_metadata["name"]
        elif stream_name=='CU_SMS_LENGTH--edu.dartmouth.eureka':
            smsstream = CC.get_stream(stream_metadata["identifier"], day="20180102", data_type=DataSet.COMPLETE)
            input_stream2["id"] = stream_metadata["identifier"]
            input_stream2["name"] = stream_metadata["name"]


    data = average_inter_phone_call_sms_time_hourly(callstream, smsstream)
    store_data("metadata/average_inter_phone_call_sms_time_hourly.json", [input_stream1, input_stream2], user_id, data, CC)

    data = average_inter_phone_call_sms_time_four_hourly(callstream, smsstream)
    store_data("metadata/average_inter_phone_call_sms_time_four_hourly.json", [input_stream1, input_stream2], user_id, data, CC)

    data = average_inter_phone_call_sms_time_daily(callstream, smsstream)
    store_data("metadata/average_inter_phone_call_sms_time_daily.json", [input_stream1, input_stream2], user_id, data, CC)

    data = variance_inter_phone_call_sms_time_daily(callstream, smsstream)
    store_data("metadata/variance_inter_phone_call_sms_time_daily.json", [input_stream1, input_stream2], user_id, data, CC)

    data = variance_inter_phone_call_sms_time_hourly(callstream, smsstream)
    store_data("metadata/variance_inter_phone_call_sms_time_hourly.json", [input_stream1, input_stream2], user_id, data, CC)

    data = variance_inter_phone_call_sms_time_four_hourly(callstream, smsstream)
    store_data("metadata/variance_inter_phone_call_sms_time_four_hourly.json", [input_stream1, input_stream2], user_id, data, CC)

    data = average_inter_phone_call_time_hourly(callstream)
    store_data("metadata/average_inter_phone_call_time_hourly.json", [input_stream1], user_id, data, CC)

    data = average_inter_phone_call_time_four_hourly(callstream)
    store_data("metadata/average_inter_phone_call_time_four_hourly.json", [input_stream1], user_id, data, CC)

    data = average_inter_phone_call_time_daily(callstream)
    store_data("metadata/average_inter_phone_call_time_daily.json", [input_stream1], user_id, data, CC)

    data = average_inter_sms_time_hourly(smsstream)
    store_data("metadata/average_inter_sms_time_hourly.json", [input_stream2], user_id, data, CC)

    data = average_inter_sms_time_four_hourly(smsstream)
    store_data("metadata/average_inter_sms_time_four_hourly.json", [input_stream2], user_id, data, CC)

    data = average_inter_sms_time_daily(smsstream)
    store_data("metadata/average_inter_sms_time_daily.json", [input_stream2], user_id, data, CC)


class PhoneFeatures(ComputeFeatureBase):
    def process(self):
        if self.CC is not None:
            print("Processing PhoneFeatures")
            all_users_data("mperf",  self.CC)
        
    
'''
if __name__ == '__main__':
    # create and load CerebralCortex object and configs
    parser = argparse.ArgumentParser(description='CerebralCortex Reporting Application.')
    parser.add_argument("-cc", "--cc_config_filepath", help="Configuration file path", required=True)
    args = vars(parser.parse_args())

    CC = CerebralCortex(args["cc_config_filepath"])

    # run for all the participants in a study
'''


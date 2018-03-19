from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint
from cerebralcortex.core.datatypes.stream_types import StreamTypes
from core.computefeature import ComputeFeatureBase

import datetime
import numpy as np
from datetime import timedelta


feature_class_name='PhoneFeatures'

class PhoneFeatures(ComputeFeatureBase):
    def inter_event_time_list(self, data):
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


    def average_inter_phone_call_sms_time_hourly(self, phonedatastream: DataStream, smsdatastream: DataStream):

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
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=sum(self.inter_event_time_list(datalist))/(len(datalist)-1)))

        return new_data


    def average_inter_phone_call_sms_time_four_hourly(self, phonedatastream: DataStream, smsdatastream: DataStream):

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
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=sum(self.inter_event_time_list(datalist))/(len(datalist)-1)))

        return new_data



    def average_inter_phone_call_sms_time_daily(self, phonedatastream: DataStream, smsdatastream: DataStream):

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
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset, sample= sum(self.inter_event_time_list(combined_data)) / (len(combined_data)-1))]

        return new_data


    def variance_inter_phone_call_sms_time_daily(self, phonedatastream: DataStream, smsdatastream: DataStream):

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

        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset, sample= np.var(self.inter_event_time_list(combined_data)) )]

        return new_data


    def variance_inter_phone_call_sms_time_hourly(self, phonedatastream: DataStream, smsdatastream: DataStream):

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
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=np.var(self.inter_event_time_list(datalist))))

        return new_data


    def variance_inter_phone_call_sms_time_four_hourly(self, phonedatastream: DataStream, smsdatastream: DataStream):

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
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=np.var(self.inter_event_time_list(datalist))))

        return new_data



    def average_inter_phone_call_time_hourly(self, phonedatastream: DataStream):

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
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=sum(self.inter_event_time_list(datalist))/(len(datalist)-1)))

        return new_data


    def average_inter_phone_call_time_four_hourly(self, phonedatastream: DataStream):

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
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=sum(self.inter_event_time_list(datalist))/(len(datalist)-1)))

        return new_data


    def average_inter_phone_call_time_daily(self, phonedatastream: DataStream):

        if len(phonedatastream.data) <=1:
            return None

        combined_data = phonedatastream.data

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset, sample= sum(self.inter_event_time_list(combined_data)) / (len(combined_data)-1))]

        return new_data



    def average_inter_sms_time_hourly(self, smsdatastream: DataStream):

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
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=sum(self.inter_event_time_list(datalist))/(len(datalist)-1)))

        return new_data


    def average_inter_sms_time_four_hourly(self, smsdatastream: DataStream):

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
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset, sample=sum(self.inter_event_time_list(datalist))/(len(datalist)-1)))

        return new_data


    def average_inter_sms_time_daily(self, smsdatastream: DataStream):

        if len(smsdatastream.data) <=1:
            return None

        combined_data = smsdatastream.data

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month, day=combined_data[0].start_time.day)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset, sample= sum(self.inter_event_time_list(combined_data)) / (len(combined_data)-1))]

        return new_data


    def process_day_data(self, user_id, callstream, smsstream, input_stream1, input_stream2):
        try:
            data = self.average_inter_phone_call_sms_time_hourly(callstream, smsstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_sms_time_hourly.json",
                                input_streams=[input_stream1, input_stream2],
                                user_id=user_id, data=data)
        except Exception as e:
            print("Exception:", str(e))
            
        try:
            data = self.average_inter_phone_call_sms_time_four_hourly(callstream, smsstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_sms_time_four_hourly.json",
                                input_streams=[input_stream1, input_stream2],
                                user_id=user_id, data=data)
        except Exception as e:
            print("Exception:",str(e))

        try:
            data = self.average_inter_phone_call_sms_time_daily(callstream, smsstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_sms_time_daily.json",
                                input_streams=[input_stream1, input_stream2],
                                user_id=user_id, data=data)
        except Exception as e:
            print("Exception:",str(e))

        try:
            self.data = self.variance_inter_phone_call_sms_time_daily(callstream, smsstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_sms_time_daily.json",
                                input_streams=[input_stream1, input_stream2],
                                user_id=user_id, data=data)
        except Exception as e:
            print("Exception:",str(e))

        try:
            data = self.variance_inter_phone_call_sms_time_hourly(callstream, smsstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_sms_time_hourly.json",
                                input_streams=[input_stream1, input_stream2],
                                user_id=user_id, data=data)
        except Exception as e:
            print("Exception:",str(e))

        try:
            data = self.variance_inter_phone_call_sms_time_four_hourly(callstream, smsstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_sms_time_four_hourly.json",
                                input_streams=[input_stream1, input_stream2],
                                user_id=user_id, data=data)
        except Exception as e:
            print("Exception:",str(e))

        try:
            data = self.average_inter_phone_call_time_hourly(callstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_time_hourly.json",
                                input_streams=[input_stream1], user_id=user_id,
                                data=data)
        except Exception as e:
            print("Exception:",str(e))

        try:
            data = self.average_inter_phone_call_time_four_hourly(callstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_time_four_hourly.json",
                                input_streams=[input_stream1], user_id=user_id,
                                data=data)
        except Exception as e:
            print("Exception:",str(e))

        try:
            data = self.average_inter_phone_call_time_daily(callstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_time_daily.json",
                                input_streams=[input_stream1], user_id=user_id,
                                data=data)
        except Exception as e:
            print("Exception:",str(e))

        try:
            data = self.average_inter_sms_time_hourly(smsstream)
            if data:
                self.store_stream(filepath="average_inter_sms_time_hourly.json",
                                input_streams=[input_stream2], user_id=user_id,
                                data=data)
        except Exception as e:
            print("Exception:",str(e))

        try:
            data = self.average_inter_sms_time_four_hourly(smsstream)
            if data:
                self.store_stream(filepath="average_inter_sms_time_four_hourly.json",
                                input_streams=[input_stream2], user_id=user_id,
                                data=data)
        except Exception as e:
            print("Exception:",str(e))

        try:
            data = self.average_inter_sms_time_daily(smsstream)
            if data:
                self.store_stream(filepath="average_inter_sms_time_daily.json",
                                input_streams=[input_stream2], user_id=user_id,
                                data=data)
        except Exception as e:
            print("Exception:",str(e))

        
        
    def process_data(self, user_id, all_user_streams, all_days):

        input_stream1 = None
        input_stream2 = None
        call_stream_name = 'CU_CALL_DURATION--edu.dartmouth.eureka'
        sms_stream_name = 'CU_SMS_LENGTH--edu.dartmouth.eureka' 
        streams = all_user_streams
        days = None

        if not len(streams):
            self.CC.logging.log('No streams found for user %s for feature %s'
                                % (str(user_id), self.__class__.__name__))
            return

        for stream_name,stream_metadata in streams.items():
            if stream_name==call_stream_name:
                input_stream1 = stream_metadata
            elif stream_name== sms_stream_name:
                input_stream2 = stream_metadata

        if not input_stream1:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" % 
                                (self.__class__.__name__, call_stream_name, 
                                 str(user_id)))
            return

        if not input_stream2:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %" % 
                                (self.__class__.__name__, sms_stream_name, 
                                 str(user_id)))
            return

        
        
        for day in all_days:
            callstream = self.CC.get_stream(input_stream1["identifier"], user_id=user_id, day=day)
            smsstream = self.CC.get_stream(input_stream2["identifier"], user_id=user_id, day=day)
            self.process_day_data(user_id,callstream,smsstream,input_stream1,input_stream2)
            

    def process(self, user_id, all_days):
        if self.CC is not None:
            print("Processing PhoneFeatures")
            streams = self.CC.get_user_streams(user_id)
            self.process_data(user_id, streams, all_days)

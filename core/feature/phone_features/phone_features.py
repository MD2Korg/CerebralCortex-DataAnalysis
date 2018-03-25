# Copyright (c) 2018, MD2K Center of Excellence
# - Md Shiplu Hawlader <shiplu.cse.du@gmail.com; mhwlader@memphis.edu>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint
from cerebralcortex.core.datatypes.stream_types import StreamTypes
from core.computefeature import ComputeFeatureBase

from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime
import numpy as np
from datetime import timedelta
import time
import copy
from functools import lru_cache

from sklearn.mixture import GaussianMixture


feature_class_name = 'PhoneFeatures'


class PhoneFeatures(ComputeFeatureBase):

    APPUSAGE_GAP_THRESHOLD = timedelta(minutes=2)

    def get_data_by_stream_name(self, stream_name, user_id, day, localtime=True):
        """
        method to get combined data from CerebralCortex as there can be multiple stream id for same stream
        :param stream_name: Name of the stream corresponding to the datastream
        :param user_id:
        :param day:
        :return: combined data if there are multiple stream id
        """

        stream_ids = self.CC.get_stream_id(user_id, stream_name)
        data = []
        for stream in stream_ids:
            data += self.CC.get_stream(stream['identifier'], user_id=user_id, day=day, localtime=localtime).data

        return data

    def inter_event_time_list(self, data):
        """
        Helper function to find inter event gaps
        :param data:
        :return:
        """
        if len(data) == 0:
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

        return list(map(lambda x: x / 60.0, ret))

    def average_inter_phone_call_sms_time_hourly(self, phonedatastream, smsdatastream):
        """

        :param phonedatastream:
        :param smsdatastream:
        :return:
        """

        if len(phonedatastream) + len(smsdatastream) <= 1:
            return None

        tmpphonestream = phonedatastream
        tmpsmsstream = smsdatastream
        for s in tmpphonestream:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
        for s in tmpsmsstream:
            s.end_time = s.start_time

        combined_data = phonedatastream + smsdatastream

        combined_data.sort(key=lambda x: x.start_time)

        new_data = []

        tmp_time = copy.deepcopy(combined_data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in combined_data:
                if start <= d.start_time <= end or start <= d.end_time <= end:
                    datalist.append(d)
            if len(datalist) <= 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset,
                                      sample=sum(self.inter_event_time_list(datalist)) / (len(datalist) - 1)))

        return new_data

    def average_inter_phone_call_sms_time_four_hourly(self, phonedatastream, smsdatastream):

        if len(phonedatastream) + len(smsdatastream) <= 1:
            return None

        tmpphonestream = phonedatastream
        tmpsmsstream = smsdatastream
        for s in tmpphonestream:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
        for s in tmpsmsstream:
            s.end_time = s.start_time

        combined_data = phonedatastream + smsdatastream

        combined_data.sort(key=lambda x: x.start_time)

        new_data = []
        tmp_time = copy.deepcopy(combined_data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in combined_data:
                if start <= d.start_time <= end or start <= d.end_time <= end:
                    datalist.append(d)
            if len(datalist) <= 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset,
                                      sample=sum(self.inter_event_time_list(datalist)) / (len(datalist) - 1)))

        return new_data

    def average_inter_phone_call_sms_time_daily(self, phonedatastream, smsdatastream):

        if len(phonedatastream) + len(smsdatastream) <= 1:
            return None

        tmpphonestream = phonedatastream
        tmpsmsstream = smsdatastream
        for s in tmpphonestream:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
        for s in tmpsmsstream:
            s.end_time = s.start_time

        combined_data = phonedatastream + smsdatastream

        combined_data.sort(key=lambda x: x.start_time)
        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month,
                                       day=combined_data[0].start_time.day, tzinfo=combined_data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset,
                              sample=sum(self.inter_event_time_list(combined_data)) / (len(combined_data) - 1))]

        return new_data

    def variance_inter_phone_call_sms_time_daily(self, phonedatastream, smsdatastream):

        if len(phonedatastream) + len(smsdatastream) <= 1:
            return None

        tmpphonestream = phonedatastream
        tmpsmsstream = smsdatastream
        for s in tmpphonestream:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
        for s in tmpsmsstream:
            s.end_time = s.start_time

        combined_data = phonedatastream + smsdatastream

        combined_data.sort(key=lambda x: x.start_time)
        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month,
                                       day=combined_data[0].start_time.day, tzinfo=combined_data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)

        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset,
                              sample=np.var(self.inter_event_time_list(combined_data)))]

        return new_data

    def variance_inter_phone_call_sms_time_hourly(self, phonedatastream, smsdatastream):

        if len(phonedatastream) + len(smsdatastream) <= 1:
            return None

        tmpphonestream = phonedatastream
        tmpsmsstream = smsdatastream
        for s in tmpphonestream:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
        for s in tmpsmsstream:
            s.end_time = s.start_time

        combined_data = phonedatastream + smsdatastream

        combined_data.sort(key=lambda x: x.start_time)

        new_data = []
        tmp_time = copy.deepcopy(combined_data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in combined_data:
                if start <= d.start_time <= end or start <= d.end_time <= end:
                    datalist.append(d)
            if len(datalist) <= 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset,
                                      sample=np.var(self.inter_event_time_list(datalist))))

        return new_data

    def variance_inter_phone_call_sms_time_four_hourly(self, phonedatastream, smsdatastream):

        if len(phonedatastream) + len(smsdatastream) <= 1:
            return None

        tmpphonestream = phonedatastream
        tmpsmsstream = smsdatastream
        for s in tmpphonestream:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
        for s in tmpsmsstream:
            s.end_time = s.start_time

        combined_data = phonedatastream + smsdatastream

        combined_data.sort(key=lambda x: x.start_time)

        new_data = []
        tmp_time = copy.deepcopy(combined_data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in combined_data:
                if start <= d.start_time <= end or start <= d.end_time <= end:
                    datalist.append(d)
            if len(datalist) <= 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset,
                                      sample=np.var(self.inter_event_time_list(datalist))))

        return new_data

    def average_inter_phone_call_time_hourly(self, phonedatastream):

        if len(phonedatastream) <= 1:
            return None

        combined_data = phonedatastream

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        new_data = []
        tmp_time = copy.deepcopy(combined_data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in combined_data:
                if start <= d.start_time <= end or start <= d.end_time <= end:
                    datalist.append(d)
            if len(datalist) <= 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset,
                                      sample=sum(self.inter_event_time_list(datalist)) / (len(datalist) - 1)))

        return new_data

    def average_inter_phone_call_time_four_hourly(self, phonedatastream):

        if len(phonedatastream) <= 1:
            return None

        combined_data = phonedatastream

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        new_data = []
        tmp_time = copy.deepcopy(combined_data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in combined_data:
                if start <= d.start_time <= end or start <= d.end_time <= end:
                    datalist.append(d)
            if len(datalist) <= 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset,
                                      sample=sum(self.inter_event_time_list(datalist)) / (len(datalist) - 1)))

        return new_data

    def average_inter_phone_call_time_daily(self, phonedatastream):

        if len(phonedatastream) <= 1:
            return None

        combined_data = phonedatastream

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month,
                                       day=combined_data[0].start_time.day, tzinfo=combined_data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset,
                              sample=sum(self.inter_event_time_list(combined_data)) / (len(combined_data) - 1))]

        return new_data

    def variance_inter_phone_call_time_hourly(self, phonedatastream):

        if len(phonedatastream) <= 1:
            return None

        combined_data = phonedatastream

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        new_data = []
        tmp_time = copy.deepcopy(combined_data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in combined_data:
                if start <= d.start_time <= end or start <= d.end_time <= end:
                    datalist.append(d)
            if len(datalist) <= 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset,
                                      sample=np.var(self.inter_event_time_list(datalist)) ))

        return new_data

    def variance_inter_phone_call_time_four_hourly(self, phonedatastream):

        if len(phonedatastream) <= 1:
            return None

        combined_data = phonedatastream

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        new_data = []
        tmp_time = copy.deepcopy(combined_data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in combined_data:
                if start <= d.start_time <= end or start <= d.end_time <= end:
                    datalist.append(d)
            if len(datalist) <= 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset,
                                      sample=np.var(self.inter_event_time_list(datalist)) ))

        return new_data

    def variance_inter_phone_call_time_daily(self, phonedatastream):

        if len(phonedatastream) <= 1:
            return None

        combined_data = phonedatastream

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month,
                                       day=combined_data[0].start_time.day, tzinfo=combined_data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset,
                              sample=np.var(self.inter_event_time_list(combined_data)) )]

        return new_data

    def average_inter_sms_time_hourly(self, smsdatastream):

        if len(smsdatastream) <= 1:
            return None

        combined_data = smsdatastream

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        new_data = []
        tmp_time = copy.deepcopy(combined_data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in combined_data:
                if start <= d.start_time <= end or start <= d.end_time <= end:
                    datalist.append(d)
            if len(datalist) <= 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset,
                                      sample=sum(self.inter_event_time_list(datalist)) / (len(datalist) - 1)))

        return new_data

    def average_inter_sms_time_four_hourly(self, smsdatastream):

        if len(smsdatastream) <= 1:
            return None

        combined_data = smsdatastream

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        new_data = []
        tmp_time = copy.deepcopy(combined_data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in combined_data:
                if start <= d.start_time <= end or start <= d.end_time <= end:
                    datalist.append(d)
            if len(datalist) <= 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset,
                                      sample=sum(self.inter_event_time_list(datalist)) / (len(datalist) - 1)))

        return new_data

    def average_inter_sms_time_daily(self, smsdatastream):

        if len(smsdatastream) <= 1:
            return None

        combined_data = smsdatastream

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month,
                                       day=combined_data[0].start_time.day, tzinfo=combined_data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset,
                              sample=sum(self.inter_event_time_list(combined_data)) / (len(combined_data) - 1))]

        return new_data

    def variance_inter_sms_time_hourly(self, smsdatastream):

        if len(smsdatastream) <= 1:
            return None

        combined_data = smsdatastream

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        new_data = []
        tmp_time = copy.deepcopy(combined_data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in combined_data:
                if start <= d.start_time <= end or start <= d.end_time <= end:
                    datalist.append(d)
            if len(datalist) <= 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset,
                                      sample=np.var(self.inter_event_time_list(datalist))))

        return new_data

    def variance_inter_sms_time_four_hourly(self, smsdatastream):

        if len(smsdatastream) <= 1:
            return None

        combined_data = smsdatastream

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        new_data = []
        tmp_time = copy.deepcopy(combined_data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in combined_data:
                if start <= d.start_time <= end or start <= d.end_time <= end:
                    datalist.append(d)
            if len(datalist) <= 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset,
                                      sample=np.var(self.inter_event_time_list(datalist))))

        return new_data

    def variance_inter_sms_time_daily(self, smsdatastream):

        if len(smsdatastream) <= 1:
            return None

        combined_data = smsdatastream

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month,
                                       day=combined_data[0].start_time.day, tzinfo=combined_data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset,
                              sample=np.var(self.inter_event_time_list(combined_data)) )]

        return new_data

    def average_call_duration_daily(self, phonedatastream):

        if len(phonedatastream) < 1:
            return None

        data = phonedatastream

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=sum([d.sample for d in data]) / len(data))]

        return new_data

    def average_call_duration_hourly(self, phonedatastream):

        if len(phonedatastream) < 1:
            return None

        data = copy.deepcopy(phonedatastream)
        for s in data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end and start <= d.end_time <= end:
                    datalist.append(d.sample)
                elif start <= d.start_time <= end:
                    datalist.append((end - d.start_time).total_seconds())
                elif start <= d.end_time <= end:
                    datalist.append((d.start_time - end).total_seconds())

            if len(datalist) < 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=sum(datalist) / len(datalist)))

        return new_data

    def average_call_duration_four_hourly(self, phonedatastream):

        if len(phonedatastream) < 1:
            return None

        data = copy.deepcopy(phonedatastream)
        for s in data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(hours = 3, minutes=59)
            for d in data:
                if start <= d.start_time <= end and start <= d.end_time <= end:
                    datalist.append(d.sample)
                elif start <= d.start_time <= end:
                    datalist.append((end - d.start_time).total_seconds())
                elif start <= d.end_time <= end:
                    datalist.append((d.start_time - end).total_seconds())

            if len(datalist) < 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=sum(datalist) / len(datalist)))

        return new_data

    def average_sms_length_daily(self, smsdata):

        if len(smsdata) < 1:
            return None

        data = smsdata

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=sum([d.sample for d in data]) / len(data))]

        return new_data

    def average_sms_length_hourly(self, smsdata):

        if len(smsdata) < 1:
            return None

        data = smsdata

        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=sum(datalist) / len(datalist)))

        return new_data

    def average_sms_length_four_hourly(self, smsdata):

        if len(smsdata) < 1:
            return None

        data = smsdata

        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(hours = 3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=sum(datalist) / len(datalist)))

        return new_data

    def variance_sms_length_daily(self, smsdata):

        if len(smsdata) < 1:
            return None

        data = smsdata

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=np.var([d.sample for d in data]))]

        return new_data

    def variance_sms_length_hourly(self, smsdata):

        if len(smsdata) < 1:
            return None

        data = smsdata

        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=np.var(datalist) ))

        return new_data

    def variance_sms_length_four_hourly(self, smsdata):

        if len(smsdata) < 1:
            return None

        data = smsdata

        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(hours = 3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=np.var(datalist)))

        return new_data

    def variance_call_duration_daily(self, phonedatastream):

        if len(phonedatastream) < 1:
            return None

        data = phonedatastream

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=np.var([d.sample for d in data]) )]

        return new_data

    def variance_call_duration_hourly(self, phonedatastream):

        if len(phonedatastream) < 1:
            return None

        data = copy.deepcopy(phonedatastream)
        for s in data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end and start <= d.end_time <= end:
                    datalist.append(d.sample)
                elif start <= d.start_time <= end:
                    datalist.append((end - d.start_time).total_seconds())
                elif start <= d.end_time <= end:
                    datalist.append((d.start_time - end).total_seconds())

            if len(datalist) < 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=np.var(datalist) ))

        return new_data

    def variance_call_duration_four_hourly(self, phonedatastream):

        if len(phonedatastream) < 1:
            return None

        data = copy.deepcopy(phonedatastream)
        for s in data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(hours = 3, minutes=59)
            for d in data:
                if start <= d.start_time <= end and start <= d.end_time <= end:
                    datalist.append(d.sample)
                elif start <= d.start_time <= end:
                    datalist.append((end - d.start_time).total_seconds())
                elif start <= d.end_time <= end:
                    datalist.append((d.start_time - end).total_seconds())

            if len(datalist) < 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=np.var(datalist)))

        return new_data

    def average_ambient_light_daily(self, lightdata, frequency = 16, minimum_data_percent = 40):
        if len(lightdata) < frequency * 24 * 60 * 60 * minimum_data_percent / 100:
            return None
        start_time = datetime.datetime.combine(lightdata[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=lightdata[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours = 23, minutes = 59)
        return [DataPoint(start_time, end_time, lightdata[0].offset, np.mean([x.sample for x in lightdata]))]

    def average_ambient_light_hourly(self, lightdata, data_frequency = 16, minimum_data_percent = 40):

        if len(lightdata) < 1:
            return None

        data = lightdata

        new_data = []
        tmp_time = copy.deepcopy(data[0].start_time)
        tmp_time = tmp_time.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < data_frequency * 60 * 60 * minimum_data_percent / 100:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=np.mean(datalist) ))

        return new_data

    def average_ambient_light_four_hourly(self, lightdata, data_frequency = 16, minimum_data_percent = 40):

        if len(lightdata) < 1:
            return None

        data = lightdata

        new_data = []
        tmp_time = copy.deepcopy(data[0].start_time)
        tmp_time = tmp_time.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(hours = 3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < data_frequency * 4 * 60 * 60 * minimum_data_percent / 100:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=np.mean(datalist) ))

        return new_data

    def variance_ambient_light_daily(self, lightdata, data_frequency = 16, minimum_data_percent = 40):
        if len(lightdata) < data_frequency * 24 * 60 * 60 * minimum_data_percent / 100:
            return None
        start_time = datetime.datetime.combine(lightdata[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=lightdata[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours = 23, minutes = 59)
        return [DataPoint(start_time, end_time, lightdata[0].offset, np.var([x.sample for x in lightdata]))]

    def variance_ambient_light_hourly(self, lightdata, data_frequency = 16, minimum_data_percent = 40):

        if len(lightdata) < 1:
            return None

        data = lightdata

        new_data = []
        tmp_time = copy.deepcopy(data[0].start_time)
        tmp_time = tmp_time.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < data_frequency * 60 * 60 * minimum_data_percent / 100:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=np.var(datalist) ))

        return new_data

    def variance_ambient_light_four_hourly(self, lightdata, data_frequency=16, minimum_data_percent=40):

        if len(lightdata) < 1:
            return None

        data = lightdata

        new_data = []
        tmp_time = copy.deepcopy(data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(hours = 3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < data_frequency * 4 * 60 * 60 * minimum_data_percent / 100:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=np.var(datalist) ))

        return new_data

    def calculate_phone_outside_duration(data, phone_inside_threshold_second=60):
        outside_data = []
        threshold = timedelta(seconds=phone_inside_threshold_second)
        L = len(data)

        i = 0
        while i < L and data[i].sample == 0:
            i += 1
        if i == L:
            return outside_data

        start = data[i].start_time

        while i < L:

            while i < L and data[i].sample > 0:
                i += 1
            if i == L:
                outside_data.append(DataPoint(start, data[i - 1].start_time, data[i - 1].offset, "Outside"))
                break

            cur = data[i].start_time
            while i < L and data[i].sample == 0:
                i += 1

            if i == L or i < L and data[i].start_time - cur >= threshold:
                outside_data.append(DataPoint(start, cur, data[0].offset, "Outside"))
                if i < L:
                    start = data[i].start_time

        return outside_data

    @lru_cache(maxsize=256)
    def get_app_category(self, appid):
        appid = appid.strip()
        time.sleep(1.0)
        if appid == "com.samsung.android.messaging":
            return [appid, "Communication", "Samsung Message", None]

        url = "https://play.google.com/store/apps/details?id=" + appid
        try:
            response = urlopen(url)
        except Exception:
            return [appid, None, None, None]

        soup = BeautifulSoup(response, 'html.parser')
        text = soup.find('span', itemprop='genre')

        name = soup.find('div', class_='id-app-title')

        cat = soup.find('a', class_='document-subtitle category')
        if cat:
            category = cat.attrs['href'].split('/')[-1]
        else:
            category = None

        if category and category.startswith('GAME_'):
            return [appid, "Game", name.contents[0] if name else None, text.contents[0]]
        elif text:
            return [appid, text.contents[0], name.contents[0] if name else None, None]
        else:
            return [appid, None, name.contents[0] if name else None, None]

    def get_appusage_duration_by_category(self, appdata, categories: list):
        appdata = sorted(appdata, key=lambda x: x.start_time)
        appusage = []

        i = 0
        while i< len(appdata):
            d = appdata[i]
            category = d.sample[1]
            if category not in categories:
                i += 1
                continue
            j = i+1
            while j<len(appdata) and d.sample == appdata[j].sample \
                    and appdata[j-1].start_time + self.APPUSAGE_GAP_THRESHOLD <= appdata[j].start_time:
                j += 1

            if j > i+1:
                appusage.append([d.start_time, appdata[j-1].start_time, category])
                i = j-1
            i += 1

        return appusage

    def appusage_interval_list(self, data, appusage):
        ret = []
        i = 0
        for a in appusage:
            while i<len(data) and data[i].start_time<a[0]:
                i += 1
            last = 0
            while i<len(data) and data[i].start_time <= a[1]:
                if last > 0:
                    ret.append(int(data[i].sample - last))
                last = data[i].sample
                i += 1
        return ret

    def label_appusage_intervals(self, data, appusage, intervals, interval_label):
        ret = []
        i = 0
        for a in appusage:
            while i<len(data) and data[i].start_time<a[0]:
                i += 1
            last = None
            while i<len(data) and data[i].start_time <= a[1]:
                if last:
                    diff = (data[i].start_time - last).total_seconds()
                    for j in range(len(interval_label)):
                        if intervals[j][0] <= diff <= intervals[j][1]:
                            if len(ret) > 0:
                                last_entry = ret.pop()
                                if last_entry.end_time == last and last_entry.sample == interval_label[j]:
                                    ret.append(DataPoint(start_time = last_entry.start_time,
                                                         end_time = data[i].start_time, offset = last_entry.offset,
                                                         sample = last_entry.sample))
                                else:
                                    ret.append(last_entry)
                                    ret.append(DataPoint(start_time = last, end_time = data[i].start_time,
                                                         offset = data[i].offset, sample=interval_label[j]))
                            else:
                                ret.append(DataPoint(start_time = last, end_time = data[i].start_time,
                                                     offset = data[i].offset, sample=interval_label[j]))
                            break;
                last = data[i].start_time
                i += 1
        return ret

    def process_phonescreen_day_data(self, user_id, touchstream, categorystream, input_touchstream, input_categorystream):
        touchstream = sorted(touchstream, key=lambda x: x.start_time)

        appusage = self.get_appusage_duration_by_category(categorystream, ["Communication", "Productivity"])
        tapping_gap = self.appusage_interval_list(touchstream, appusage)
        tapping_gap = sorted(tapping_gap)

        gm = GaussianMixture(n_components = 4, max_iter = 500)#, covariance_type = 'spherical')
        X = (np.array(tapping_gap)/1000).reshape(-1, 1)
        gm.fit(X)

        P = gm.predict(X)
        mx = np.zeros(4)
        mn = np.full(4, np.inf)
        for i in range(len(P)):
            x = P[i]
            mx[x] = max(mx[x], X[i][0])
            mn[x] = min(mn[x], X[i][0])

        intervals = []
        for i in range(len(mx)):
            intervals.append((mn[i], mx[i]))
        intervals = sorted(intervals)

        try:
            data = self.label_appusage_intervals(touchstream, appusage, intervals, ["typing", "pause", "reading", "unknown"])
            if data:
                self.store_stream(filepath="phone_touch_type.json",
                                  input_streams=[input_touchstream, input_categorystream],
                                  user_id=user_id, data=data)
        except Exception as e:
            print("Exception:", str(e))

    def process_callsmsstream_day_data(self, user_id, callstream, smsstream, input_callstream, input_smsstream):
        try:
            data = self.average_inter_phone_call_sms_time_hourly(callstream, smsstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_sms_time_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_inter_phone_call_sms_time_four_hourly(callstream, smsstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_sms_time_four_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_inter_phone_call_sms_time_daily(callstream, smsstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_sms_time_daily.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_inter_phone_call_sms_time_daily(callstream, smsstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_sms_time_daily.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_inter_phone_call_sms_time_hourly(callstream, smsstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_sms_time_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_inter_phone_call_sms_time_four_hourly(callstream, smsstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_sms_time_four_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_inter_phone_call_time_hourly(callstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_time_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_inter_phone_call_time_four_hourly(callstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_time_four_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_inter_phone_call_time_daily(callstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_time_daily.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_inter_phone_call_time_hourly(callstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_time_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_inter_phone_call_time_four_hourly(callstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_time_four_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_inter_phone_call_time_daily(callstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_time_daily.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_inter_sms_time_hourly(smsstream)
            if data:
                self.store_stream(filepath="average_inter_sms_time_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_inter_sms_time_four_hourly(smsstream)
            if data:
                self.store_stream(filepath="average_inter_sms_time_four_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_inter_sms_time_daily(smsstream)
            if data:
                self.store_stream(filepath="average_inter_sms_time_daily.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_inter_sms_time_hourly(smsstream)
            if data:
                self.store_stream(filepath="variance_inter_sms_time_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_inter_sms_time_four_hourly(smsstream)
            if data:
                self.store_stream(filepath="variance_inter_sms_time_four_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_inter_sms_time_daily(smsstream)
            if data:
                self.store_stream(filepath="variance_inter_sms_time_daily.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_call_duration_daily(callstream)
            if data:
                self.store_stream(filepath="average_call_duration_daily.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_call_duration_hourly(callstream)
            if data:
                self.store_stream(filepath="average_call_duration_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_call_duration_four_hourly(callstream)
            if data:
                self.store_stream(filepath="average_call_duration_four_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_sms_length_daily(smsstream)
            if data:
                self.store_stream(filepath="average_sms_length_daily.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_sms_length_hourly(smsstream)
            if data:
                self.store_stream(filepath="average_sms_length_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_sms_length_four_hourly(smsstream)
            if data:
                self.store_stream(filepath="average_sms_length_four_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_sms_length_daily(smsstream)
            if data:
                self.store_stream(filepath="variance_sms_length_daily.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_sms_length_hourly(smsstream)
            if data:
                self.store_stream(filepath="variance_sms_length_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_sms_length_four_hourly(smsstream)
            if data:
                self.store_stream(filepath="variance_sms_length_four_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_call_duration_daily(callstream)
            if data:
                self.store_stream(filepath="variance_call_duration_daily.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_call_duration_hourly(callstream)
            if data:
                self.store_stream(filepath="variance_call_duration_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_call_duration_four_hourly(callstream)
            if data:
                self.store_stream(filepath="variance_call_duration_four_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

    def process_light_day_data(self, user_id, lightdata, input_lightstream):
        try:
            data = self.average_ambient_light_daily(lightdata)
            if data:
                self.store_stream(filepath="average_ambient_light_daily.json",
                                  input_streams=[input_lightstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_ambient_light_hourly(lightdata)
            if data:
                self.store_stream(filepath="average_ambient_light_hourly.json",
                                  input_streams=[input_lightstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.average_ambient_light_four_hourly(lightdata)
            if data:
                self.store_stream(filepath="average_ambient_light_four_hourly.json",
                                  input_streams=[input_lightstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_ambient_light_daily(lightdata)
            if data:
                self.store_stream(filepath="variance_ambient_light_daily.json",
                                  input_streams=[input_lightstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_ambient_light_hourly(lightdata)
            if data:
                self.store_stream(filepath="variance_ambient_light_hourly.json",
                                  input_streams=[input_lightstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

        try:
            data = self.variance_ambient_light_four_hourly(lightdata)
            if data:
                self.store_stream(filepath="variance_ambient_light_four_hourly.json",
                                  input_streams=[input_lightstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            print("Exception:", str(e))

    def process_proximity_day_data(self, user_id, proximitystream, input_proximitystream):
        try:
            data = self.calculate_phone_outside_duration(proximitystream)
            if data:
                self.store_stream(filepath="phone_outside_duration.json",
                                  input_streams=[input_proximitystream],
                                  user_id=user_id, data=data)
        except Exception as e:
            print("Exception:", str(e))

    def process_appcategory_day_data(self, user_id, appcategorystream, input_appcategorystream):
        try:
            data = []
            for d in appcategorystream:
                if type(d.sample) is not str:
                    continue
                dnew = d
                dnew.sample = self.get_app_category(d.sample)
                data.append(dnew)
                
            if data:
                self.store_stream(filepath="app_usage_category.json",
                                  input_streams=[input_appcategorystream],
                                  user_id=user_id, data=data)
        except Exception as e:
            print("Exception:", str(e))

    def process_data(self, user_id, all_user_streams, all_days):

        input_callstream = None
        input_smsstream = None
        input_proximitystream = None
        input_appusagestream = None
        input_touchscreenstream = None
        input_appcategorystream = None
        input_lightstream = None

        call_stream_name = 'CU_CALL_DURATION--edu.dartmouth.eureka'
        sms_stream_name = 'CU_SMS_LENGTH--edu.dartmouth.eureka'
        proximity_stream_name = 'PROXIMITY--org.md2k.phonesensor--PHONE'
        appusage_stream_name = 'CU_APPUSAGE--edu.dartmouth.eureka'
        touchescreen_stream_name = "TOUCH_SCREEN--org.md2k.phonesensor--PHONE"
        appcategory_stream_name = "org.md2k.data_analysis.feature.phone.app_usage_category"
        light_stream_name = 'AMBIENT_LIGHT--org.md2k.phonesensor--PHONE'
        streams = all_user_streams
        days = None

        if not len(streams):
            self.CC.logging.log('No streams found for user %s for feature %s'
                                % (str(user_id), self.__class__.__name__))
            return

        for stream_name, stream_metadata in streams.items():
            if stream_name == call_stream_name:
                input_callstream = stream_metadata
            elif stream_name == sms_stream_name:
                input_smsstream = stream_metadata
            elif stream_name == proximity_stream_name:
                input_proximitystream = stream_metadata
            elif stream_name == appusage_stream_name:
                input_appusagestream = stream_metadata
            elif stream_name == touchescreen_stream_name:
                input_touchscreenstream = stream_metadata
            elif stream_name == appcategory_stream_name:
                input_appcategorystream = stream_metadata
            elif stream_name == light_stream_name:
                input_lightstream = stream_metadata

        # Processing Call and SMS related features
        if not input_callstream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, call_stream_name,
                                 str(user_id)))

        elif not input_smsstream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %" %
                                (self.__class__.__name__, sms_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                callstream = self.get_data_by_stream_name(call_stream_name, user_id, day, localtime=False)
                smsstream = self.get_data_by_stream_name(sms_stream_name, user_id, day, localtime=False)
                self.process_callsmsstream_day_data(user_id, callstream, smsstream, input_callstream, input_smsstream)

        # processing proximity sensor related features
        if not input_proximitystream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %" %
                                (self.__class__.__name__, proximity_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                proximitystream = self.get_data_by_stream_name(proximity_stream_name, user_id, day)
                self.process_proximity_day_data(user_id, proximitystream, input_proximitystream)

        # processing app usage and category related features
        if not input_appusagestream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %" %
                                (self.__class__.__name__, appusage_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                appusagestream = self.get_data_by_stream_name(appusage_stream_name, user_id, day)
                self.process_appcategory_day_data(user_id, appusagestream, input_appusagestream)

        # Processing phone touche and typing related features
        if not input_touchscreenstream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, touchescreen_stream_name,
                                 str(user_id)))

        elif not input_appcategorystream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %" %
                                (self.__class__.__name__, appcategory_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                touchstream = self.get_data_by_stream_name(touchescreen_stream_name, user_id, day)
                appcategorystream  = self.get_data_by_stream_name(appcategory_stream_name, user_id, day)
                self.process_callsmsstream_day_data(user_id, touchstream, appcategorystream, input_touchscreenstream, input_appcategorystream)

        # Processing ambient light related features

        if not input_lightstream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %" %
                                (self.__class__.__name__, light_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                lightstream = self.get_data_by_stream_name(light_stream_name, user_id, day, localtime=False)
                self.process_light_day_data(user_id, lightstream, input_lightstream)


    def process(self, user_id, all_days):
        if self.CC is not None:
            print("Processing PhoneFeatures")
            streams = self.CC.get_user_streams(user_id)
            self.process_data(user_id, streams, all_days)

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
import math

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
import traceback
from functools import lru_cache
import math

from sklearn.mixture import GaussianMixture


feature_class_name = 'PhoneFeatures'


class PhoneFeatures(ComputeFeatureBase):

    def get_filtered_data(self, data, admission_control = None):
        if admission_control is None:
            return data
        return [d for d in data if admission_control(d.sample)]

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
            if stream is not None:
                ds = self.CC.get_stream(stream['identifier'], user_id=user_id, day=day, localtime=localtime)
                if ds is not None:
                    if ds.data is not None:
                        data += ds.data
        if len(stream_ids)>1:
            data = sorted(data, key=lambda x: x.start_time)
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

    def average_inter_phone_call_sms_time_hourly(self, phonedata, smsdata):
        """
        Average time (in minutes) between two consecutive events (call and sms)
        for each hour window. If there is not enough data for a window then
        there will be no data point for that window.
        :param phonedata:
        :param smsdata:
        :return:
        """
        tmpphonestream = self.get_filtered_data(phonedata)
        tmpsmsstream = self.get_filtered_data(smsdata)
        if len(tmpphonestream) + len(tmpsmsstream) <= 1:
            return None


        for s in tmpphonestream:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
        for s in tmpsmsstream:
            s.end_time = s.start_time

        combined_data = phonedata + smsdata

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

    def average_inter_phone_call_sms_time_four_hourly(self, phonedata, smsdata):
        """
        Average time (in minutes) between two consecutive events (call and sms)
        for each four hour window. If there is not enough data for a window then
        there will be no data point for that window.
        :param phonedata:
        :param smsdata:
        :return:
        """
        tmpphonestream = self.get_filtered_data(phonedata)
        tmpsmsstream = self.get_filtered_data(smsdata)
        if len(tmpphonestream) + len(tmpsmsstream) <= 1:
            return None
        for s in tmpphonestream:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
        for s in tmpsmsstream:
            s.end_time = s.start_time

        combined_data = phonedata + smsdata

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

    def average_inter_phone_call_sms_time_daily(self, phonedata, smsdata):
        """
        Average time (in minutes) between two consecutive events (call and sms)
        for whole day. If there is not enough data then it will return None.
        :param phonedata:
        :param smsdata:
        :return:
        """
        if len(phonedata) + len(smsdata) <= 1:
            return None

        tmpphonestream = phonedata
        tmpsmsstream = smsdata
        for s in tmpphonestream:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
        for s in tmpsmsstream:
            s.end_time = s.start_time

        combined_data = phonedata + smsdata

        combined_data.sort(key=lambda x: x.start_time)
        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month,
                                       day=combined_data[0].start_time.day, tzinfo=combined_data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset,
                              sample=sum(self.inter_event_time_list(combined_data)) / (len(combined_data) - 1))]

        return new_data

    def variance_inter_phone_call_sms_time_daily(self, phonedata, smsdata):
        """
        Variance of time (in minutes) between two consecutive events (call and sms)
        for whole day. If there is not enough data then it will return None.
        :param phonedata:
        :param smsdata:
        :return:
        """
        if len(phonedata) + len(smsdata) <= 1:
            return None

        tmpphonestream = phonedata
        tmpsmsstream = smsdata
        for s in tmpphonestream:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
        for s in tmpsmsstream:
            s.end_time = s.start_time

        combined_data = phonedata + smsdata

        combined_data.sort(key=lambda x: x.start_time)
        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month,
                                       day=combined_data[0].start_time.day, tzinfo=combined_data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)

        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset,
                              sample=np.var(self.inter_event_time_list(combined_data)))]

        return new_data

    def variance_inter_phone_call_sms_time_hourly(self, phonedata, smsdata):
        """
        Variance of time (in minutes) between two consecutive events (call and sms)
        for each hour window. If there is not enough data for a window then
        there will be no data point for that window.
        :param phonedata:
        :param smsdata:
        :return:
        """
        if len(phonedata) + len(smsdata) <= 1:
            return None

        tmpphonestream = phonedata
        tmpsmsstream = smsdata
        for s in tmpphonestream:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
        for s in tmpsmsstream:
            s.end_time = s.start_time

        combined_data = phonedata + smsdata

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

    def variance_inter_phone_call_sms_time_four_hourly(self, phonedata, smsdata):
        """
        Variance of time (in minutes) between two consecutive events (call and sms)
        for each four hour window. If there is not enough data for a window then
        there will be no data point for that window.
        :param phonedata:
        :param smsdata:
        :return:
        """
        if len(phonedata) + len(smsdata) <= 1:
            return None

        tmpphonestream = phonedata
        tmpsmsstream = smsdata
        for s in tmpphonestream:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)
        for s in tmpsmsstream:
            s.end_time = s.start_time

        combined_data = phonedata + smsdata

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

    def average_inter_phone_call_time_hourly(self, phonedata):
        """
        Average time (in minutes) between two consecutive call for each hour window.
        If there is not enough data for a window then there will be no data point for that window.
        :param phonedata:
        :return:
        """
        if len(phonedata) <= 1:
            return None

        combined_data = phonedata

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

    def average_inter_phone_call_time_four_hourly(self, phonedata):
        """
        Average time (in minutes) between two consecutive call for each four hour window.
        If there is not enough data for a window then there will be no data point for that window.
        :param phonedata:
        :return:
        """
        if len(phonedata) <= 1:
            return None

        combined_data = phonedata

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

    def average_inter_phone_call_time_daily(self, phonedata):
        """
        Average time (in minutes) between two consecutive call for a whole day.
        If there is not enough data for the day then it will return None.
        :param phonedata:
        :return:
        """
        if len(phonedata) <= 1:
            return None

        combined_data = phonedata

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month,
                                       day=combined_data[0].start_time.day, tzinfo=combined_data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset,
                              sample=sum(self.inter_event_time_list(combined_data)) / (len(combined_data) - 1))]

        return new_data

    def variance_inter_phone_call_time_hourly(self, phonedata):
        """
        Variance of time (in minutes) between two consecutive call for each hour window.
        If there is not enough data for a window then there will be no data point for that window.
        :param phonedata:
        :return:
        """
        if len(phonedata) <= 1:
            return None

        combined_data = phonedata

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

    def variance_inter_phone_call_time_four_hourly(self, phonedata):
        """
        Variance of time (in minutes) between two consecutive call for each four hour window.
        If there is not enough data for a window then there will be no data point for that window.
        :param phonedata:
        :return:
        """
        if len(phonedata) <= 1:
            return None

        combined_data = phonedata

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

    def variance_inter_phone_call_time_daily(self, phonedata):
        """
        Average time (in minutes) between two consecutive call for a day.
        If there is not enough data for the day then it will return None.
        :param phonedata:
        :return:
        """
        if len(phonedata) <= 1:
            return None

        combined_data = phonedata

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month,
                                       day=combined_data[0].start_time.day, tzinfo=combined_data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset,
                              sample=np.var(self.inter_event_time_list(combined_data)) )]

        return new_data

    def average_inter_sms_time_hourly(self, smsdata):
        """
        Average time (in minutes) between two consecutive sms for each hour window.
        If there is not enough data for a window then there will be no data point for that window.
        :param smsdata:
        :return:
        """
        if len(smsdata) <= 1:
            return None

        combined_data = smsdata

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

    def average_inter_sms_time_four_hourly(self, smsdata):
        """
        Average time (in minutes) between two consecutive sms for each four hour window.
        If there is not enough data for a window then there will be no data point for that window.
        :param smsdata:
        :return:
        """
        if len(smsdata) <= 1:
            return None

        combined_data = smsdata

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

    def average_inter_sms_time_daily(self, smsdata):
        """
        Average time (in minutes) between two consecutive sms for a day.
        If there is not enough data for the day then it will return None.
        :param smsdata:
        :return:
        """
        if len(smsdata) <= 1:
            return None

        combined_data = smsdata

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month,
                                       day=combined_data[0].start_time.day, tzinfo=combined_data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset,
                              sample=sum(self.inter_event_time_list(combined_data)) / (len(combined_data) - 1))]

        return new_data

    def variance_inter_sms_time_hourly(self, smsdata):
        """
        Variance of time (in minutes) between two consecutive sms for each hour window.
        If there is not enough data for a window then there will be no data point for that window.
        :param smsdata:
        :return:
        """
        if len(smsdata) <= 1:
            return None

        combined_data = smsdata

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

    def variance_inter_sms_time_four_hourly(self, smsdata):
        """
        Average time (in minutes) between two consecutive sms for each four hour window.
        If there is not enough data for a window then there will be no data point for that window.
        :param smsdata:
        :return:
        """
        if len(smsdata) <= 1:
            return None

        combined_data = smsdata

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

    def variance_inter_sms_time_daily(self, smsdata):
        """
        Average time (in minutes) between two consecutive sms for a day.
        If there is not enough data for that day, then it will return None.
        :param smsdata:
        :return:
        """
        if len(smsdata) <= 1:
            return None

        combined_data = smsdata

        for s in combined_data:
            s.end_time = s.start_time + datetime.timedelta(seconds=s.sample)

        start_time = datetime.datetime(year=combined_data[0].start_time.year, month=combined_data[0].start_time.month,
                                       day=combined_data[0].start_time.day, tzinfo=combined_data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=combined_data[0].offset,
                              sample=np.var(self.inter_event_time_list(combined_data)) )]

        return new_data

    def average_call_duration_daily(self, phonedata):
        """
        Average time (in minutes) spent in call in a day. If there is not enough data
        for that day then it will return None.
        :param phonedata:
        :return:
        """
        if len(phonedata) < 1:
            return None

        data = phonedata

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=sum([d.sample for d in data]) / len(data))]

        return new_data

    def average_call_duration_hourly(self, phonedata):
        """
        Average time (in minutes) spent in call for each hour window. If there is not enough data
        for any window then there will no data point for that window.
        :param phonedata:
        :return:
        """
        if len(phonedata) < 1:
            return None

        data = copy.deepcopy(phonedata)
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

    def average_call_duration_four_hourly(self, phonedata):
        """
        Average time (in minutes) spent in call for each four hour window. If there is not enough data
        for any window then there will no data point for that window.
        :param phonedata:
        :return:
        """
        if len(phonedata) < 1:
            return None

        data = copy.deepcopy(phonedata)
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
        """
        Average sms length for a day. If there is not enough data for that day
        then it will return None.
        :param smsdata:
        :return:
        """
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
        """
        Average sms length for each hour window. If there is not enough data
        for any window then there will no data point for that window.
        :param smsdata:
        :return:
        """
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
        """
        Average sms length for each four hour window. If there is not enough data
        for any window then there will no data point for that window.
        :param smsdata:
        :return:
        """
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
        """
        Variance of sms length for a day. If there is not enough data
        for that day, then it will return None.
        :param smsdata:
        :return:
        """
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
        """
        Variance of sms length for each hour window. If there is not enough data
        for any window then there will no data point for that window.
        :param smsdata:
        :return:
        """
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
        """
        Variance of sms length for each four hour window. If there is not enough data
        for any window then there will no data point for that window.
        :param smsdata:
        :return:
        """
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

    def variance_call_duration_daily(self, phonedata):
        """
        Variance of call duration in minutes for a day. If there is not enough data
        for that day then it will return None.
        :param phonedata:
        :return:
        """
        if len(phonedata) < 1:
            return None

        data = phonedata

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=np.var([d.sample for d in data]) )]

        return new_data

    def variance_call_duration_hourly(self, phonedata):
        """
        Variance of call duration in minutes for each hour window. If there is not enough data
        for any window then there will no data point for that window.
        :param phonedata:
        :return:
        """
        if len(phonedata) < 1:
            return None

        data = copy.deepcopy(phonedata)
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

    def variance_call_duration_four_hourly(self, phonedata):
        """
        Variance of call duration in minutes for each four hour window. If there is not enough data
        for any window then there will no data point for that window.
        :param phonedata:
        :return:
        """
        if len(phonedata) < 1:
            return None

        data = copy.deepcopy(phonedata)
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

    def average_ambient_light_daily(self, lightdata, data_frequency=16, minimum_data_percent=40):
        """
        Average ambient light (in flux) for a day. If the input light data is less than minimum_data_percent%
        which is default 40%, it will return None.
        :param lightdata:
        :param data_frequency: How many data point should generate in a second
        :param minimum_data_percent: Minimum percent of data should be available
        :return:
        """
        if len(lightdata) < data_frequency * 24 * 60 * 60 * minimum_data_percent / 100:
            return None
        start_time = datetime.datetime.combine(lightdata[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=lightdata[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours = 23, minutes = 59)
        return [DataPoint(start_time, end_time, lightdata[0].offset, np.mean([x.sample for x in lightdata]))]

    def average_ambient_light_hourly(self, lightdata, data_frequency=16, minimum_data_percent=40):
        """
        Average ambient light (in flux) for each hour window in a day. If the input light data is less than minimum_data_percent%
        which is default 40%, in a window then it will not generate any data point for that window.
        :param lightdata:
        :param data_frequency: How many data point should generate in a second
        :param minimum_data_percent: Minimum percent of data should be available
        :return:
        """
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

    def average_ambient_light_four_hourly(self, lightdata, data_frequency=16, minimum_data_percent=40):
        """
        Average ambient light (in flux) for each four hour window in a day. If the input light data is less than
        minimum_data_percent%, which is default 40%, in a window then it will not generate any data point for that
         window.
        :param lightdata:
        :param data_frequency: How many data point should generate in a second
        :param minimum_data_percent: Minimum percent of data should be available
        :return:
        """
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

    def variance_ambient_light_daily(self, lightdata, data_frequency=16, minimum_data_percent=40):
        """
        Variance of ambient light (in flux) for a day. If the input light data is less than minimum_data_percent%
        which is default 40%, it will return None.
        :param lightdata:
        :param data_frequency: How many data point should generate in a second
        :param minimum_data_percent: Minimum percent of data should be available
        :return:
        """
        if len(lightdata) < data_frequency * 24 * 60 * 60 * minimum_data_percent / 100:
            return None
        start_time = datetime.datetime.combine(lightdata[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=lightdata[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours = 23, minutes = 59)
        return [DataPoint(start_time, end_time, lightdata[0].offset, np.var([x.sample for x in lightdata]))]

    def variance_ambient_light_hourly(self, lightdata, data_frequency=16, minimum_data_percent=40):
        """
        Variance of ambient light (in flux) for each hour window in a day. If the input light data is less than
         minimum_data_percent%, which is default 40%, in a window then it will not generate any data point for that window.
        :param lightdata:
        :param data_frequency: How many data point should generate in a second
        :param minimum_data_percent: Minimum percent of data should be available
        :return:
        """
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
        """
        Variance of ambient light (in flux) for each four hour window in a day. If the input light data is
        less than minimum_data_percent%, which is default 40%, in a window then it will not generate any data
         point for that window.
        :param lightdata:
        :param data_frequency: How many data point should generate in a second
        :param minimum_data_percent: Minimum percent of data should be available
        :return:
        """
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

    def calculate_phone_outside_duration(self, data, phone_inside_threshold_second=60):
        """
        Finds the duration (start_time and end_time) of phone outside (not in pocket or parse).
        It uses a threshold (phone_inside_threshold_second), such that, if there is a duration of
        at least this amount of consecutive time the phone proximity is 0, then this will be a
        period of phone inside.
        :param data:
        :param phone_inside_threshold_second:
        :return:
        """
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

    # lru_cache is used to cache the result of this function
    @lru_cache(maxsize=256)
    def get_app_category(self, appid):
        """
        Fetch and parse the google play store page of the android app
        and return the category. If there are multiple category it will
        return the first one in the webpage. Only for the GAME category
        it will return the sub-category also.
        :param appid: package name of an app
        :return: [package_name, category, app_name, sub_category]
        """
        appid = appid.strip()
        time.sleep(2.0)
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
            return [appid, "Game", str(name.contents[0]) if name else None, str(text.contents[0])]
        elif text:
            return [appid, str(text.contents[0]), str(name.contents[0]) if name else None, None]
        else:
            return [appid, None, str(name.contents[0]) if name else None, None]

    def get_appusage_duration_by_category(self, appdata, categories: list, appusage_gap_threshold_seconds=120):
        """
        Given the app category, it will return the list of duration when the app was used.
        It is assumed that if the gap between two consecutive data points with same app usage
        is within the appusage_gap_threshold_seconds time then, the app usage is in same session.
        :param appdata:
        :param categories:
        :param appusage_gap_threshold_seconds:
        :return:
        """
        appdata = sorted(appdata, key=lambda x: x.start_time)
        appusage = []

        i = 0
        threshold = timedelta(seconds=appusage_gap_threshold_seconds)
        while i< len(appdata):
            d = appdata[i]
            category = d.sample[1]
            if category not in categories:
                i += 1
                continue
            j = i+1
            while j<len(appdata) and d.sample == appdata[j].sample \
                    and appdata[j-1].start_time + threshold <= appdata[j].start_time:
                j += 1

            if j > i+1:
                appusage.append([d.start_time, appdata[j-1].start_time, category])
                i = j-1
            i += 1

        return appusage

    def appusage_interval_list(self, data, appusage):
        """
        Helper function to get screen touch gap between appusage
        :param data:
        :param appusage:
        :return:
        """
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
        """
        Helper function to label screen touch in a fixed app category usage
        :param data:
        :param appusage:
        :param intervals:
        :param interval_label:
        :return:
        """
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

    def process_appusage_day_data(self, user_id, appcategorydata, input_appcategorystream):
        #print(appcategorydata)
        data = {}
        try:
            categories =list(set([y.sample[1] for y in appcategorydata if y.sample[1]]))
            categories.sort(key=lambda x: x.start_time)
            for c in categories:
                d = self.get_appusage_duration_by_category(appcategorydata, [c],300)

                if d:
                    newd = [{ "start_time": x[0], "end_time": x[1]} for x in d]
                    data[c] = newd

            if data:
                st = appcategorydata[0].start_time.date()
                start_time = datetime.datetime.combine(st, datetime.time.min)
                start_time = start_time.replace(tzinfo=appcategorydata[0].start_time.tzinfo)
                end_time = datetime.datetime.combine(st, datetime.time.max)
                end_time = end_time.replace(tzinfo=appcategorydata[0].start_time.tzinfo)
                dp = DataPoint(start_time, end_time, appcategorydata[0].offset, data)

                self.store_stream(filepath="appusage_duration_by_category.json",
                                  input_streams=[input_appcategorystream], user_id=user_id,
                                  data=[dp], localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

    def get_contact_entropy(self, data: list) -> float:
        contact = {}
        for d in data:
            if d in contact:
                contact[d] += 1
            else:
                contact[d] = 1
        entropy = 0
        for f in contact.values():
            entropy -= f * math.log(f)
        return entropy

    def get_call_daily_entropy(self, data: list) -> list:
        if not data:
            return None
        entropy = self.get_contact_entropy([d.sample for d in data])

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=entropy)]
        return new_data

    def get_call_hourly_entropy(self, data: list) -> list:
        if not data:
            return None
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

            if len(datalist) == 0:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=self.get_contact_entropy(datalist) ))
        return new_data

    def get_call_four_hourly_entropy(self, data: list) -> list:
        if not data:
            return None
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) == 0:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=self.get_contact_entropy(datalist) ))
        return new_data

    def get_sms_daily_entropy(self, data: list) -> list:
        if not data:
            return None
        entropy = self.get_contact_entropy([d.sample for d in data])

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=entropy)]
        return new_data

    def get_sms_hourly_entropy(self, data: list) -> list:
        if not data:
            return None
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

            if len(datalist) == 0:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=self.get_contact_entropy(datalist) ))
        return new_data

    def get_sms_four_hourly_entropy(self, data: list) -> list:
        if not data:
            return None
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) == 0:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=self.get_contact_entropy(datalist) ))
        return new_data

    def get_call_sms_daily_entropy(self, calldata: list, smsdata: list) -> list:
        data = calldata + smsdata
        if not data:
            return None
        data.sort(key=lambda x: x.start_time)
        entropy = self.get_contact_entropy([d.sample for d in data])

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=entropy)]
        return new_data

    def get_call_sms_hourly_entropy(self, calldata: list, smsdata: list) -> list:
        data = calldata + smsdata
        if not data:
            return None
        data.sort(key=lambda x: x.start_time)
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

            if len(datalist) == 0:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=self.get_contact_entropy(datalist) ))
        return new_data

    def get_call_sms_four_hourly_entropy(self, calldata: list, smsdata: list) -> list:
        data = calldata + smsdata
        if not data:
            return None
        data.sort(key=lambda x: x.start_time)
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) == 0:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=self.get_contact_entropy(datalist) ))
        return new_data

    def get_total_call_daily(self, data: list) -> list:

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset, sample=len(data))]
        return new_data

    def get_total_call_hourly(self, data: list) -> list:
        if not data:
            return None
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

            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=len(datalist)))
        return new_data

    def get_total_call_four_hourly(self, data: list) -> list:
        if not data:
            return None
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=len(datalist)))
        return new_data

    def get_total_sms_daily(self, data: list) -> list:

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=len(data))]
        return new_data

    def get_total_sms_hourly(self, data: list) -> list:
        if not data:
            return None
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

            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=len(datalist)))
        return new_data

    def get_total_sms_four_hourly(self, data: list) -> list:
        if not data:
            return None
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=len(datalist)))
        return new_data

    def get_total_call_sms_daily(self, calldata: list, smsdata: list) -> list:
        data = calldata + smsdata

        data.sort(key=lambda x: x.start_time)

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=len(data))]
        return new_data

    def get_total_call_sms_hourly(self, calldata: list, smsdata: list) -> list:
        data = calldata + smsdata

        data.sort(key=lambda x: x.start_time)
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

            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=len(datalist) ))
        return new_data

    def get_total_call_sms_four_hourly(self, calldata: list, smsdata: list) -> list:
        data = calldata + smsdata
        if not data:
            return None
        data.sort(key=lambda x: x.start_time)
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour = h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=len(datalist)))
        return new_data

    def process_callsmsnumber_day_data(self, user_id, call_number_data, sms_number_data, input_callstream, input_smsstream):
        try:
            data = self.get_call_sms_daily_entropy(call_number_data, sms_number_data)
            if data:
                self.store_stream(filepath="call_sms_daily_entropy.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_call_sms_hourly_entropy(call_number_data, sms_number_data)
            if data:
                self.store_stream(filepath="call_sms_hourly_entropy.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_call_sms_four_hourly_entropy(call_number_data, sms_number_data)
            if data:
                self.store_stream(filepath="call_sms_four_hourly_entropy.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_call_daily_entropy(call_number_data)
            if data:
                self.store_stream(filepath="call_daily_entropy.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_call_hourly_entropy(call_number_data)
            if data:

                self.store_stream(filepath="call_hourly_entropy.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_call_four_hourly_entropy(call_number_data)
            if data:
                self.store_stream(filepath="call_four_hourly_entropy.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_sms_daily_entropy(sms_number_data)
            if data:
                self.store_stream(filepath="sms_daily_entropy.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_sms_hourly_entropy(sms_number_data)
            if data:
                self.store_stream(filepath="sms_hourly_entropy.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_sms_four_hourly_entropy(sms_number_data)
            if data:
                self.store_stream(filepath="sms_four_hourly_entropy.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_call_sms_daily(call_number_data, sms_number_data)
            if data:
                self.store_stream(filepath="total_call_sms_daily.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_call_sms_hourly(call_number_data, sms_number_data)
            if data:
                print(data)
                self.store_stream(filepath="total_call_sms_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_call_sms_four_hourly(call_number_data, sms_number_data)
            if data:
                print(data)
                self.store_stream(filepath="total_call_sms_four_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_call_daily(call_number_data)
            if data:
                self.store_stream(filepath="total_call_daily.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_call_hourly(call_number_data)
            if data:
                self.store_stream(filepath="total_call_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_call_four_hourly(call_number_data)
            if data:
                self.store_stream(filepath="total_call_four_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_sms_daily(sms_number_data)
            if data:
                self.store_stream(filepath="total_sms_daily.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_sms_hourly(sms_number_data)
            if data:
                self.store_stream(filepath="total_sms_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_sms_four_hourly(sms_number_data)
            if data:
                print(data)
                self.store_stream(filepath="total_sms_four_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

    def process_callsmsstream_day_data(self, user_id, callstream, smsstream, input_callstream, input_smsstream):
        """
        Process all the call and sms related features and store them as datastreams.
        :param user_id:
        :param callstream:
        :param smsstream:
        :param input_callstream:
        :param input_smsstream:
        :return:
        """
        try:
            data = self.average_inter_phone_call_sms_time_hourly(callstream, smsstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_sms_time_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_inter_phone_call_sms_time_four_hourly(callstream, smsstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_sms_time_four_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_inter_phone_call_sms_time_daily(callstream, smsstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_sms_time_daily.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_inter_phone_call_sms_time_daily(callstream, smsstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_sms_time_daily.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_inter_phone_call_sms_time_hourly(callstream, smsstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_sms_time_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_inter_phone_call_sms_time_four_hourly(callstream, smsstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_sms_time_four_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_inter_phone_call_time_hourly(callstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_time_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_inter_phone_call_time_four_hourly(callstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_time_four_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_inter_phone_call_time_daily(callstream)
            if data:
                self.store_stream(filepath="average_inter_phone_call_time_daily.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_inter_phone_call_time_hourly(callstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_time_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_inter_phone_call_time_four_hourly(callstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_time_four_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_inter_phone_call_time_daily(callstream)
            if data:
                self.store_stream(filepath="variance_inter_phone_call_time_daily.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_inter_sms_time_hourly(smsstream)
            if data:
                self.store_stream(filepath="average_inter_sms_time_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_inter_sms_time_four_hourly(smsstream)
            if data:
                self.store_stream(filepath="average_inter_sms_time_four_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_inter_sms_time_daily(smsstream)
            if data:
                self.store_stream(filepath="average_inter_sms_time_daily.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_inter_sms_time_hourly(smsstream)
            if data:
                self.store_stream(filepath="variance_inter_sms_time_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_inter_sms_time_four_hourly(smsstream)
            if data:
                self.store_stream(filepath="variance_inter_sms_time_four_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_inter_sms_time_daily(smsstream)
            if data:
                self.store_stream(filepath="variance_inter_sms_time_daily.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_call_duration_daily(callstream)
            if data:
                self.store_stream(filepath="average_call_duration_daily.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_call_duration_hourly(callstream)
            if data:
                self.store_stream(filepath="average_call_duration_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_call_duration_four_hourly(callstream)
            if data:
                self.store_stream(filepath="average_call_duration_four_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_sms_length_daily(smsstream)
            if data:
                self.store_stream(filepath="average_sms_length_daily.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_sms_length_hourly(smsstream)
            if data:
                self.store_stream(filepath="average_sms_length_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_sms_length_four_hourly(smsstream)
            if data:
                self.store_stream(filepath="average_sms_length_four_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_sms_length_daily(smsstream)
            if data:
                self.store_stream(filepath="variance_sms_length_daily.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_sms_length_hourly(smsstream)
            if data:
                self.store_stream(filepath="variance_sms_length_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_sms_length_four_hourly(smsstream)
            if data:
                self.store_stream(filepath="variance_sms_length_four_hourly.json",
                                  input_streams=[input_smsstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_call_duration_daily(callstream)
            if data:
                self.store_stream(filepath="variance_call_duration_daily.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_call_duration_hourly(callstream)
            if data:
                self.store_stream(filepath="variance_call_duration_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_call_duration_four_hourly(callstream)
            if data:
                self.store_stream(filepath="variance_call_duration_four_hourly.json",
                                  input_streams=[input_callstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

    def process_light_day_data(self, user_id, lightdata, input_lightstream):
        """
        Process all the ambient light related features and store the output
        streams.
        :param user_id:
        :param lightdata:
        :param input_lightstream:
        :return:
        """
        try:
            data = self.average_ambient_light_daily(lightdata)
            if data:
                self.store_stream(filepath="average_ambient_light_daily.json",
                                  input_streams=[input_lightstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_ambient_light_hourly(lightdata)
            if data:
                self.store_stream(filepath="average_ambient_light_hourly.json",
                                  input_streams=[input_lightstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.average_ambient_light_four_hourly(lightdata)
            if data:
                self.store_stream(filepath="average_ambient_light_four_hourly.json",
                                  input_streams=[input_lightstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_ambient_light_daily(lightdata)
            if data:
                self.store_stream(filepath="variance_ambient_light_daily.json",
                                  input_streams=[input_lightstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_ambient_light_hourly(lightdata)
            if data:
                self.store_stream(filepath="variance_ambient_light_hourly.json",
                                  input_streams=[input_lightstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.variance_ambient_light_four_hourly(lightdata)
            if data:
                self.store_stream(filepath="variance_ambient_light_four_hourly.json",
                                  input_streams=[input_lightstream], user_id=user_id,
                                  data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

    def process_proximity_day_data(self, user_id, proximitystream, input_proximitystream):
        try:
            data = self.calculate_phone_outside_duration(proximitystream)
            if data:
                self.store_stream(filepath="phone_outside_duration.json",
                                  input_streams=[input_proximitystream],
                                  user_id=user_id, data=data)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

    def get_overlapped_value(self, px1, py1, px2, py2):
        x = max(px1, px2)
        y = min(py1, py2)
        if x > y:
            return 0
        return (y - x).total_seconds() / 60

    def process_appusage_context_day_data(self, user_id: str, app_usage_data: list, input_usage_stream: DataStream,
                                          gps_semantic_data: list, input_gps_semantic_stream: DataStream):
        if not app_usage_data:
            return

        total = [{},{}, {}, {}]
        try:
            for category, data in app_usage_data[0].sample.items():
                total[0][category] = 0
                total[1][category] = 0
                total[2][category] = 0
                total[3][category] = 0
                for d in data:
                    total[0][category] += (d["end_time"] - d["start_time"]).total_seconds() / 60
                    for gd in gps_semantic_data:
                        val = self.get_overlapped_value(d["start_time"], d["end_time"], gd.start_time, gd.end_time)
                        if gd.sample == "work":
                            total[1][category] += val
                        elif gd.sample == "home":
                            total[2][category] += val

                total[3][category] = total[0][category] - total[1][category] - total[2][category]

            context_total = [24, 0, 0, 0]
            for gd in gps_semantic_data:
                if gd.sample == "work":
                    context_total[1] += (gd.end_time - gd.start_time).total_seconds() / (60*60)
                elif gd.sample == "home":
                    context_total[2] += (gd.end_time - gd.start_time).total_seconds() / (60*60)
            context_total[3] = context_total[0] - context_total[1] - context_total[2]

            st = app_usage_data[0].start_time.date()
            start_time = datetime.datetime.combine(st, datetime.time.min)
            start_time = start_time.replace(tzinfo=app_usage_data[0].start_time.tzinfo)
            end_time = datetime.datetime.combine(st, datetime.time.max)
            end_time = end_time.replace(tzinfo=app_usage_data[0].start_time.tzinfo)

            dp1 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[0])
            dp2 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[1])
            dp3 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[2])
            dp4 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[3])

            self.store_stream(filepath="appusage_duration_total_by_category.json",
                              input_streams=[input_usage_stream], user_id=user_id,
                              data=[dp1], localtime=False)
            self.store_stream(filepath="appusage_duration_total_by_category_work.json",
                              input_streams=[input_usage_stream, input_gps_semantic_stream], user_id=user_id,
                              data=[dp2], localtime=False)
            self.store_stream(filepath="appusage_duration_total_by_category_home.json",
                              input_streams=[input_usage_stream, input_gps_semantic_stream], user_id=user_id,
                              data=[dp3], localtime=False)
            self.store_stream(filepath="appusage_duration_total_by_category_outside.json",
                              input_streams=[input_usage_stream, input_gps_semantic_stream], user_id=user_id,
                              data=[dp4], localtime=False)

            for i in range(4):
                if context_total[i] == 0:
                    continue
                for category in app_usage_data[0].sample:
                    total[i][category] /= context_total[i]

            dp5 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[0])
            dp6 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[1])
            dp7 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[2])
            dp8 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[3])

            self.store_stream(filepath="appusage_duration_average_by_category.json",
                              input_streams=[input_usage_stream], user_id=user_id,
                              data=[dp5], localtime=False)
            self.store_stream(filepath="appusage_duration_average_by_category_work.json",
                              input_streams=[input_usage_stream, input_gps_semantic_stream], user_id=user_id,
                              data=[dp6], localtime=False)
            self.store_stream(filepath="appusage_duration_average_by_category_home.json",
                              input_streams=[input_usage_stream, input_gps_semantic_stream], user_id=user_id,
                              data=[dp7], localtime=False)
            self.store_stream(filepath="appusage_duration_average_by_category_outside.json",
                              input_streams=[input_usage_stream, input_gps_semantic_stream], user_id=user_id,
                              data=[dp8], localtime=False)

        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

    def process_appcategory_day_data(self, user_id, appcategorystream, input_appcategorystream):
        """
        process all app category related features.
        :param user_id:
        :param appcategorystream:
        :param input_appcategorystream:
        :return:
        """
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
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

    def process_data(self, user_id, all_user_streams, all_days):
        """
        Getting all the necessary input datastreams for a user
        and run all feature processing modules for all the days
        of the user.
        :param user_id:
        :param all_user_streams:
        :param all_days:
        :return:
        """
        input_callstream = None
        input_smsstream = None
        input_proximitystream = None
        input_cuappusagestream = None
        input_appcategorystream = None
        input_lightstream = None
        input_appusage_stream = None
        input_gpssemanticstream = None
        input_callnumberstream = None
        input_smsnumberstream = None

        call_stream_name = 'CU_CALL_DURATION--edu.dartmouth.eureka'
        sms_stream_name = 'CU_SMS_LENGTH--edu.dartmouth.eureka'
        proximity_stream_name = 'PROXIMITY--org.md2k.phonesensor--PHONE'
        cu_appusage_stream_name = 'CU_APPUSAGE--edu.dartmouth.eureka'
        touchescreen_stream_name = "TOUCH_SCREEN--org.md2k.phonesensor--PHONE"
        light_stream_name = 'AMBIENT_LIGHT--org.md2k.phonesensor--PHONE'
        appcategory_stream_name = "org.md2k.data_analysis.feature.phone.app_usage_category"
        appusage_stream_name = "org.md2k.data_analysis.feature.phone.app_usage_interval"
        gpssemantic_stream_name = "org.md2k.data_analysis.feature.gps_semantic_location.daywise_split.utc"
        call_number_stream_name = "CU_CALL_NUMBER--edu.dartmouth.eureka"
        sms_number_stream_name = "CU_SMS_NUMBER--edu.dartmouth.eureka"

        streams = all_user_streams
        days = None

        if not streams or not len(streams):
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
            elif stream_name == cu_appusage_stream_name:
                input_cuappusagestream = stream_metadata
            elif stream_name == light_stream_name:
                input_lightstream = stream_metadata
            elif stream_name == call_number_stream_name:
                input_callnumberstream = stream_metadata
            elif stream_name == sms_number_stream_name:
                input_smsnumberstream = stream_metadata

        # Processing Call and SMS related features
        if not input_callstream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, call_stream_name,
                                 str(user_id)))

        elif not input_smsstream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, sms_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                callstream = self.get_data_by_stream_name(call_stream_name, user_id, day, localtime=False)
                callstream = self.get_filtered_data(callstream, lambda x: (type(x) is float and x>=0))
                smsstream = self.get_data_by_stream_name(sms_stream_name, user_id, day, localtime=False)
                smsstream = self.get_filtered_data(smsstream, lambda x: (type(x) is float and x>=0))
                self.process_callsmsstream_day_data(user_id, callstream, smsstream, input_callstream, input_smsstream)

        # processing proximity sensor related features
        if not input_proximitystream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, proximity_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                proximitystream = self.get_data_by_stream_name(proximity_stream_name, user_id, day)
                proximitystream = self.get_filtered_data(proximitystream, lambda x: (type(x) is float and x>=0))
                self.process_proximity_day_data(user_id, proximitystream, input_proximitystream)

        # Processing ambient light related features
        if not input_lightstream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, light_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                lightstream = self.get_data_by_stream_name(light_stream_name, user_id, day, localtime=False)
                lightstream = self.get_filtered_data(lightstream, lambda x: (type(x) is float and x>=0))
                self.process_light_day_data(user_id, lightstream, input_lightstream)
        # processing app usage and category related features
        if not input_cuappusagestream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, cu_appusage_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                appusagestream = self.get_data_by_stream_name(cu_appusage_stream_name, user_id, day)
                appusagestream = self.get_filtered_data(appusagestream, lambda x: type(x) is str)
                self.process_appcategory_day_data(user_id, appusagestream, input_cuappusagestream)

        # Processing phone touche and typing related features
        streams = self.CC.get_user_streams(user_id)
        if not streams or not len(streams):
            self.CC.logging.log('No streams found for user %s for feature %s'
                                % (str(user_id), self.__class__.__name__))
            return

        for stream_name, stream_metadata in streams.items():
            if stream_name == appcategory_stream_name:
                input_appcategorystream = stream_metadata

        if not input_appcategorystream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, appcategory_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                appcategorydata = self.get_data_by_stream_name(appcategory_stream_name, user_id, day, localtime=False)
                appcategorydata = self.get_filtered_data(appcategorydata, lambda x: (type(x) is list and len(x)==4))
                self.process_appusage_day_data(user_id, appcategorydata, input_appcategorystream)

        streams = self.CC.get_user_streams(user_id)
        if not streams or not len(streams):
            self.CC.logging.log('No streams found for user %s for feature %s'
                                % (str(user_id), self.__class__.__name__))
            return

        for stream_name, stream_metadata in streams.items():
            if stream_name == appusage_stream_name:
                input_appusage_stream = stream_metadata
            elif stream_name == gpssemantic_stream_name:
                input_gpssemanticstream = stream_metadata

        if not input_appusage_stream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, appusage_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                app_usage_data = self.get_data_by_stream_name(appusage_stream_name, user_id, day, localtime=False)
                app_usage_data = self.get_filtered_data(app_usage_data, lambda x: type(x) is dict)

                gps_semantic_data = self.get_data_by_stream_name(gpssemantic_stream_name, user_id, day, localtime=False)
                gps_semantic_data = self.get_filtered_data(gps_semantic_data,
                                                           lambda x: ((type(x) is str) or (type(x) is np.str_) ))

                self.process_appusage_context_day_data(user_id, app_usage_data, input_appusage_stream,
                                                       gps_semantic_data, input_gpssemanticstream)

        if not input_callnumberstream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, call_number_stream_name,
                                 str(user_id)))
        elif not input_smsnumberstream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, sms_number_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                callnumberdata = self.get_data_by_stream_name(call_number_stream_name, user_id, day, localtime=False)
                callnumberdata = self.get_filtered_data(callnumberdata, lambda x: (type(x) is str))
                smsnumberdata = self.get_data_by_stream_name(sms_number_stream_name, user_id, day, localtime=False)
                smsnumberdata = self.get_filtered_data(smsnumberdata, lambda x: (type(x) is str))
                self.process_callsmsnumber_day_data(user_id, callnumberdata, smsnumberdata, input_callnumberstream,
                                                    input_smsnumberstream)


    def process(self, user_id, all_days):
        if self.CC is not None:
            self.CC.logging.log("Processing PhoneFeatures")
            streams = self.CC.get_user_streams(user_id)
            self.process_data(user_id, streams, all_days)

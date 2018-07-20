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
import base64
import pickle

from sklearn.mixture import GaussianMixture
from typing import List, Callable, Any

feature_class_name = 'PhoneFeatures'

# Constants
IN_VEHICLE = 6.0
ON_BICYCLE = 5.0
STILL = 0.0
ON_FOOT = 1.0
TILTING = 2.0
WALKING = 3.0
RUNNING = 4.0
UNKNOWN = 7.0

OUTGOING_TYPE = 2.0
MESSAGE_TYPE_SENT = 2.0


class PhoneFeatures(ComputeFeatureBase):
    """
    This class is responsible for computing features based on streams of data
    derived from the smartphone sensors.
    """

    def get_filtered_data(self, data: List[DataPoint],
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

        return filtered_data

    def get_data_by_stream_name(self, stream_name: str, user_id: str, day: str,
                                localtime: bool=True, ingested_stream=True) -> List[DataPoint]:
        """
        Combines data from multiple streams data of same stream based on stream name.

        :param str stream_name: Name of the stream
        :param str user_id: UUID of the stream owner
        :param str day: The day (YYYYMMDD) on which to operate
        :param bool localtime: The way to structure time, True for operating in participant's local time, False for UTC
        :return: Combined stream data if there are multiple stream id
        :rtype: List(DataPoint)
        """

        if ingested_stream:
            stream_ids = self.CC.get_stream_id(user_id, stream_name)
        else:
            stream_ids = self.get_latest_stream_id(user_id, stream_name)

        data = []
        for stream in stream_ids:
            if stream is not None:
                ds = self.CC.get_stream(stream['identifier'], user_id=user_id, day=day, localtime=localtime)
                if ds is not None:
                    if ds.data is not None:
                        data += ds.data
        if len(stream_ids) > 1:
            data = sorted(data, key=lambda x: x.start_time)
        return data

    def inter_event_time_list(self, data: List[DataPoint]) -> List[float]:
        """
        Helper function to compute inter-event times

        :param List(DataPoint) data: A list of DataPoints
        :return: Time deltas between DataPoints in minutes
        :rtype: list(float)
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

    def average_inter_phone_call_sms_time_hourly(self, phonedata: List[DataPoint], smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Average time (in minutes) between two consecutive events (call and sms)
        for each hour window. If there is not enough data for a window then
        there will be no data point for that window.

        :param List(DataPoint) phonedata: Phone call DataStream
        :param List(DataPoint) smsdata: SMS DataStream
        :return: Average inter-phone call and sms time over 1 hour windows
        :rtype: List(DataPoint) or None
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

    def average_inter_phone_call_sms_time_four_hourly(self, phonedata: List[DataPoint], smsdata: List[DataPoint]) \
            -> List[DataPoint]:
        """
        Average time (in minutes) between two consecutive events (call and sms)
        for each four hour window. If there is not enough data for a window then
        there will be no data point for that window.

        :param List(DataPoint) phonedata: Phone call DataStream
        :param List(DataPoint) smsdata: SMS DataStream
        :return: Average inter-phone call and sms time over 4 hour windows
        :rtype: List(DataPoint) or None
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

    def average_inter_phone_call_sms_time_daily(self, phonedata: List[DataPoint], smsdata: List[DataPoint])\
            -> List[DataPoint]:
        """
        Average time (in minutes) between two consecutive events (call and sms)
        for whole day. If there is not enough data then it will return None.

        :param List(DataPoint) phonedata: Phone call DataStream
        :param List(DataPoint) smsdata: SMS DataStream
        :return: Average inter-phone call and sms time over 1 day windows
        :rtype: List(DataPoint) or None
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

    def variance_inter_phone_call_sms_time_daily(self, phonedata: List[DataPoint], smsdata: List[DataPoint]) \
            -> List[DataPoint]:
        """
        Variance of time (in minutes) between two consecutive events (call and sms)
        for whole day. If there is not enough data then it will return None.

        :param List(DataPoint) phonedata: Phone call DataStream
        :param List(DataPoint) smsdata: SMS DataStream
        :return: Variance of inter-phone call and sms time over 1 day windows
        :rtype: List(DataPoint) or None
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

    def variance_inter_phone_call_sms_time_hourly(self, phonedata: List[DataPoint], smsdata: List[DataPoint])\
            ->List[DataPoint]:
        """
        Variance of time (in minutes) between two consecutive events (call and sms)
        for each hour window. If there is not enough data for a window then
        there will be no data point for that window.

        :param List(DataPoint) phonedata: Phone call DataStream
        :param List(DataPoint) smsdata: SMS DataStream
        :return: Variances of inter-phone call and sms time over 1 hour windows
        :rtype: List(DataPoint) or None
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

    def variance_inter_phone_call_sms_time_four_hourly(self, phonedata: List[DataPoint], smsdata: List[DataPoint])\
            ->List[DataPoint]:
        """
        Variance of time (in minutes) between two consecutive events (call and sms)
        for each four hour window. If there is not enough data for a window then
        there will be no data point for that window.

        :param List(DataPoint) phonedata: Phone call DataStream
        :param List(DataPoint) smsdata: SMS DataStream
        :return: Variances of inter-phone call and sms time over 4 hour windows
        :rtype: List(DataPoint) or None
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

    def average_inter_phone_call_time_hourly(self, phonedata: List[DataPoint])->List[DataPoint]:
        """
        Average time (in minutes) between two consecutive call for each hour window.
        If there is not enough data for a window then there will be no data point for that window.

        :param List(DataPoint) phonedata: Phone call DataStream
        :return: Average inter-phone call time over 1 hour windows
        :rtype: List(DataPoint) or None
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

    def average_inter_phone_call_time_four_hourly(self, phonedata: List[DataPoint])->List[DataPoint]:
        """
        Average time (in minutes) between two consecutive call for each four hour window.
        If there is not enough data for a window then there will be no data point for that window.

        :param List(DataPoint) phonedata: Phone call DataStream
        :return: Average inter-phone call time over 4 hour windows
        :rtype: List(DataPoint) or None
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

    def average_inter_phone_call_time_daily(self, phonedata: List[DataPoint])->List[DataPoint]:
        """
        Average time (in minutes) between two consecutive call for a whole day.
        If there is not enough data for the day then it will return None.

        :param List(DataPoint) phonedata: Phone call DataStream
        :return: Average inter-phone call time over 1 day window
        :rtype: List(DataPoint) or None
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

    def variance_inter_phone_call_time_hourly(self, phonedata: List[DataPoint])->List[DataPoint]:
        """
        Variance of time (in minutes) between two consecutive call for each hour window.
        If there is not enough data for a window then there will be no data point for that window.

        :param List(DataPoint) phonedata: Phone call DataStream
        :return: Average inter-phone call time over 1 hour windows
        :rtype: List(DataPoint) or None
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
                                      sample=np.var(self.inter_event_time_list(datalist))))

        return new_data

    def variance_inter_phone_call_time_four_hourly(self, phonedata: List[DataPoint])->List[DataPoint]:
        """
        Variance of time (in minutes) between two consecutive call for each four hour window.
        If there is not enough data for a window then there will be no data point for that window.

        :param List(DataPoint) phonedata: Phone call DataStream
        :return: Variance of inter-phone call time over 4 hour windows
        :rtype: List(DataPoint) or None
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
                                      sample=np.var(self.inter_event_time_list(datalist))))

        return new_data

    def variance_inter_phone_call_time_daily(self, phonedata: List[DataPoint]) -> List[DataPoint]:
        """
        Average time (in minutes) between two consecutive call for a day.
        If there is not enough data for the day then it will return None.

        :param List(DataPoint) phonedata: Phone call DataStream
        :return: Variance of inter-phone call time over 1 day windows
        :rtype: List(DataPoint) or None
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
                              sample=np.var(self.inter_event_time_list(combined_data)))]

        return new_data

    def average_inter_sms_time_hourly(self, smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Average time (in minutes) between two consecutive sms for each hour window.
        If there is not enough data for a window then there will be no data point for that window.

        :param List(DataPoint) smsdata: SMS DataStream
        :return: Average inter-sms time over 1 hour windows
        :rtype: List(DataPoint) or None
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

    def average_inter_sms_time_four_hourly(self, smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Average time (in minutes) between two consecutive sms for each four hour window.
        If there is not enough data for a window then there will be no data point for that window.

        :param List(DataPoint) smsdata: SMS DataStream
        :return: Average inter-sms time over 4 hour windows
        :rtype: List(DataPoint) or None
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

    def average_inter_sms_time_daily(self, smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Average time (in minutes) between two consecutive sms for a day.
        If there is not enough data for the day then it will return None.

        :param List(DataPoint) smsdata: SMS DataStream
        :return: Average inter-sms time over 1 day windows
        :rtype: List(DataPoint) or None
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

    def variance_inter_sms_time_hourly(self, smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Variance of time (in minutes) between two consecutive sms for each hour window.
        If there is not enough data for a window then there will be no data point for that window.

        :param List(DataPoint) smsdata: SMS DataStream
        :return: Variance of inter-sms time over 1 hour windows
        :rtype: List(DataPoint) or None
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

    def variance_inter_sms_time_four_hourly(self, smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Average time (in minutes) between two consecutive sms for each four hour window.
        If there is not enough data for a window then there will be no data point for that window.

        :param List(DataPoint) smsdata: SMS DataStream
        :return: Variance of inter-sms time over 4 hour windows
        :rtype: List(DataPoint) or None
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

    def variance_inter_sms_time_daily(self, smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Average time (in minutes) between two consecutive sms for a day.
        If there is not enough data for that day, then it will return None.

        :param List(DataPoint) smsdata: SMS DataStream
        :return: Variance of inter-sms time over 1 daily windows
        :rtype: List(DataPoint) or None
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
                              sample=np.var(self.inter_event_time_list(combined_data)))]

        return new_data

    def average_call_duration_daily(self, phonedata: List[DataPoint]) -> List[DataPoint]:
        """
        Average time (in minutes) spent in call in a day. If there is not enough data
        for that day then it will return None.

        :param List(DataPoint) phonedata: Phone call DataStream
        :return: Average call duration over 1 day windows
        :rtype: List(DataPoint) or None
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

    def average_call_duration_hourly(self, phonedata: List[DataPoint]) -> List[DataPoint]:
        """
        Average time (in minutes) spent in call for each hour window. If there is not enough data
        for any window then there will no data point for that window.

        :param List(DataPoint) phonedata: Phone call DataStream
        :return: Average phone call duration over 1 hour windows
        :rtype: List(DataPoint) or None
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
            start = tmp_time.replace(hour=h)
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

    def average_call_duration_four_hourly(self, phonedata: List[DataPoint]) -> List[DataPoint]:
        """
        Average time (in minutes) spent in call for each four hour window. If there is not enough data
        for any window then there will no data point for that window.

        :param List(DataPoint) phonedata: Phone call DataStream
        :return: Average phone call duration over 4 hour windows
        :rtype: List(DataPoint) or None
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
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
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

    def average_sms_length_daily(self, smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Average sms length for a day. If there is not enough data for that day
        then it will return None.

        :param List(DataPoint) phonedata: Phone call DataStream
        :return: Average sms length over 1 day windows
        :rtype: List(DataPoint) or None

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

    def average_sms_length_hourly(self, smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Average sms length for each hour window. If there is not enough data
        for any window then there will no data point for that window.

        :param List(DataPoint) smsdata: SMS DataStream
        :return: Average SMS length over 1 hour windows
        :rtype: List(DataPoint) or None
        """
        if len(smsdata) < 1:
            return None

        data = smsdata

        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=sum(datalist) / len(datalist)))

        return new_data

    def average_sms_length_four_hourly(self, smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Average sms length for each four hour window. If there is not enough data
        for any window then there will no data point for that window.

        :param List(DataPoint) smsdata: SMS DataStream
        :return: Average sms length over 4 hour windows
        :rtype: List(DataPoint) or None
        """
        if len(smsdata) < 1:
            return None

        data = smsdata

        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=sum(datalist) / len(datalist)))

        return new_data

    def variance_sms_length_daily(self, smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Variance of sms length for a day. If there is not enough data
        for that day, then it will return None.

        :param List(DataPoint) smsdata: SMS DataStream
        :return: Variance of SMS length over 1 day windows
        :rtype: List(DataPoint) or None
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

    def variance_sms_length_hourly(self, smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Variance of sms length for each hour window. If there is not enough data
        for any window then there will no data point for that window.

        :param List(DataPoint) smsdata: SMS DataStream
        :return: Variance of SMS length over 1 hour windows
        :rtype: List(DataPoint) or None
        """
        if len(smsdata) < 1:
            return None

        data = smsdata

        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=np.var(datalist)))

        return new_data

    def variance_sms_length_four_hourly(self, smsdata: List[DataPoint])-> List[DataPoint]:
        """
        Variance of sms length for each four hour window. If there is not enough data
        for any window then there will no data point for that window.

        :param List(DataPoint) smsdata: SMS DataStream
        :return: Variance of SMS length over 4 hour windows
        :rtype: List(DataPoint) or None
        """
        if len(smsdata) < 1:
            return None

        data = smsdata

        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < 1:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=np.var(datalist)))

        return new_data

    def variance_call_duration_daily(self, phonedata: List[DataPoint]) -> List[DataPoint]:
        """
        Variance of call duration in minutes for a day. If there is not enough data
        for that day then it will return None.

        :param List(DataPoint) phonedata: Phone call duration DataStream
        :return: Variance of phone call duration over 1 day windows
        :rtype: List(DataPoint) or None
        """
        if len(phonedata) < 1:
            return None

        data = phonedata

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=np.var([d.sample for d in data]))]

        return new_data

    def variance_call_duration_hourly(self, phonedata: List[DataPoint]) -> List[DataPoint]:
        """
        Variance of call duration in minutes for each hour window. If there is not enough data
        for any window then there will no data point for that window.

        :param List(DataPoint) phonedata: Phone call duration DataStream
        :return: Variance of phone call duration over 1 hour windows
        :rtype: List(DataPoint) or None
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
            start = tmp_time.replace(hour=h)
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
                                      sample=np.var(datalist)))

        return new_data

    def variance_call_duration_four_hourly(self, phonedata: List[DataPoint]) -> List[DataPoint]:
        """
        Variance of call duration in minutes for each four hour window. If there is not enough data
        for any window then there will no data point for that window.

        :param List(DataPoint) phonedata: Phone call duration DataStream
        :return: Variance of phone call duration over 4 hour windows
        :rtype: List(DataPoint) or None
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
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
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

    def average_ambient_light_daily(self, lightdata: List[DataPoint], data_frequency: float=16,
                                    minimum_data_percent: float=40) -> List[DataPoint]:
        """
        Average ambient light (in flux) for a day. If the input light data is less than minimum_data_percent%
        which is default 40%, it will return None.

        :param List(DataPoint) lightdata: Phone ambient light DataStream
        :param float data_frequency: How many data point should generate in a second
        :param float minimum_data_percent: Minimum percent of data should be available
        :return: Average of ambient light over 1 day windows
        :rtype: List(DataPoint) or None

        """
        if len(lightdata) < data_frequency * 24 * 60 * 60 * minimum_data_percent / 100:
            return None
        start_time = datetime.datetime.combine(lightdata[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=lightdata[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        return [DataPoint(start_time, end_time, lightdata[0].offset, np.mean([x.sample for x in lightdata]))]

    def average_ambient_light_hourly(self, lightdata: List[DataPoint], data_frequency: float=16,
                                     minimum_data_percent: float=40) -> List[DataPoint]:
        """
        Average ambient light (in flux) for each hour window in a day. If the input light data is less than minimum_data_percent%
        which is default 40%, in a window then it will not generate any data point for that window.

        :param List(DataPoint) lightdata: Phone ambient light DataStream
        :param float data_frequency: How many data point should generate in a second
        :param float minimum_data_percent: Minimum percent of data should be available
        :return: Average of ambient light over 1 hour windows
        :rtype: List(DataPoint) or None
        """
        if len(lightdata) < 1:
            return None

        data = lightdata

        new_data = []
        tmp_time = copy.deepcopy(data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
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
                                      sample=np.mean(datalist)))

        return new_data

    def average_ambient_light_four_hourly(self, lightdata: List[DataPoint], data_frequency: float=16,
                                          minimum_data_percent: float=40) -> List[DataPoint]:
        """
        Average ambient light (in flux) for each four hour window in a day. If the input light data is less than
        minimum_data_percent%, which is default 40%, in a window then it will not generate any data point for that
        window.

        :param List(DataPoint) lightdata: Phone ambient light DataStream
        :param float data_frequency: How many data point should generate in a second
        :param float minimum_data_percent: Minimum percent of data should be available
        :return: Average of ambient light over 4 hour windows
        :rtype: List(DataPoint) or None
        """
        if len(lightdata) < 1:
            return None

        data = lightdata

        new_data = []
        tmp_time = copy.deepcopy(data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < data_frequency * 4 * 60 * 60 * minimum_data_percent / 100:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=np.mean(datalist)))

        return new_data

    def variance_ambient_light_daily(self, lightdata: List[DataPoint], data_frequency: float=16,
                                     minimum_data_percent: float=40) -> List[DataPoint]:
        """
        Variance of ambient light (in flux) for a day. If the input light data is less than minimum_data_percent%
        which is default 40%, it will return None.

        :param List(DataPoint) lightdata: Phone ambient light DataStream
        :param float data_frequency: How many data point should generate in a second
        :param float minimum_data_percent: Minimum percent of data should be available
        :return: Variance of ambient light over 1 day windows
        :rtype: List(DataPoint) or None
        """
        if len(lightdata) < data_frequency * 24 * 60 * 60 * minimum_data_percent / 100:
            return None
        start_time = datetime.datetime.combine(lightdata[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=lightdata[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        return [DataPoint(start_time, end_time, lightdata[0].offset, np.var([x.sample for x in lightdata]))]

    def variance_ambient_light_hourly(self, lightdata: List[DataPoint], data_frequency: float=16,
                                      minimum_data_percent: float=40) -> List[DataPoint]:
        """
        Variance of ambient light (in flux) for each hour window in a day. If the input light data is less than
         minimum_data_percent%, which is default 40%, in a window then it will not generate any data point for that window.

        :param List(DataPoint) lightdata: Phone ambient light DataStream
        :param float data_frequency: How many data point should generate in a second
        :param float minimum_data_percent: Minimum percent of data should be available
        :return: Variance of ambient light over 1 hour windows
        :rtype: List(DataPoint) or None
        """
        if len(lightdata) < 1:
            return None

        data = lightdata

        new_data = []
        tmp_time = copy.deepcopy(data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
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
                                      sample=np.var(datalist)))

        return new_data

    def variance_ambient_light_four_hourly(self, lightdata: List[DataPoint], data_frequency: float=16,
                                           minimum_data_percent: float=40) -> List[DataPoint]:
        """
        Variance of ambient light (in flux) for each four hour window in a day. If the input light data is
        less than minimum_data_percent%, which is default 40%, in a window then it will not generate any data
         point for that window.

        :param List(DataPoint) lightdata: Phone ambient light DataStream
        :param float data_frequency: How many data point should generate in a second
        :param float minimum_data_percent: Minimum percent of data should be available
        :return: Variance of ambient light over 4 hour windows
        :rtype: List(DataPoint) or None
        """
        if len(lightdata) < 1:
            return None

        data = lightdata

        new_data = []
        tmp_time = copy.deepcopy(data[0].start_time)
        tmp_time = tmp_time.replace(hour=0, minute=0, second=0, microsecond=0)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) < data_frequency * 4 * 60 * 60 * minimum_data_percent / 100:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=np.var(datalist)))

        return new_data

    def calculate_phone_outside_duration(self, data: List[DataPoint],
                                         phone_inside_threshold_second: float=60) -> List[DataPoint]:
        """
        Finds the duration (start_time and end_time) of phone outside (not in pocket or parse).
        It uses a threshold (phone_inside_threshold_second), such that, if there is a duration of
        at least this amount of consecutive time the phone proximity is 0, then this will be a
        period of phone inside.

        :param List(DataPoint) data: Phone proximity Datastream
        :param float phone_inside_threshold_second: Threshold in seconds, that is allowed with
                proximity 0 with phone outside
        :return: DataPoints containing intervals of phone outside
        :rtype: List(DataPoint)
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
    def get_app_category(self, appid: str) -> List[str]:
        """
        Fetch and parse the google play store page of the android app
        and return the category. If there are multiple category it will
        return the first one in the webpage. Only for the GAME category
        it will return the sub-category also.

        :param str appid: package name of an app
        :return: [package_name, category (if exists, otherwise None) ,
                    app_name (if exists, otherwise None), sub_category (if exists, otherwise None)]
        :rtype: List(str)
        """
        appid = appid.strip()
        if appid == "com.samsung.android.messaging":
            return [appid, "Communication", "Samsung Message", None]

        url = "https://play.google.com/store/apps/details?id=" + appid
        cached_response = None
        #cached_response = self.CC.get_cache_value(appid)
        response = None
        
        if cached_response is None:
            try:
                time.sleep(2.0)
                self.CC.logging.log('%s not found in cache.' % (appid))
                response = urlopen(url)
            except Exception:
                toreturn = [appid, None, None, None]
                objstr = base64.b64encode(pickle.dumps(toreturn))
                #self.CC.set_cache_value(appid, objstr.decode())
                return toreturn
        else:
            return pickle.loads(base64.decodebytes(cached_response.encode()))

        soup = BeautifulSoup(response, 'html.parser')
        text = soup.find('span', itemprop='genre')

        name = soup.find('div', class_='id-app-title')

        cat = soup.find('a', class_='document-subtitle category')
        if cat:
            category = cat.attrs['href'].split('/')[-1]
        else:
            category = None

        toreturn = None
        if category and category.startswith('GAME_'):
            toreturn = [appid, "Game", str(name.contents[0]) if name else None, str(text.contents[0])]
        elif text:
            toreturn = [appid, str(text.contents[0]), str(name.contents[0]) if name else None, None]
        else:
            toreturn = [appid, None, str(name.contents[0]) if name else None, None]

        objstr = base64.b64encode(pickle.dumps(toreturn))
        self.CC.set_cache_value(appid, objstr.decode())
        return toreturn

    def get_appusage_duration_by_category(self, appdata: List[DataPoint], categories: List[str],
                                          appusage_gap_threshold_seconds: float=120) -> List:
        """
        Given the app category, it will return the list of duration when the app was used.
        It is assumed that if the gap between two consecutive data points with same app usage
        is within the appusage_gap_threshold_seconds time then, the app usage is in same session.

        :param List(DataPoint) appdata: App category data stream
        :param List(str) categories: List of app categories of which the usage duration should be calculated
        :param float appusage_gap_threshold_seconds: Threshold in seconds, which is the gap allowed between two
                        consecutive DataPoint of same app
        :return: A list of intervals of the given apps (categories) usage [start_time, end_time, category]
        :rtype: List
        """
        appdata = sorted(appdata, key=lambda x: x.start_time)
        appusage = []

        i = 0
        threshold = timedelta(seconds=appusage_gap_threshold_seconds)
        while i < len(appdata):
            d = appdata[i]
            category = d.sample[1]
            if category not in categories:
                i += 1
                continue
            j = i + 1
            while j < len(appdata) and d.sample == appdata[j].sample \
                    and appdata[j - 1].start_time + threshold <= appdata[j].start_time:
                j += 1

            if j > i + 1:
                appusage.append([d.start_time, appdata[j - 1].start_time, category])
                i = j - 1
            i += 1

        return appusage

    def appusage_interval_list(self, data: List[DataPoint], appusage: List) -> List[int]:
        """
        Helper function to get screen touch gap for specific app categories

        :param List(DataPoint) data: Phone screen touch data stream
        :param List appusage: list of app usage duration of specific app categories of the form
                                [start_time, end_time, category]
        :return: A list of integers containing screen touch gap as in touch screen timestamp unit (milliseconds)
        :rtype: List(int)
        """
        ret = []
        i = 0
        for a in appusage:
            while i < len(data) and data[i].start_time < a[0]:
                i += 1
            last = 0
            while i < len(data) and data[i].start_time <= a[1]:
                if last > 0:
                    ret.append(int(data[i].sample - last))
                last = data[i].sample
                i += 1
        return ret

    def label_appusage_intervals(self, data: List[DataPoint], appusage: List, intervals: List,
                                 interval_label: List[str]) -> List[DataPoint]:
        """
        Helper function to label screen touch in a fixed app category usage

        :param List(DataPoint) data: Phone touch screen data stream
        :param List appusage: List appusage: list of app usage duration of specific app categories of the form
                                [start_time, end_time, category]
        :param intervals: List of integers containing screen touch gap as in touch screen timestamp unit (milliseconds)
        :param interval_label: A list of possible type of screen touch which are [typing, pause, reading, unknown]
        :return: Labelled touche interval
        :rtype: List(DataPoint)
        """
        ret = []
        i = 0
        for a in appusage:
            while i < len(data) and data[i].start_time < a[0]:
                i += 1
            last = None
            while i < len(data) and data[i].start_time <= a[1]:
                if last:
                    diff = (data[i].start_time - last).total_seconds()
                    for j in range(len(interval_label)):
                        if intervals[j][0] <= diff <= intervals[j][1]:
                            if len(ret) > 0:
                                last_entry = ret.pop()
                                if last_entry.end_time == last and last_entry.sample == interval_label[j]:
                                    ret.append(DataPoint(start_time=last_entry.start_time,
                                                         end_time=data[i].start_time, offset=last_entry.offset,
                                                         sample=last_entry.sample))
                                else:
                                    ret.append(last_entry)
                                    ret.append(DataPoint(start_time=last, end_time=data[i].start_time,
                                                         offset=data[i].offset, sample=interval_label[j]))
                            else:
                                ret.append(DataPoint(start_time=last, end_time=data[i].start_time,
                                                     offset=data[i].offset, sample=interval_label[j]))
                            break;
                last = data[i].start_time
                i += 1
        return ret

    def process_appusage_day_data(self, user_id: str, appcategorydata: List[DataPoint],
                                  input_appcategorystream: DataStream):
        """
        Processing all app usage by category modules.

        :param str user_id: UUID of the stream owner
        :param List(DataPoint) appcategorydata: App category data stream
        :param DataStream input_appcategorystream: DataStream object of app category stream
        :return:
        """
        data = {}
        category_datapoints = []
        try:
            categories = list(set([y.sample[1] for y in appcategorydata if y.sample[1]]))
            for c in categories:
                d = self.get_appusage_duration_by_category(appcategorydata, [c], 300)

                if d:
                    newd = [{"start_time": x[0], "end_time": x[1]} for x in d]
                    data[c] = newd
                    for interval in d:
                        category_datapoints.append(DataPoint(interval[0], interval[1], appcategorydata[0].offset, c))

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
            if category_datapoints:
                self.store_stream(filepath="appusage_by_category.json",
                                  input_streams=[input_appcategorystream], user_id=user_id,
                                  data=category_datapoints, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

    def get_contact_entropy(self, data: List[str]) -> float:
        """
        Helper method to calculate entropy of a list of contacts

        :param List(str) data: List of contacts
        :return: Entropy of the given contact list
        :rtype: float
        """

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

    def get_call_daily_entropy(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Entropy of phone call for a whole day.

        :param List(DataPoint) data: Phone call number data stream
        :return: Entropy of phone call for 1 day window
        :rtype: List(DataPoint) or None
        """
        if not data:
            return None
        entropy = self.get_contact_entropy([d.sample for d in data])

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=entropy)]
        return new_data

    def get_call_hourly_entropy(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Entropy of phone call for each hour in a day.

        :param List(DataPoint) data: Phone call number data stream
        :return: Entropy of phone call for 1 hour windows
        :rtype: List(DataPoint) or None
        """
        if not data:
            return None
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)
            if len(datalist) == 0:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=self.get_contact_entropy(datalist)))
        return new_data

    def get_call_four_hourly_entropy(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Entropy of phone call for each four hour window in a day.

        :param List(DataPoint) data: Phone call number data stream
        :return: Entropy of phone call for 4 hour windows
        :rtype: List(DataPoint) or None
        """
        if not data:
            return None
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) == 0:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=self.get_contact_entropy(datalist)))
        return new_data

    def get_sms_daily_entropy(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Entropy of SMS for a whole day.

        :param List(DataPoint) data: SMS number data stream
        :return: Entropy of sms for 1 day window
        :rtype: List(DataPoint) or None
        """
        if not data:
            return None
        entropy = self.get_contact_entropy([d.sample for d in data])

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=entropy)]
        return new_data

    def get_sms_hourly_entropy(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Entropy of SMS for each hour of a day.

        :param List(DataPoint) data: SMS number data stream
        :return: Entropy of SMS for 1 hour windows
        :rtype: List(DataPoint) or None
        """
        if not data:
            return None
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) == 0:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=self.get_contact_entropy(datalist)))
        return new_data

    def get_sms_four_hourly_entropy(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Entropy of SMS for each four hour window in a day.

        :param List(DataPoint) data: SMS number data stream
        :return: Entropy of SMS for 4 hour windows
        :rtype: List(DataPoint) or None
        """
        if not data:
            return None
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) == 0:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=self.get_contact_entropy(datalist)))
        return new_data

    def get_call_sms_daily_entropy(self, calldata: List[DataPoint], smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Entropy of phone call and SMS (combined) for a day.

        :param List(DataPoint) calldata: Phone call number data stream
        :param List(DataPoint) smsdata: SMS number data stream
        :return: Entropy of phone call and SMS for 1 day windows
        :rtype: List(DataPoint) or None
        """

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

    def get_call_sms_hourly_entropy(self, calldata: List[DataPoint], smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Entropy of phone call and SMS (combined) for each one hour window in a day.

        :param List(DataPoint) calldata: Phone call number data stream
        :param List(DataPoint) smsdata: SMS number data stream
        :return: Entropy of phone call and SMS for 1 hour windows
        :rtype: List(DataPoint) or None
        """
        data = calldata + smsdata
        if not data:
            return None
        data.sort(key=lambda x: x.start_time)
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) == 0:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=self.get_contact_entropy(datalist)))
        return new_data

    def get_call_sms_four_hourly_entropy(self, calldata: List[DataPoint], smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Entropy of phone call and SMS (combined) for each four hour window in a day.

        :param List(DataPoint) calldata: Phone call number data stream
        :param List(DataPoint) smsdata: SMS number data stream
        :return: Entropy of phone call and SMS for 4 hour windows
        :rtype: List(DataPoint) or None
        """

        data = calldata + smsdata
        if not data:
            return None
        data.sort(key=lambda x: x.start_time)
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            if len(datalist) == 0:
                continue
            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=self.get_contact_entropy(datalist)))
        return new_data

    def get_total_call_daily(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Number of phone calls in a day

        :param List(DataPoint) data: Phone call number data stream
        :return: Number of phone calls in a day
        :rtype: List(DataPoint) or None
        """
        if not data:
            return None
        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset, sample=len(data))]
        return new_data

    def get_total_call_hourly(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Number of phone calls for each hour in a day.

        :param List(DataPoint) data: Phone call number data stream
        :return: number of phone calls for 1 hour windows
        :rtype: List(DataPoint) or None
        """
        if not data:
            return None
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=len(datalist)))
        return new_data

    def get_total_call_four_hourly(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Number of phone calls for each four hour in a day.

        :param List(DataPoint) data: Phone call number data stream
        :return: number of phone calls for 4 hour windows
        :rtype: List(DataPoint) or None
        """

        if not data:
            return None
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=len(datalist)))
        return new_data

    def get_total_sms_daily(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Number of SMS in a day

        :param List(DataPoint) data: SMS number data stream
        :return: Number of SMS in a day
        :rtype: List(DataPoint)
        """
        if not data:
            return None
        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=len(data))]
        return new_data

    def get_total_sms_hourly(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Number of SMS for each hour in a day.

        :param List(DataPoint) data: SMS number data stream
        :return: number of sms for 1 hour windows
        :rtype: List(DataPoint) or None
        """
        if not data:
            return None
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=len(datalist)))
        return new_data

    def get_total_sms_four_hourly(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Number of SMS for each four hour in a day.

        :param List(DataPoint) data: SMS number data stream
        :return: number of SMS for 4 hour windows
        :rtype: List(DataPoint)
        """
        if not data:
            return None
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=len(datalist)))
        return new_data

    def get_total_call_sms_daily(self, calldata: List[DataPoint], smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Number of phone and SMS for a day.

        :param List(DataPoint) calldata: Phone call number data stream
        :param List(DataPoint) smsdata: SMS number data stream
        :return: Number of phone and SMS in 1 day windows
        :rtype: List(DataPoint) or None
        """
        data = calldata + smsdata
        if not data:
            return None
        data.sort(key=lambda x: x.start_time)

        start_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        start_time = start_time.replace(tzinfo=data[0].start_time.tzinfo)
        end_time = start_time + datetime.timedelta(hours=23, minutes=59)
        new_data = [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                              sample=len(data))]
        return new_data

    def get_total_call_sms_hourly(self, calldata: List[DataPoint], smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Number of phone and SMS for each one hour of a day.

        :param List(DataPoint) calldata: Phone call number data stream
        :param List(DataPoint) smsdata: SMS number data stream
        :return: Number of phone and SMS for 1 hour windows
        :rtype: List(DataPoint) or None
        """

        data = calldata + smsdata
        if not data:
            return None
        data.sort(key=lambda x: x.start_time)
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=len(datalist)))
        return new_data

    def get_total_call_sms_four_hourly(self, calldata: List[DataPoint], smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Number of phone and SMS for each four hour window for a day.

        :param List(DataPoint) calldata: Phone call number data stream
        :param List(DataPoint) smsdata: SMS number data stream
        :return: Number of phone and SMS in 4 hour windows
        :rtype: List(DataPoint) or None
        """

        data = calldata + smsdata
        if not data:
            return None
        data.sort(key=lambda x: x.start_time)
        new_data = []
        tmp_time = datetime.datetime.combine(data[0].start_time.date(), datetime.datetime.min.time())
        tmp_time = tmp_time.replace(tzinfo=data[0].start_time.tzinfo)
        for h in range(0, 24, 4):
            datalist = []
            start = tmp_time.replace(hour=h)
            end = start + datetime.timedelta(hours=3, minutes=59)
            for d in data:
                if start <= d.start_time <= end:
                    datalist.append(d.sample)

            new_data.append(DataPoint(start_time=start, end_time=end, offset=data[0].offset,
                                      sample=len(datalist)))
        return new_data

    def process_callsmsnumber_day_data(self, user_id: str, call_number_data: List[DataPoint],
                        sms_number_data: List[DataPoint], input_callstream: DataStream, input_smsstream: DataStream):
        """
        Process all phone call number and SMS number related modules

        :param str user_id: UUID of the stream owner
        :param List(DataPoint) call_number_data: Phone call number stream
        :param List(DataPoint) sms_number_data: SMS number stream
        :param DataStream input_callstream: DataStream object of phone call number
        :param DataStream input_smsstream: DataStream object of SMS call number
        :return:
        """
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
                self.store_stream(filepath="total_call_sms_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_call_sms_four_hourly(call_number_data, sms_number_data)
            if data:
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
                self.store_stream(filepath="total_sms_four_hourly.json",
                                  input_streams=[input_callstream, input_smsstream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

    def process_callsmsstream_day_data(self, user_id: str, callstream: List[DataPoint],
                                smsstream: List[DataPoint], input_callstream: DataStream, input_smsstream: DataStream):
        """
        Process all the call and sms related features and store them as datastreams.

        :param str user_id: UUID of the stream owner
        :param List(DataPoint) callstream: Phone call duration data stream
        :param List(DataPoint) smsstream: SMS length data stream
        :param DataStream input_callstream: DataStream object of phone call stream
        :param DataStream input_smsstream: DataStream object of SMS call stream
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

    def process_light_day_data(self, user_id: str, lightdata: List[DataPoint], input_lightstream: DataStream):
        """
        Process all the ambient light related features and store the output streams.

        :param str user_id: UUID of the stream owner
        :param List(DataPoint)  lightdata: Ambient light data stream
        :param DataStream input_lightstream: DataStream object of Ambient light stream
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

    def process_proximity_day_data(self, user_id: str, proximitystream: List[DataPoint],
                                   input_proximitystream: DataStream):
        """
        Process all proximity related modules

        :param str user_id: UUID of the stream owner
        :param List(DataPoint) proximitystream: Phone proximity data stream
        :param DataStream input_proximitystream: DataStream object of proximity data stream
        :return:
        """
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

    def process_appusage_context_day_data(self, user_id: str, app_usage_data: List[DataPoint],
            input_usage_stream: DataStream, gps_semantic_data: List[DataPoint], input_gps_semantic_stream: DataStream):
        """
        Process appusage related modules

        :param str user_id: UUID of stream owner
        :param List(DataPoint) app_usage_data: App usage stream data
        :param input_usage_stream: DataStram object of app usage stream
        :param gps_semantic_data: GPS semantic location data splitted daywise (localtime false)
        :param input_gps_semantic_stream: DataStream object of the gps semantic daysiwse data
        :return:
        """
        if not app_usage_data:
            return

        total = [{}, {}, {}, {}]
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
                    context_total[1] += (gd.end_time - gd.start_time).total_seconds() / (60 * 60)
                elif gd.sample == "home":
                    context_total[2] += (gd.end_time - gd.start_time).total_seconds() / (60 * 60)
            context_total[3] = context_total[0] - context_total[1] - context_total[2]

            st = app_usage_data[0].start_time.date()
            start_time = datetime.datetime.combine(st, datetime.time.min)
            start_time = start_time.replace(tzinfo=app_usage_data[0].start_time.tzinfo)
            end_time = datetime.datetime.combine(st, datetime.time.max)
            end_time = end_time.replace(tzinfo=app_usage_data[0].start_time.tzinfo)

            dp1 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[0])
            if input_gps_semantic_stream:
                dp2 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[1])
                dp3 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[2])
                dp4 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[3])

            self.store_stream(filepath="appusage_duration_total_by_category.json",
                              input_streams=[input_usage_stream], user_id=user_id,
                              data=[dp1], localtime=False)
            if input_gps_semantic_stream:
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
            if input_gps_semantic_stream:
                dp6 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[1])
                dp7 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[2])
                dp8 = DataPoint(start_time, end_time, app_usage_data[0].offset, total[3])

            self.store_stream(filepath="appusage_duration_average_by_category.json",
                              input_streams=[input_usage_stream], user_id=user_id,
                              data=[dp5], localtime=False)
            if input_gps_semantic_stream:
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

    def process_appcategory_day_data(self, user_id: str, appcategorystream: List[DataPoint],
                                     input_appcategorystream: DataStream):
        """
        process all app category related features.

        :param str user_id: UUID of the stream owner
        :param List(DataPoint) appcategorystream: App category stream data
        :param DataStream input_appcategorystream: DataStream object of the app category stream
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


    def get_total_phone_activity_time_by_type(self, data: List[DataPoint], activity_type: float) -> List[DataPoint]:
        """
        Total time in a day of type 'activity_type'.

        :param List(DataPoint) data: Phone activity API stream data points
        :param float activity_type: The activity summary to be calculated ranging from 0 to 7
        :return: List with single data point including the total time phone found the 'activity_type' in minutes
        :rtype: List(DataPoint)
        """

        if not data:
            return None
        i = 0
        total = 0

        while i < len(data):
            if data[i].sample[0] == activity_type:
                start = data[i].start_time
                i += 1
                while i < len(data) and data[i].sample[0] == activity_type:
                    i += 1
                if i == len(data):
                    last = data[-1].start_time
                else:
                    last = data[i].start_time
                total += (last - start).total_seconds()
            i += 1

        start_time = copy.deepcopy(data[0].start_time)
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = datetime.datetime.combine(start_time.date(), datetime.time.max)
        end_time = end_time.replace(tzinfo=data[0].start_time.tzinfo)
        return [DataPoint(start_time, end_time, data[0].offset, total/60)]


    def process_phone_activity_day_data(self, user_id: str, activity_data: List[DataPoint],
                                        input_activity_stream: DataStream):
        """
        Process all phone activity API stream related features

        :param str user_id: UUID of the stream owner
        :param List(DataPoint) activity_data: Phone activity API stream data points
        :param DataStream input_activity_stream: DataStream object of phone activity data
        :return:
        """

        try:
            data = self.get_total_phone_activity_time_by_type(activity_data, IN_VEHICLE)
            self.store_stream(filepath="driving_time_from_phone_activity.json",
                              input_streams=[input_activity_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_phone_activity_time_by_type(activity_data, ON_BICYCLE)
            self.store_stream(filepath="bicycle_time_from_phone_activity.json",
                              input_streams=[input_activity_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_phone_activity_time_by_type(activity_data, STILL)
            self.store_stream(filepath="still_time_from_phone_activity.json",
                              input_streams=[input_activity_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_phone_activity_time_by_type(activity_data, ON_FOOT)
            self.store_stream(filepath="on_foot_time_from_phone_activity.json",
                              input_streams=[input_activity_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_phone_activity_time_by_type(activity_data, TILTING)
            self.store_stream(filepath="tilting_time_from_phone_activity.json",
                              input_streams=[input_activity_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_phone_activity_time_by_type(activity_data, WALKING)
            self.store_stream(filepath="walking_time_from_phone_activity.json",
                              input_streams=[input_activity_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_phone_activity_time_by_type(activity_data, RUNNING)
            self.store_stream(filepath="running_time_from_phone_activity.json",
                              input_streams=[input_activity_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_total_phone_activity_time_by_type(activity_data, UNKNOWN)
            self.store_stream(filepath="unknown_time_from_phone_activity.json",
                              input_streams=[input_activity_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

    def get_percent_initiated_call(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Percent of time the user initiated a call for a day.

        :param List(DataPoint) data: Call type stream data points
        :return: List of single data point with percent of call initiated for the day
        :rtype: List(DataPoint)
        """
        if not data:
            return None
        i = 0
        count = 0

        for d in data:
            if d.sample == OUTGOING_TYPE:
                count += 1

        start_time = copy.deepcopy(data[0].start_time)
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = datetime.datetime.combine(start_time.date(), datetime.time.max)
        end_time = end_time.replace(tzinfo=data[0].start_time.tzinfo)
        return [DataPoint(start_time, end_time, data[0].offset, 100.0*count/len(data))]

    def get_percent_initiated_sms(self, data: List[DataPoint]) -> List[DataPoint]:
        """
        Percent of time the user initiated a SMS for a day.

        :param List(DataPoint) data: SMS type stream data points
        :return: List of single data point with percent of SMS initiated for the day
        :rtype: List(DataPoint)
        """
        if not data:
            return None
        i = 0
        count = 0

        for d in data:
            if d.sample == MESSAGE_TYPE_SENT:
                count += 1

        start_time = copy.deepcopy(data[0].start_time)
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = datetime.datetime.combine(start_time.date(), datetime.time.max)
        end_time = end_time.replace(tzinfo=data[0].start_time.tzinfo)
        return [DataPoint(start_time, end_time, data[0].offset, 100.0*count/len(data))]

    def get_percent_initiated_callsms(self, calldata: List[DataPoint], smsdata: List[DataPoint]) -> List[DataPoint]:
        """
        Percent of time the user initiated a Call or SMS for a day.

        :param List(DataPoint) calldata: Call type stream data points
        :param List(DataPoint) smsdata: SMS type stream data points
        :return: List of single data point with percent of Call and SMS initiated for the day
        :rtype: List(DataPoint)
        """
        data = calldata + smsdata
        if not data:
            return None
        i = 0
        count = 0

        for d in data:
            if d.sample == OUTGOING_TYPE:
                count += 1

        start_time = copy.deepcopy(data[0].start_time)
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = datetime.datetime.combine(start_time.date(), datetime.time.max)
        end_time = end_time.replace(tzinfo=data[0].start_time.tzinfo)
        return [DataPoint(start_time, end_time, data[0].offset, 100.0*count/len(data))]

    def process_callsms_type_day_data(self, user_id: str, calltype_data: List[DataPoint], smstype_data: List[DataPoint],
                                      input_call_type_stream: DataStream, input_sms_type_stream: DataStream):
        """
        Processing all streams related to call type and sms type streams.

        :param str user_id: UUID of the stream owner
        :param List(DataPoint) calltype_data: Call type stream data points
        :param List(DataPoint) smstype_data: SMS type stream data points
        :param DataStream input_call_type_stream: DataStream object of call type stream
        :param DataStream input_sms_type_stream: DataStream object of sms type stream
        :return:
        """
        try:
            data = self.get_percent_initiated_call(calltype_data)
            self.store_stream(filepath="call_initiated_percent_daily.json",
                              input_streams=[input_call_type_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_percent_initiated_sms(smstype_data)
            self.store_stream(filepath="sms_initiated_percent_daily.json",
                              input_streams=[input_sms_type_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_percent_initiated_callsms(calltype_data, smstype_data)
            self.store_stream(filepath="callsms_initiated_percent_daily.json",
                              input_streams=[input_call_type_stream, input_sms_type_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

    def process_data(self, user_id: str, all_user_streams: dict, all_days: List[str]):
        """
        Getting all the necessary input datastreams for a user
        and run all feature processing modules for all the days
        of the user.

        :param str user_id: UUID of the stream owner
        :param dict all_user_streams: Dictionary containing all the user streams, where key is the stream name, value
                                        is the stream metadata
        :param List(str) all_days: List of all days for the processing in the format 'YYYYMMDD'
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
        input_activity_stream = None
        input_call_type_stream = None
        input_sms_type_stream = None

        call_stream_name = 'CU_CALL_DURATION--edu.dartmouth.eureka'
        sms_stream_name = 'CU_SMS_LENGTH--edu.dartmouth.eureka'
        proximity_stream_name = 'PROXIMITY--org.md2k.phonesensor--PHONE'
        cu_appusage_stream_name = 'CU_APPUSAGE--edu.dartmouth.eureka'
        light_stream_name = 'AMBIENT_LIGHT--org.md2k.phonesensor--PHONE'
        appcategory_stream_name = "org.md2k.data_analysis.feature.phone.app_usage_category"
        appusage_stream_name = "org.md2k.data_analysis.feature.phone.app_usage_interval"
        gpssemantic_stream_name = "org.md2k.data_analysis.feature.gps_semantic_location.daywise_split.utc"
        call_number_stream_name = "CU_CALL_NUMBER--edu.dartmouth.eureka"
        sms_number_stream_name = "CU_SMS_NUMBER--edu.dartmouth.eureka"
        activity_stream_name = "ACTIVITY_TYPE--org.md2k.phonesensor--PHONE"
        call_type_stream_name = "CU_CALL_TYPE--edu.dartmouth.eureka"
        sms_type_stream_name = "CU_SMS_TYPE--edu.dartmouth.eureka"

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
            elif stream_name == activity_stream_name:
                input_activity_stream = stream_metadata
            elif stream_name == call_type_stream_name:
                input_call_type_stream = stream_metadata
            elif stream_name == sms_type_stream_name:
                input_sms_type_stream = stream_metadata

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
                callstream = self.get_filtered_data(callstream, lambda x: (type(x) is float and x >= 0))
                smsstream = self.get_data_by_stream_name(sms_stream_name, user_id, day, localtime=False)
                smsstream = self.get_filtered_data(smsstream, lambda x: (type(x) is float and x >= 0))
                self.process_callsmsstream_day_data(user_id, callstream, smsstream, input_callstream, input_smsstream)

        if not input_call_type_stream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, call_type_stream_name,
                                 str(user_id)))
        elif not input_sms_type_stream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, sms_type_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                calltype_data = self.get_data_by_stream_name(call_type_stream_name, user_id, day, localtime=False)
                calltype_data = self.get_filtered_data(calltype_data, lambda x: (type(x) is float))
                smstype_data = self.get_data_by_stream_name(sms_type_stream_name, user_id, day, localtime=False)
                smstype_data = self.get_filtered_data(smstype_data, lambda x: (type(x) is float))
                self.process_callsms_type_day_data(user_id, calltype_data, smstype_data, input_call_type_stream,
                                                   input_sms_type_stream)

        # processing proximity sensor related features
        if not input_proximitystream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, proximity_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                proximitystream = self.get_data_by_stream_name(proximity_stream_name, user_id, day)
                proximitystream = self.get_filtered_data(proximitystream, lambda x: (type(x) is float and x >= 0))
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
                lightstream = self.get_filtered_data(lightstream, lambda x: (type(x) is float and x >= 0))
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


        latest_appcategorystreamid = self.get_latest_stream_id(user_id,
                                                               appcategory_stream_name)

        if not latest_appcategorystreamid:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, appcategory_stream_name,
                                 str(user_id)))
        else:
            input_appcategorystream = self.CC.get_stream_metadata(latest_appcategorystreamid[0]['identifier'])
            for day in all_days:
                appcategorydata = self.get_data_by_stream_name(appcategory_stream_name, user_id,
                                             day, localtime=False, ingested_stream=False)
                appcategorydata = self.get_filtered_data(appcategorydata, lambda x: (type(x) is list and len(x) == 4))
                self.process_appusage_day_data(user_id, appcategorydata, input_appcategorystream)

        streams = self.CC.get_user_streams(user_id)
        if not streams or not len(streams):
            self.CC.logging.log('No streams found for user %s for feature %s'
                                % (str(user_id), self.__class__.__name__))
            return

        
        latest_appusage_streamid = self.get_latest_stream_id(user_id,
                                                             appusage_stream_name)
        latest_gps_semantic_streamid = self.get_latest_stream_id(user_id,
                                                             gpssemantic_stream_name)

        if not latest_appusage_streamid:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, appusage_stream_name,
                                 str(user_id)))
        else:
            input_appusage_stream = self.CC.get_stream_metadata(latest_appusage_streamid[0]['identifier'])
            input_gpssemanticstream = self.CC.get_stream_metadata(latest_gps_semantic_streamid[0]['identifier'])
            for day in all_days:
                app_usage_data = self.get_data_by_stream_name(appusage_stream_name, user_id, day,
                                             localtime=False,
                                             ingested_stream=False)
                app_usage_data = self.get_filtered_data(app_usage_data, lambda x: type(x) is dict)

                gps_semantic_data = self.get_data_by_stream_name(gpssemantic_stream_name, user_id,
                                             day, localtime=False, ingested_stream=False)
                gps_semantic_data = self.get_filtered_data(gps_semantic_data,
                                                           lambda x: ((type(x) is str) or (type(x) is np.str_)))

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


        # processing phone activity data related features
        if not input_activity_stream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, activity_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                activity_data = self.get_data_by_stream_name(activity_stream_name, user_id, day, localtime=False)
                activity_data = self.get_filtered_data(activity_data, lambda x: (type(x) is list and len(x) == 2))
                self.process_phone_activity_day_data(user_id, activity_data, input_activity_stream)

    def process(self, user_id: str, all_days: List[str]):
        """
        Main processing function inherited from ComputerFeatureBase

        :param str user_id: UUID of the user
        :param List(str) all_days: List of days with format 'YYYYMMDD'
        :return:
        """
        if self.CC is not None:
            self.CC.logging.log("Processing PhoneFeatures")
            streams = self.CC.get_user_streams(user_id)
            self.process_data(user_id, streams, all_days)

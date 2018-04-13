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
import numpy as np
from datetime import timedelta
import traceback
from sklearn.mixture import GaussianMixture
from typing import List, Callable, Any

feature_class_name = 'PhoneScreenTouchFeatures'


class PhoneScreenTouchFeatures(ComputeFeatureBase):

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
        return [d for d in data if admission_control(d.sample)]

    def get_data_by_stream_name(self, stream_name: str, user_id: str, day: str,
                                localtime: bool=True) -> List[DataPoint]:
        """
        Combines data from multiple streams data of same stream based on stream name.

        :param str stream_name: Name of the stream
        :param str user_id: UUID of the stream owner
        :param str day: The day (YYYYMMDD) on which to operate
        :param bool localtime: The way to structure time, True for operating in participant's local time, False for UTC
        :return: Combined stream data if there are multiple stream id
        :rtype: List(DataPoint)
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
            while i<len(data) and data[i].start_time<a[0]:
                i += 1
            last = 0
            while i<len(data) and data[i].start_time <= a[1]:
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

    def process_phonescreen_all_day_data(self, user_id: str, all_days: List[str],
                                         touchescreen_stream_name: str, appcategory_stream_name: str):
        """

        :param user_id:
        :param all_days:
        :param touchescreen_stream_name:
        :param appcategory_stream_name:
        :return:
        """
        MIN_TAP_DATA = 100
        td = []
        appd = []
        for day in all_days:
            touchstream = self.get_data_by_stream_name(touchescreen_stream_name, user_id, day)
            touchstream = self.get_filtered_data(touchstream, lambda x: (type(x) is float and x>1000000000.0))
            td += touchstream
            appcategorystream  = self.get_data_by_stream_name(appcategory_stream_name, user_id, day)
            appcategorystream = self.get_filtered_data(appcategorystream, lambda x: (type(x) is list and len(x)==4))
            appd += appcategorystream

        td = sorted(td, key=lambda x: x.start_time)

        appusage = self.get_appusage_duration_by_category(appd, ["Communication", "Productivity"])
        tapping_gap = self.appusage_interval_list(td, appusage)
        if len(tapping_gap) < MIN_TAP_DATA:
            self.CC.logging.log("Not enough screen touch data")
            return None
        tapping_gap = sorted(tapping_gap)

        gm = GaussianMixture(n_components = 4, max_iter = 500)#, covariance_type = 'spherical')
        X = (np.array(tapping_gap)/1000).reshape(-1, 1)
        gm.fit(X)
        return gm

    def process_phonescreen_day_data(self, user_id, touchstream, categorystream, \
                                     input_touchstream, input_categorystream, gm):
        """
        Analyze the phone touch screen gap to find typing, pause between typing, reading
        and unknown sessions. It uses the Gaussian Mixture algorithm to find different peaks
        in a mixture of 4 different gaussian distribution of screen touch gap.
        :param user_id:
        :param touchstream:
        :param categorystream:
        :param input_touchstream:
        :param input_categorystream:
        :return:
        """
        touchstream = sorted(touchstream, key=lambda x: x.start_time)

        appusage = self.get_appusage_duration_by_category(categorystream, ["Communication", "Productivity"])
        tapping_gap = self.appusage_interval_list(touchstream, appusage)
        #         if len(tapping_gap) < 50:
        #             self.CC.logging.log("Not enough screen touch data")
        #             return
        tapping_gap = sorted(tapping_gap)
        if len(tapping_gap)==0:
            self.CC.logging.log("Not enough screen touch data")
            return

        #gm = GaussianMixture(n_components = 4, max_iter = 500)#, covariance_type = 'spherical')
        X = (np.array(tapping_gap)/1000).reshape(-1, 1)
        #gm.fit(X)

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
            data = self.label_appusage_intervals(touchstream, appusage, intervals,
                                                 ["typing", "pause", "reading", "unknown"])
            if data:
                self.store_stream(filepath="phone_touch_type.json",
                                  input_streams=[input_touchstream, input_categorystream],
                                  user_id=user_id, data=data, localtime=False)
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

        input_touchscreenstream = None
        input_appcategorystream = None

        touchescreen_stream_name = "TOUCH_SCREEN--org.md2k.phonesensor--PHONE"
        appcategory_stream_name = "org.md2k.data_analysis.feature.phone.app_usage_category"

        streams = all_user_streams
        days = None

        if not streams or not len(streams):
            self.CC.logging.log('No streams found for user %s for feature %s'
                                % (str(user_id), self.__class__.__name__))
            return

        for stream_name, stream_metadata in streams.items():
            if stream_name == touchescreen_stream_name:
                input_touchscreenstream = stream_metadata
            elif stream_name == appcategory_stream_name:
                input_appcategorystream = stream_metadata

        if not input_touchscreenstream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, touchescreen_stream_name,
                                 str(user_id)))

        elif not input_appcategorystream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, appcategory_stream_name,
                                 str(user_id)))
        else:
            gm = self.process_phonescreen_all_day_data(user_id, all_days, touchescreen_stream_name,
                                                       appcategory_stream_name)
            if gm:
                for day in all_days:
                    touchstream = self.get_data_by_stream_name(touchescreen_stream_name, user_id, day)
                    touchstream = self.get_filtered_data(touchstream, lambda x: (type(x) is float and x>=0))
                    appcategorystream  = self.get_data_by_stream_name(appcategory_stream_name, user_id, day)
                    appcategorystream = self.get_filtered_data(appcategorystream,
                                                               lambda x: (type(x) is list and len(x)==4))
                    self.process_phonescreen_day_data(user_id, touchstream, appcategorystream, input_touchscreenstream,
                                                      input_appcategorystream, gm)

    def process(self, user_id, all_days):
        if self.CC is not None:
            self.CC.logging.log("Processing PhoneTouchScreenFeatures")
            streams = self.CC.get_user_streams(user_id)
            self.process_data(user_id, streams, all_days)

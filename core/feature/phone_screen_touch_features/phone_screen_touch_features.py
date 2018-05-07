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
import datetime
import traceback
import copy
from sklearn.mixture import GaussianMixture
from typing import List, Callable, Any

feature_class_name = 'PhoneScreenTouchFeatures'


class PhoneScreenTouchFeatures(ComputeFeatureBase):
    """
    Compute all features related to phone touch screen which needed all days data of a user and can not be paralleled.
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

    def inter_event_time_list(self, data: List[DataPoint]) -> List[float]:
        """
        Helper function to compute inter-event times
        :param List(DataPoint) data: A list of DataPoints
        :return: Time deltas between DataPoints in seconds
        :rtype: list(float)
        """
        if not data:
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

        return list(filter(lambda x: x != 0.0, ret))

    def get_screen_touch_variance_hourly(self, data: List[DataPoint], typing_episodes: List) -> List[DataPoint]:
        """
        This method returns hourly variance of time between two consecutive touch in a typing episode. In case of
        multiple typing episode, variance is calculated for each typing episode and combined using standard formula
        to combine multiple variances.

        :param List(DataPoint) data: screen touch stream data points
        :param List(Tuple) typing_episodes: (start_time, end_time) for each item in the list, the starting and end time
                                            of a typing episode
        :return: A list of variances for each hour (if there is input data for this hour) of a day.
        :rtype: List(DataPoint)
        """
        if len(data) <= 1:
            return None

        combined_data = copy.deepcopy(data)

        for s in combined_data:
            s.end_time = s.start_time

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
            splitted_data = [[]]*len(typing_episodes)
            for i, ep in enumerate(typing_episodes):
                for d in datalist:
                    if ep[0]<= d.start_time and d.end_time <= ep[1]:
                        splitted_data[i].append(d)
            splitted_data = list(filter(lambda x: len(x)>1, splitted_data))
            if not splitted_data:
                continue
            episode_data = list(map(self.inter_event_time_list, splitted_data))
            Xc = np.mean(episode_data)
            var = 0
            n = 0
            for L in episode_data:
                X = np.mean(L)
                V = np.var(L)
                var += len(L) * (V + (X - Xc)*(X - Xc))
                n += len(L)
            var /= n
            if np.isnan(var):
                continue

            new_data.append(DataPoint(start_time=start, end_time=end, offset=combined_data[0].offset,
                                      sample=var))
        return new_data

    def get_screen_touch_rate(self, data: List[DataPoint], typing_episodes: List) -> List[DataPoint]:
        """
        Average screen touch rate for a whole day during typing episodes (only productivity and communication apps are
        considered during calculation)

        :param List(DataPoint) data: screen touch stream data points
        :param List(Tuple) typing_episodes: (start_time, end_time) for each item in the list, the starting and end time
                                            of a typing episode
        :return: A list with single data point containing the average screen touch rate.
        :rtype: List(DataPoint)
        """
        if not data:
            return None
        total_touch_count = 0
        total_typing_time = 0
        for ep in typing_episodes:
            total_typing_time += (ep[1] - ep[0]).total_seconds()
            for d in data:
                if ep[0] <= d.start_time <= ep[1]:
                    total_touch_count += 1

        if total_typing_time == 0 or total_touch_count == 0:
            return None

        start_time = copy.deepcopy(data[0].start_time)
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = datetime.datetime.combine(start_time.date(), datetime.time.max)
        end_time = end_time.replace(tzinfo=data[0].start_time.tzinfo)
        return [DataPoint(start_time=start_time, end_time=end_time, offset=data[0].offset,
                          sample=total_touch_count/total_typing_time)]

    def get_typing_episodes(self, typing_episodes: List) -> List[DataPoint]:
        new_data = []
        for d in typing_episodes:
            new_data.append(DataPoint(d[0], d[1], 0, (d[1] - d[0]).total_seconds()))
        return new_data

    def process_screentouch_type_day_data(self, user_id, touchtypedata, touchscreendata, input_touchtype_stream,
                                          input_touchscreen_stream):
        """
        This method is responsible to calculate and store the screen touch related features, for example, hourly
        variance of screen touch time gap.

        :param user_id: UUID of the stream owner
        :param touchtypedata: screen touch type stream data points
        :param touchscreendata: screen touch time stream data points
        :param input_touchtype_stream: touch type stream object
        :param input_touchscreen_stream: touch time stream object
        :return:
        """
        typing_episodes = []
        pos = 0
        while pos < len(touchtypedata):
            while pos < len(touchtypedata):
                t = touchtypedata[pos]
                if t.sample in ["typing", "pause", "reading"]:
                    break
                pos += 1
            if pos == len(touchtypedata):
                break
            start = pos
            pos += 1
            while pos < len(touchtypedata):
                t = touchtypedata[pos]
                if t.sample not in ["typing", "pause", "reading"]:
                    break
                t1 = touchtypedata[pos-1]
                if t1.end_time != t.start_time:
                    break
                pos += 1
            typing_episodes.append((touchtypedata[start].start_time, touchtypedata[pos-1].start_time))

        try:
            data = self.get_typing_episodes(typing_episodes)
            self.store_stream(filepath="phone_typing_episode.json",
                              input_streams=[input_touchtype_stream, input_touchscreen_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_screen_touch_variance_hourly(touchscreendata, typing_episodes)
            self.store_stream(filepath="phone_touch_response_time_variance.json",
                              input_streams=[input_touchtype_stream, input_touchscreen_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = self.get_screen_touch_rate(touchscreendata, typing_episodes)
            self.store_stream(filepath="phone_screen_touch_rate.json",
                              input_streams=[input_touchtype_stream, input_touchscreen_stream], user_id=user_id,
                              data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

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
                                         touchescreen_stream_name: str, appcategory_stream_name: str,
                                         input_touchstream: DataStream, input_categorystream: DataStream) -> GaussianMixture:
        """
        This method create a unsupervised model using screen touch gap during productivity and communication app usage.

        :param str user_id: UUID of the user.
        :param List(str) all_days: List of days with format 'YYYYMMDD'
        :param str touchescreen_stream_name: Phone touch screen stream name
        :param str appcategory_stream_name: App category stream name
        :return: GaussianMixture object of the created model
        :rtype: GaussianMixture

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
        P = gm.predict(X)
        values = [[], [], [], []]
        for idx in range(len(X)):
            values[P[idx]].append(X[idx][0])
        parameters = []
        for i in range(4):
            print(np.mean(values[i]), np.std(values[i]))
            parameters.append((np.mean(values[i]), np.std(values[i])))
        parameters.sort()
        try:
            data = []
            for day in all_days:
                start_time = datetime.datetime.strptime(day,"%Y%m%d")
                start_time = start_time.replace(tzinfo=datetime.timezone.utc)
                end_time = datetime.datetime.combine(start_time.date(), datetime.time.max)
                end_time = end_time.replace(tzinfo=datetime.timezone.utc)
                data.append(DataPoint(start_time, end_time, 0, [parameters[0][0], parameters[0][1]]))
            if data:
                self.store_stream(filepath="phone_active_typing_parameters.json",
                                  input_streams=[input_touchstream, input_categorystream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = []
            for day in all_days:
                start_time = datetime.datetime.strptime(day,"%Y%m%d")
                start_time = start_time.replace(tzinfo=datetime.timezone.utc)
                end_time = datetime.datetime.combine(start_time.date(), datetime.time.max)
                end_time = end_time.replace(tzinfo=datetime.timezone.utc)
                data.append(DataPoint(start_time, end_time, 0, [parameters[1][0], parameters[1][1]]))
            if data:
                self.store_stream(filepath="phone_typing_pause_parameters.json",
                                  input_streams=[input_touchstream, input_categorystream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        try:
            data = []
            for day in all_days:
                start_time = datetime.datetime.strptime(day,"%Y%m%d")
                start_time = start_time.replace(tzinfo=datetime.timezone.utc)
                end_time = datetime.datetime.combine(start_time.date(), datetime.time.max)
                end_time = end_time.replace(tzinfo=datetime.timezone.utc)
                data.append(DataPoint(start_time, end_time, 0, [parameters[2][0], parameters[2][1]]))
            if data:
                self.store_stream(filepath="phone_reading_in_typing_parameters.json",
                                  input_streams=[input_touchstream, input_categorystream],
                                  user_id=user_id, data=data, localtime=False)
        except Exception as e:
            self.CC.logging.log("Exception:", str(e))
            self.CC.logging.log(str(traceback.format_exc()))

        return gm

    def process_phonescreen_day_data(self, user_id: str, touchstream: List[DataPoint], categorystream: List[DataPoint],
                                input_touchstream: DataStream, input_categorystream: DataStream, gm: GaussianMixture):
        """
        Analyze the phone touch screen gap to find typing, pause between typing, reading
        and unknown sessions. It uses the Gaussian Mixture algorithm to find different peaks
        in a mixture of 4 different gaussian distribution of screen touch gap.

        :param str user_id: UUID of the stream owner
        :param List(DataPoint) touchstream: Phone touch screen stream data
        :param List(DataPoint) categorystream: ApP category stream data
        :param DataStream input_touchstream: DataStream object of phone touch screen
        :param DataStream input_categorystream: DataStream object of app category stream
        :param GaussianMixture gm: GaussianMixture object created from all day data of the user
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
                                                       appcategory_stream_name, input_touchscreenstream,
                                                       input_appcategorystream)
            if gm:
                for day in all_days:
                    touchstream = self.get_data_by_stream_name(touchescreen_stream_name, user_id, day)
                    touchstream = self.get_filtered_data(touchstream, lambda x: (type(x) is float and x>=0))
                    appcategorystream  = self.get_data_by_stream_name(appcategory_stream_name, user_id, day)
                    appcategorystream = self.get_filtered_data(appcategorystream,
                                                               lambda x: (type(x) is list and len(x)==4))
                    self.process_phonescreen_day_data(user_id, touchstream, appcategorystream, input_touchscreenstream,
                                                      input_appcategorystream, gm)


        input_touchtype_stream = None
        touchtype_stream_name = 'org.md2k.data_analysis.feature.phone.touch_type'

        streams = self.CC.get_user_streams(user_id)
        for stream_name, stream_metadata in streams.items():
            if stream_name == touchtype_stream_name:
                input_touchtype_stream = stream_metadata
            elif stream_name == touchescreen_stream_name:
                input_touchscreen_stream = stream_metadata

        if not input_touchtype_stream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, touchtype_stream_name,
                                 str(user_id)))
        elif not input_touchscreen_stream:
            self.CC.logging.log("No input stream found FEATURE %s STREAM %s "
                                "USERID %s" %
                                (self.__class__.__name__, touchescreen_stream_name,
                                 str(user_id)))
        else:
            for day in all_days:
                touchtypedata = self.get_data_by_stream_name(touchtype_stream_name, user_id, day, localtime=False)
                touchtypedata = self.get_filtered_data(touchtypedata, lambda x: (type(x) is str and
                                                                        x in ["typing", "pause", "reading", "unknown"]))
                touchscreendata = self.get_data_by_stream_name(touchescreen_stream_name, user_id, day, localtime=False)
                touchscreendata = self.get_filtered_data(touchscreendata, lambda x: (type(x) is float or
                                                            (type(x) is list and len(x)==1 and type(x[0]) is float)))
                for d in touchscreendata:
                    if type(d.sample) is list:
                        d.sample = d.sample[0]
                self.process_screentouch_type_day_data(user_id, touchtypedata, touchscreendata,
                                                       input_touchtype_stream, input_touchscreen_stream)

    def process(self, user_id: str, all_days: List[str]):
        """
        Main processing function inherited from ComputerFeatureBase

        :param str user_id: UUID of the user
        :param List(str) all_days: List of days with format 'YYYYMMDD'
        :return:
        """
        if self.CC is not None:
            self.CC.logging.log("Processing PhoneTouchScreenFeatures")
            streams = self.CC.get_user_streams(user_id)
            self.process_data(user_id, streams, all_days)

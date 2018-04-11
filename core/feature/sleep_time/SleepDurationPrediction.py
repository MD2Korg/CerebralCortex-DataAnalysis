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


import argparse
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint

import numpy
from datetime import datetime, timedelta, timezone, tzinfo


from core.feature.sleep_duration.SleepUnsupervisedPredictor import SleepUnsupervisedPredictor

# Sleep duration calculatation works from 8 PM (12 - 4) to 8 PM (12 + 20)
DAY_START_HOUR = -4;
DAY_END_HOUR = 20;

class SleepDurationPredictor:

    def __init__(self, CC):
        self.CC = CC

    def get_time_range(self, day):
        """
        Calculates the time range for example 8 PM of previous day
        to 8 PM of current day
        """
        start_time = day + timedelta(hours=DAY_START_HOUR)
        end_time = day + timedelta(hours=DAY_END_HOUR)
        return start_time, end_time


    def get_data_by_stream_name(self, stream_name, user_id, day, localtime=False):
        """
        Get all the data under a single stream name by gathering all the stream ids
        and getting data from them
        """
        stream_ids = self.CC.get_stream_id(user_id, stream_name)
        data = []
        for stream in stream_ids:
            d = self.CC.get_stream(stream['identifier'], user_id = user_id, day=day, localtime=localtime).data
            if d:
                data += d

        return data


    def get_data(self, stream_name, user_id, start_time, end_time, admission_control = None):
        """
        Get data for sleep, as it is requires 24 hour window data from two days
        """
        start_date = start_time.date()
        end_date = end_time.date()
        allday_data = []


        while start_date <= end_date:
            ds = self.get_data_by_stream_name(stream_name, user_id, start_date.strftime("%Y%m%d"))
            for d in ds:
                if not d.start_time.tzinfo:
                    d.start_time = d.start_time.replace(tzinfo=timezone.utc)
                if start_time.replace(tzinfo=timezone(timedelta(milliseconds=d.offset))) \
                        <= d.start_time <= end_time.replace(tzinfo=timezone(timedelta(milliseconds=d.offset))) \
                        and (admission_control == None or admission_control(d.sample)):
                    allday_data.append(d)
            start_date += timedelta(days = 1)

        return allday_data


    def get_sleep_duration(self, user_id, day):
        """
        Calculates the sleep duration and returns a DataPoint with sample is list with index 0 as sleep duration
        It gets four streams (light, activity from android, phone screen on/off and audio energy) and convert them
        into a list of size 24*60*60 (total seconds in 24 hours). Then a unsupervised method written in
        SleepUnsupervisedPredictor class is used to get the sleep duration in hours. If there is not enough data,
        then it returns None.
        """
        start_time, end_time = self.get_time_range(day)
        streams = self.CC.get_user_streams(user_id = user_id)
        if streams is None:
            return None
        flag = 0
        for stream_name,stream_metadata in streams.items():
            if stream_name=='ACTIVITY_TYPE--org.md2k.phonesensor--PHONE':
                activity_data = self.get_data('ACTIVITY_TYPE--org.md2k.phonesensor--PHONE', user_id, \
                                    start_time, end_time, lambda x: (type(x) == list and len(x) == 2 \
                                        and type(x[0])==float and type(x[1]) == float and 0<=x[0]<=7 and 0<=x[1]<=100))

                if len(activity_data)<100:
                    return None
                flag += 1
                if activity_data:
                    st = start_time.replace(tzinfo=timezone(timedelta(milliseconds=activity_data[0].offset)))
                activity_still =  numpy.full(int((end_time - start_time).total_seconds()), True, dtype=bool)
                for avt in activity_data:
                    idx = int((avt.start_time - st).total_seconds())
                    if avt.sample[0] != 0.0:
                        activity_still[idx] = False

            elif stream_name=='CU_IS_SCREEN_ON--edu.dartmouth.eureka':
                screen_data = self.get_data('CU_IS_SCREEN_ON--edu.dartmouth.eureka', user_id, \
                                            start_time, end_time, lambda x: type(x) == str and
                                                                            (x.strip()=="true" or x.strip() == "false"))

                if len(screen_data)<10:
                    return None
                screen_off =  -1 * numpy.ones(int((end_time - start_time).total_seconds()), dtype=int)
                if screen_data:
                    st = start_time.replace(tzinfo=timezone(timedelta(milliseconds=screen_data[0].offset)))

                last_time = 0
                for scr in screen_data:

                    if scr.sample.strip() == "true":
                        val = 1
                    else:
                        val = 0;


                    idx = int((scr.start_time - st).total_seconds())
                    if idx>last_time:
                        screen_off[last_time:idx] = val
                    last_time = idx
                flag += 1
                if len(screen_data)>0:
                    scr = screen_data[-1]
                    if scr.sample.strip() == "true":
                        val = 0
                    else:
                        val = 1;
                    if len(screen_off)>last_time:
                        screen_off[last_time:] = val

            elif stream_name=='CU_AUDIO_ENERGY--edu.dartmouth.eureka':
                audio_data = self.get_data('CU_AUDIO_ENERGY--edu.dartmouth.eureka', user_id, \
                                           start_time, end_time, lambda x: type(x) == float )

                if len(audio_data)<1000:
                    return None
                audio_amp =  numpy.zeros(int((end_time - start_time).total_seconds()), dtype=float)
                if audio_data:
                    st = start_time.replace(tzinfo=timezone(timedelta(milliseconds=audio_data[0].offset)))
                cnt = 0
                flag += 1
                for ad in audio_data:
                    idx = int((ad.start_time - st).total_seconds())
                    audio_amp[idx] = max(audio_amp[idx], ad.sample)

            elif stream_name=='AMBIENT_LIGHT--org.md2k.phonesensor--PHONE':
                light_data = self.get_data('AMBIENT_LIGHT--org.md2k.phonesensor--PHONE', user_id, \
                                           start_time, end_time, lambda x: type(x) == float)

                if len(light_data)<1000:
                    return None
                flag += 1
                light_readings =  numpy.zeros(int((end_time - start_time).total_seconds()), dtype=float)
                if light_data:
                    st = start_time.replace(tzinfo=timezone(timedelta(milliseconds=light_data[0].offset)))
                for ld in light_data:
                    idx = int((ld.start_time - st).total_seconds())
                    light_readings[idx] = max(light_readings[idx], ld.sample)

        if flag < 4:
            return None
        sleep_predictor = SleepUnsupervisedPredictor()
        longest_start_idx, longest_end_idx, max_idx = \
            sleep_predictor.predict(audio_amp, light_readings, activity_still, screen_off);
        sleep_duration = (longest_end_idx - longest_start_idx) / 8.0
        # print(sleep_duration, longest_start_idx, longest_end_idx, max_idx)
        sample = [sleep_duration]
        utc_day = day.replace(tzinfo=light_data[0].start_time.tzinfo)
        temp = DataPoint(utc_day, utc_day + timedelta(hours=23, minutes=59), light_data[0].offset, sample)
        return temp




    def get_sleep_time(self, user_id, day):
        """
        Calculates the sleep duration and returns a DataPoint with sample is list with index 0 as sleep duration
        It gets four streams (light, activity from android, phone screen on/off and audio energy) and convert them
        into a list of size 24*60*60 (total seconds in 24 hours), index 1 is the sleep onset or start time in localtime
        and index 2 is the sleep offset or end time. Then a unsupervised method written in SleepUnsupervisedPredictor class
        is used to get the sleep duration in hours. If there is not enough data, then it returns None.
        """
        start_time, end_time = self.get_time_range(day)
        streams = self.CC.get_user_streams(user_id = user_id)
        if streams is None:
            return None
        flag = 0
        if type(streams) is list:
            return None
        for stream_name,stream_metadata in streams.items():
            if stream_name=='ACTIVITY_TYPE--org.md2k.phonesensor--PHONE':
                activity_data = self.get_data('ACTIVITY_TYPE--org.md2k.phonesensor--PHONE', user_id, \
                                    start_time, end_time, lambda x: (type(x) == list and len(x) == 2 \
                                        and type(x[0])==float and type(x[1]) == float and 0<=x[0]<=7 and 0<=x[1]<=100))

                if len(activity_data)<100:
                    return None
                flag += 1
                if activity_data:
                    st = start_time.replace(tzinfo=timezone(timedelta(milliseconds=activity_data[0].offset)))
                activity_still =  numpy.full(int((end_time - start_time).total_seconds()), True, dtype=bool)
                for avt in activity_data:
                    idx = int((avt.start_time - st).total_seconds())
                    if avt.sample[0] != 0.0:
                        activity_still[idx] = False

            elif stream_name=='CU_IS_SCREEN_ON--edu.dartmouth.eureka':
                screen_data = self.get_data('CU_IS_SCREEN_ON--edu.dartmouth.eureka', user_id, \
                        start_time, end_time, lambda x: type(x) == str and (x.strip()=="true" or x.strip() == "false"))

                if len(screen_data)<10:
                    return None
                screen_off = -1 * numpy.ones(int((end_time - start_time).total_seconds()), dtype=int)
                if screen_data:
                    st = start_time.replace(tzinfo=timezone(timedelta(milliseconds=screen_data[0].offset)))

                last_time = 0
                for scr in screen_data:

                    if scr.sample.strip() == "true":
                        val = 1
                    else:
                        val = 0;
                    idx = int((scr.start_time - st).total_seconds())
                    if idx>last_time:
                        screen_off[last_time:idx] = val
                    last_time = idx
                flag += 1
                if len(screen_data)>0:
                    scr = screen_data[-1]
                    if scr.sample.strip() == "true":
                        val = 0
                    else:
                        val = 1;
                    if len(screen_off)>last_time:
                        screen_off[last_time:] = val

            elif stream_name=='CU_AUDIO_ENERGY--edu.dartmouth.eureka':
                audio_data = self.get_data('CU_AUDIO_ENERGY--edu.dartmouth.eureka', user_id, \
                                           start_time, end_time, lambda x: type(x) == float )

                if len(audio_data)<1000:
                    return None
                audio_amp =  numpy.zeros(int((end_time - start_time).total_seconds()), dtype=float)
                if audio_data:
                    st = start_time.replace(tzinfo=timezone(timedelta(milliseconds=audio_data[0].offset)))
                cnt = 0
                flag += 1
                for ad in audio_data:
                    idx = int((ad.start_time - st).total_seconds())
                    audio_amp[idx] = max(audio_amp[idx], ad.sample)

            elif stream_name=='AMBIENT_LIGHT--org.md2k.phonesensor--PHONE':
                light_data = self.get_data('AMBIENT_LIGHT--org.md2k.phonesensor--PHONE', user_id, \
                                           start_time, end_time, lambda x: type(x) == float)

                if len(light_data)<1000:
                    return None
                flag += 1
                light_readings =  numpy.zeros(int((end_time - start_time).total_seconds()), dtype=float)
                if light_data:
                    st = start_time.replace(tzinfo=timezone(timedelta(milliseconds=light_data[0].offset)))
                for ld in light_data:
                    idx = int((ld.start_time - st).total_seconds())
                    light_readings[idx] = max(light_readings[idx], ld.sample)

        if flag < 4:
            return None
        sleep_predictor = SleepUnsupervisedPredictor()
        longest_start_idx, longest_end_idx, max_idx = \
            sleep_predictor.predict(audio_amp, light_readings, activity_still, screen_off);
        sleep_duration = (longest_end_idx - longest_start_idx) / 8.0
        # print(sleep_duration, longest_start_idx, longest_end_idx, max_idx)
        sample = [sleep_duration]
        utc_day = day.replace(tzinfo=light_data[0].start_time.tzinfo)
        onset = start_time.replace(tzinfo=timezone(timedelta(milliseconds=light_data[0].offset)))+\
                timedelta(minutes=longest_start_idx * 7.5)
        #onset = onset.astimezone()
        sample.append(onset)
        offset = start_time.replace(tzinfo=timezone(timedelta(milliseconds=light_data[0].offset)))+\
                 timedelta(minutes=longest_end_idx * 7.5)
        #offset = offset.astimezone()
        sample.append(offset)

        temp = DataPoint(utc_day, utc_day + timedelta(hours=23, minutes=59), light_data[0].offset, sample)
        return temp





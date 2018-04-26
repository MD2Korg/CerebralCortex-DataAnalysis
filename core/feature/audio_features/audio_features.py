# Copyright (c) 2018, MD2K Center of Excellence
# -Rabin Banjade <rbnjade1@memphis.edu;rabin.banjade@gmail.com>
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


import os
import datetime
import json
import uuid
import traceback
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.util.data_types import DataPoint
from core.computefeature import ComputeFeatureBase
from core.signalprocessing.window import window
from time import mktime
from collections import defaultdict
from typing import List

feature_class_name = 'AudioFeatures'


class AudioFeatures(ComputeFeatureBase):
    """
    This class is based on audio_inference features that label data as voiced or
    noise. Based on whether the data sample is voice or noise, number of minutes
    of voice in an hour and average minutes on a day based on amount of data pre
    sent.

    """

    def mark_audio_stream(self, streams: dict, stream1_name: str, stream2_name: str, user_id: str, day: str):
        """Fetches audio stream and office time stream to do calculations and assigns respective ids as input streams

        Args:
            streams: Input dict of DataPoints
            stream1_name: Input stream 1
            stream2_name: Input stream 2
            user_id: User id
            day: day for which calculation is done

        """

        input_streams_audio = []
        input_streams_audio_work = []
        if stream1_name in streams:
            stream_id = streams[stream1_name]["identifier"]
            stream_name = streams[stream1_name]["name"]
            input_streams_audio.append({"identifier": stream_id, "name": stream_name})

            stream1 = self.CC.get_stream(stream_id, user_id=user_id, day=day, localtime=True)

            if len(stream1.data) > 0:
                self.audio_context(user_id, input_streams_audio, stream1.data, input_streams_audio)

        if stream1_name and stream2_name in streams:
            stream_id1 = streams[stream1_name]["identifier"]
            stream_name1 = streams[stream1_name]["name"]
            input_streams_audio_work.append({"identifier": stream_id1, "name": stream_name1})

            stream_id2 = streams[stream2_name]["identifier"]
            stream_name2 = streams[stream2_name]["name"]
            input_streams_audio_work.append({"identifier": stream_id2, "name": stream_name2})

            stream1 = self.CC.get_stream(stream_id1, user_id=user_id, day=day, localtime=True)
            stream2 = self.CC.get_stream(stream_id2, user_id=user_id, day=day, localtime=True)

            if len(stream1.data) > 0 and len(stream2.data) > 0:
                self.audio_context(user_id, input_streams_audio, stream1.data, input_streams_audio_work, stream2.data)

    def audio_context(self, user_id: str, input_streams_audio: dict,
                      stream1_data: list,
                      input_streams_audio_work: dict = None,
                      stream2_data: list = None):

        """redirects appropriate streams for appropriate calculations.

        takes raw input stream and labels every minute window as voiced or noise
        based on threshold of 20 secs.

        Notes:
            window(input_audio inference stream) on 1 minute
            if voiced segments >= 20 secs
                label minute window as 'voiced'
            else:
                label minute window as 'noise'

        Args:
            user_id: id of user
            input_streams_audio: input stream for audio for whole day
            stream1_data: list of Datapoints
            input_streams_audio_work: input stream for audio for office only
            stream2_data: list of DataPoints

        """

        if len(stream1_data) > 0:
            audio_stream = stream1_data
            timestamp = []

            for items in audio_stream:
                timestamp_in_unix = mktime(items.start_time.timetuple())
                timestamp.append(items.start_time)
            windowed_data = window(audio_stream, 60, False)

            no_voiced_segments = 0
            for key in windowed_data:
                temp_voice = 0
                temp_noise = 0
                for idx, val in enumerate(windowed_data[key]):
                    if (idx + 1) < len(windowed_data[key]):
                        if val.sample[0] == "noise":
                            temp_noise += (windowed_data[key][idx + 1].start_time
                                           - val.start_time).total_seconds()
                        else:
                            temp_voice += (windowed_data[key][idx + 1].start_time
                                           - val.start_time).total_seconds()

                if temp_voice >= 20:
                    windowed_data[key] = "voice"
                    no_voiced_segments += 1
                else:
                    windowed_data[key] = "noise"

            audio_data = []
            for keys in windowed_data.keys():
                audio_data.append(DataPoint(start_time=keys[0], end_time=keys[1],
                                            offset=audio_stream[0].offset,
                                            sample=windowed_data[keys]))

            voiced_hourly = self.calc_voiced_segments_hourly(audio_data)
            file_path_voiced_hourly = "average_voiced_segments_hourly.json"
            self.store_data(file_path_voiced_hourly, input_streams_audio, user_id, voiced_hourly)

            voiced_daily = self.calc_voiced_segments_daily_average(audio_data, no_voiced_segments)

            file_path_voiced_daily = "average_voiced_segments_daily.json"
            self.store_data(file_path_voiced_daily, input_streams_audio, user_id, voiced_daily)
            if stream2_data:
                data_at_office = []
                office_start_time = stream2_data[0].start_time
                office_end_time = stream2_data[0].end_time
                for items in audio_data:
                    if (items.start_time >= office_start_time) and (items.end_time <= office_end_time):
                        data_at_office.append(items)

                voiced_hourly_at_work = self.calc_voiced_segments_hourly(data_at_office)
                file_path_hourly_at_work = "average_voiced_segments_office_hourly.json"
                self.store_data(file_path_hourly_at_work, input_streams_audio_work, user_id, voiced_hourly_at_work)

                voiced_daily_at_work = self.calc_voiced_segments_daily_average(data_at_office, no_voiced_segments)
                file_path_daily_at_work = "average_voiced_segments_office_daily.json"
                self.store_data(file_path_daily_at_work, input_streams_audio_work, user_id, voiced_daily_at_work)

    def store_data(self, file_path: str, input_streams: dict, user_id: str, data: list):
        """
        stores the computed data
        :param str file_path:path of metadata file
        :param dict input_streams:dict of input streams
        :param str user_id: id of user
        :param list data: output datapoint to be stored
        :return:
        """
        if data:
            try:
                self.store_stream(filepath=file_path, input_streams=input_streams,
                                  user_id=user_id, data=data, localtime=True)
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(str(traceback.format_exc()))
        else:
            self.CC.logging.log("stream not found for user %s" % str(user_id))

    def calc_voiced_segments_hourly(self, audio_data: list) -> List[DataPoint]:
        """
        calculates amout of voice_segments present every hour of a day.
        returns start_time, end_time and amount of voiced segments present
        every hour

        Algorithm:
            window(input audio_data) per hour
            Calculate voiced_segments_for_each_hour
            return voiced_segments_for_each hour


        :param audio_data:list of input data after thresholding
        :return:Datapoint with start_time,end_time and amount of voiced
        segments in minute
        :rtype:List(DataPoint)
        """
        windowed_per_hour = window(audio_data, 3600, False)

        voiced_per_hour = []
        for key in windowed_per_hour:
            no_voiced_segments_hr = 0
            for values in windowed_per_hour[key]:
                if values.sample == 'voice':
                    no_voiced_segments_hr += 1
            voiced_per_hour.append((DataPoint(start_time=key[0], end_time=key[1],
                                              offset=audio_data[0].offset,
                                              sample=no_voiced_segments_hr)))
        return voiced_per_hour

    def calc_voiced_segments_daily_average(self, audio_data: list,
                                           no_voiced_segments: int):
        """
        This function calculates daily average voiced segments. Out of total
        time for which data is present, calculates ratio of voiced segments
        to total audio segments.

        Algorithm:
            daily_average = no_of_voiced_segments/total_time

        :param audio_data: list of datapoints containing voiced or noise
        :param no_voiced_segments: out of total datapoint corresponding to
        one minute amount of voiced segments on a day in total.
        :return: returns float value indicating daily average voiced segments
        based on the total data collected for a day.
        :rtype: List(DataPoint)
        """

        total_time = len(audio_data)
        daily_average = no_voiced_segments / total_time

        voiced_segments_daily = [DataPoint(audio_data[0].start_time, audio_data[0].end_time,
                                           audio_data[0].offset, daily_average)]
        return voiced_segments_daily

    def process(self, user: str, all_days: List[str]):
        """Main entry point for a feature computation module

        Args:
            user: User id (UUID)
            all_days: What days (YYYYMMDD) to compute over

        """

        self.audio_stream = "CU_AUDIO_INFERENCE--edu.dartmouth.eureka"
        self.office_time = "org.md2k.data_analysis.feature.working_days"

        if self.CC is None:
            return

        if not user:
            return

        streams = self.CC.get_user_streams(user)
        if not len(streams):
            self.CC.logging.log('No streams found for user_id %s' % user)
            return

        if not len(streams):
            self.CC.logging.log('No streams found for user_id %s' % user)
            return

        for day in all_days:
            self.mark_audio_stream(streams, self.audio_stream, self.office_time, user, day)

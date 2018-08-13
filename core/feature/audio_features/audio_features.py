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
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from core.signalprocessing.window import window
from time import mktime
from collections import defaultdict
from typing import List

feature_class_name = 'AudioFeatures'


class AudioFeatures(ComputeFeatureBase):
    '''
    This class is based on audio_inference features that label data as voiced or
    noise. Based on whether the data sample is voice or noise, number of minutes
    of voice in an hour and average minutes on a day(based on amount of data pre
    sent)
    '''

    def get_day_data(self, stream_name: str, user_id, day):

        """
        :param stream_name: name fo the stream
        :param string user_id: UID of the user
        :param str day: retrieve the data for this day with format 'YYYYMMDD'
        :return: list of datapoints
        """

        day_data = []
        stream_ids = self.CC.get_stream_id(user_id, stream_name)
        for stream_id in stream_ids:
            data_stream = self.CC.get_stream(stream_id["identifier"],
                                             day=day,
                                             user_id=user_id,
                                             data_type=DataSet.COMPLETE,
                                             localtime=True)

            if data_stream is not None and len(data_stream.data) > 0:
                day_data.extend(data_stream.data)

        day_data.sort(key=lambda x: x.start_time)

        return day_data

    def mark_audio_stream(self, streams: dict, stream1_name: str,
                          stream2_name: str,
                          user_id: str, day: str):
        '''
        Fetches audio stream and office time stream to do calculations and
        assigns respective ids as input streams
        :param dict streams:Input dict of datapoints
        :param str stream1_name: name of stream
        :param str stream2_name: name of stream
        :param str user_id: id of user
        :param str day: day for which calculation is done
        '''
        input_streams_audio = []
        input_streams_audio_work = []
        if (stream1_name in streams) and (stream2_name not in streams):
            stream_id = streams[stream1_name]["identifier"]
            stream_name = streams[stream1_name]["name"]
            input_streams_audio.append({"identifier": stream_id,
                                        "name": stream_name})

            stream1 = self.CC.get_stream(stream_id, user_id=user_id, day=day,
                                         localtime=True)
            stream1_data = self.get_day_data(stream1_name, user_id, day)

            if len(stream1_data) > 0:
                self.audio_context(user_id, input_streams_audio, stream1_data,
                                   input_streams_audio)

        if (stream1_name in streams and stream2_name in streams):
            stream_id1 = streams[stream1_name]["identifier"]
            stream_name1 = streams[stream1_name]["name"]
            input_streams_audio_work.append({"identifier": stream_id1,
                                             "name": stream_name1})

            stream_id2 = streams[stream2_name]["identifier"]
            input_streams_audio_work.append({"identifier": stream_id2,
                                             "name": stream2_name})

            stream1 = self.CC.get_stream(stream_id1, user_id=user_id, day=day,
                                         localtime=True)
            stream2 = self.CC.get_stream(stream_id2, user_id=user_id, day=day,
                                         localtime=True)

            stream1_data = self.get_day_data(stream1_name, user_id, day)
            stream2_data = self.get_day_data(stream2_name, user_id, day)

            if len(stream1_data) > 0 and len(stream2_data) > 0:
                self.audio_context(user_id, input_streams_audio, stream1_data,
                                   input_streams_audio_work,
                                   stream2_data)

    def audio_context(self, user_id: str, input_streams_audio: dict,
                      stream1_data: list,
                      input_streams_audio_work: dict = None,
                      stream2_data: list = None):

        """
        redirects appropirate streams for appropriate calculations.
        takes raw input stream and for each voiced instance checks whether
        there is another voiced instance within 10 secs to merge it into a
        single voicing episode.
        Algorithm:
        for each voiced stream:
            if sample == 'voice' within 10 secs merge
        :param str user_id:id of user
        :param dict input_streams_audio: input stream for audio for whole day
        :param list stream1_data: list of Datapoints
        :param dict input_streams_audio_work:input stream for audio for
        office only
        :param list stream2_data: list of DataPoints
        """
        if (len(stream1_data) > 0):
            voiced_timestamps = []
            noise_timestamps = []
            inference_data = []
            for items in stream1_data:
                try:
                    if len(items.sample) == 2:
                        if items.sample[0] == 'voice':
                            voiced_timestamps.append(items)
                        if items.sample[0] == 'noise':
                            noise_timestamps.append(items)
                except:
                    self.CC.logging.log("Data is not audio_inference")

            conversation_data = self.conversation_episodes(voiced_timestamps)

            if conversation_data[0][0] > noise_timestamps[0].start_time:
                inference_data.append(
                    DataPoint(start_time=noise_timestamps[0].start_time,
                              end_time=conversation_data[0][0],
                              offset=stream1_data[0].offset,
                              sample=0))
            for idx, items in enumerate(conversation_data):
                if (idx) < len(conversation_data) - 1:

                    inference_data.append(
                        DataPoint(start_time=items[0], end_time=items[1],
                                  offset=stream1_data[0].offset,
                                  sample=1))
                    if (conversation_data[idx][1] != conversation_data[idx + 1][
                        0]):
                        inference_data.append(
                            DataPoint(start_time=conversation_data[idx][1],
                                      end_time=conversation_data[idx + 1][0],
                                      offset=stream1_data[0].offset, sample=0))

            if (conversation_data[-1][1] < noise_timestamps[-1].start_time):
                inference_data.append(
                    DataPoint(start_time=conversation_data[-1][1],
                              end_time=noise_timestamps[-1].start_time,
                              offset=stream1_data[0].offset, sample=0))

            file_path_voiced_segments = "voice_segments_context_daily.json"
            self.store_data(file_path_voiced_segments, input_streams_audio,
                            user_id, inference_data)
            voiced_hourly = self.calc_voiced_segments_hourly(inference_data)


            file_path_voiced_hourly = "average_voiced_segments_hourly.json"
            self.store_data(file_path_voiced_hourly,
                            input_streams_audio, user_id, voiced_hourly)

            voiced_daily = \
                self.calc_voiced_segments_daily_average(voiced_hourly)

            file_path_voiced_daily = "average_voiced_segments_daily.json"
            self.store_data(file_path_voiced_daily, input_streams_audio,
                                        user_id, voiced_daily)
            if stream2_data:
                data_at_office = []
                office_start_time = stream2_data[0].start_time
                office_end_time = stream2_data[0].end_time
                for items in inference_data:
                    if (items.start_time >= office_start_time) and \
                            (items.end_time <= office_end_time):
                        data_at_office.append(items)

                voiced_hourly_at_work = \
                    self.calc_voiced_segments_hourly(data_at_office)
                print(voiced_hourly_at_work)

                file_path_hourly_at_work = \
                    "average_voiced_segments_office_hourly.json"
                self.store_data(file_path_hourly_at_work,
                                input_streams_audio_work, user_id,
                                voiced_hourly_at_work)

                voiced_daily_at_work = \
                    self.calc_voiced_segments_daily_average(
                        voiced_hourly_at_work)
                print(voiced_daily_at_work)

                file_path_daily_at_work = \
                    "average_voiced_segments_office_daily.json"
                self.store_data(file_path_daily_at_work,
                                input_streams_audio_work
                                , user_id, voiced_daily_at_work)


    def conversation_episodes(self, data: list):

        conversation_values = []
        conversation_data = []
        conversation_start_time = data[0].start_time
        conversation_end_time = None

        for idx, items in enumerate(data):
            if (idx) < len(data) - 1:
                difference = ((data[idx + 1].start_time - data[
                    idx].start_time)).total_seconds()

                if difference <= 10:
                    conversation_end_time = data[idx + 1].start_time

                else:
                    conversation_data.append(
                        (conversation_start_time, conversation_end_time))
                    conversation_start_time = data[idx + 1].start_time
                    conversation_end_time = None

        for items in conversation_data:
            if items[1] != None:
                conversation_values.append(items)

        return (conversation_values)


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
            voiced_segments_hr = 0
            for values in windowed_per_hour[key]:
                if (values.sample == 1):
                    voiced_segments_hr += ((
                        values.end_time - values.start_time).total_seconds())/60
            voiced_per_hour.append(
                (DataPoint(start_time=key[0], end_time=key[1],
                           offset=audio_data[0].offset,
                           sample=voiced_segments_hr)))
        return voiced_per_hour



    def calc_voiced_segments_daily_average(self, voiced_hourly: list):

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

        total_time = 0
        total_voiced_time = 0
        if (len(voiced_hourly) > 0):
            for items in voiced_hourly:
                total_time += (
                    (items.end_time - items.start_time).total_seconds())
                total_voiced_time += (items.sample) * 60
            daily_average = total_voiced_time / total_time
            voiced_segments_daily = [DataPoint(voiced_hourly[0].start_time,
                                               voiced_hourly[0].end_time,
                                               voiced_hourly[0].offset
                                               , daily_average)]
        return voiced_segments_daily

    def store_data(self, file_path: str, input_streams: dict, user_id: str,
                   data: list):
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
                self.store_stream(filepath=file_path,
                                  input_streams=input_streams,
                                  user_id=user_id,
                                  data=data, localtime=True)
            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(str(traceback.format_exc()))
        else:
            self.CC.logging.log("stream not found for user %s" %
                                str(user_id))

    def process(self, user: str, all_days: list):
        """
        :param user: id of user
        :param all_days: list of days for calculations
        """

        self.audio_stream = "CU_AUDIO_INFERENCE--edu.dartmouth.eureka"
        self.office_time = "org.md2k.data_analysis.feature.working_days"

        if self.CC is None:
            return

        if not user:
            return

        streams = self.CC.get_user_streams(user)
        if not len(streams):
            self.CC.logging.log('No streams found for user_id %s' % (user))
            return

        if not len(streams):
            self.CC.logging.log('No streams found for user_id %s' % (user))
            return

        for day in all_days:
            self.mark_audio_stream(streams, self.audio_stream, self.office_time,
                                   user, day)

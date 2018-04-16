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
from cerebralcortex.core.util.data_types import DataPoint
from core.computefeature import ComputeFeatureBase
from core.signalprocessing.window import window
from core.signalprocessing.window import merge_consective_windows

feature_class_name = 'BeaconFeatures'


class BeaconFeatures(ComputeFeatureBase):
    """
    Categorizes beacon context as 0 or 1. 0: not around home beacon or around
    work beacon,1: around home beacon or work beacon
    """


    def mark_beacons(self, streams:dict, stream_name:str, user_id:str, day:str): 
        """
        fetches datastream for home beacons and calls home_beacon context
        :param dict streams : Input list
        :param str stream_name: name of stream
        :param str user_id: id of user
        :param str day: day 
      
        """
        if stream_name in streams:
            beacon_stream_id = streams[stream_name]["identifier"]
            beacon_stream_name = streams[stream_name]["name"]

            stream = self.CC.get_stream(
                beacon_stream_id, user_id=user_id, day=day,localtime = True)

            if (len(stream.data) > 0):
                if (stream_name ==
                        'BEACON--org.md2k.beacon--BEACON--HOME'):
                    self.home_beacon_context(
                        stream.data, beacon_stream_id, beacon_stream_name,
                        user_id)



    def merge_work_beacons(self, streams:dict, stream1_name:str, stream2_name:str,
                           user_id:str,  day:str):
        """
        merges datapoints of work1 and work2 beacons for a particular day
        if streams are from 1 and 2 both than 1 is taken as primary beacon
        :param dict streams : Input list
        :param str stream1_name: stream name representing workbeacon1
        :param str stream2_name: stream name representing workbeacon2
        :param str user_id: id of user
        :param str day: day 
       
        """
        new_data = []
        input_streams = []

        if stream1_name in streams:

            beacon_stream_id1 = streams[stream1_name]["identifier"]
            beacon_stream_name1 = streams[stream1_name]["name"]
            input_streams.append(
                {"identifier": beacon_stream_id1, "name": beacon_stream_name1})
            
            work1_stream = self.CC.get_stream(
                beacon_stream_id1, user_id=user_id, day=day,localtime = True)
            if (len(work1_stream.data) > 0):
                for items in work1_stream.data:
                    new_data.append(DataPoint(start_time=items.start_time,
                                              end_time=items.end_time,
                                              offset=items.offset, sample="1"))
        if stream2_name in streams:

            beacon_stream_id2 = streams[stream2_name]["identifier"]
            beacon_stream_name2 = streams[stream2_name]["name"]
            input_streams.append(
                {"identifier": beacon_stream_id2, "name": beacon_stream_name2})
            work2_stream = self.CC.get_stream(
                beacon_stream_id2, user_id=user_id, day=day,localtime = True)
            if (len(work2_stream.data) > 0):
                for items in work2_stream.data:
                    new_data.append(DataPoint(start_time=items.start_time,
                                              end_time=items.end_time,
                                              offset=items.offset, sample="2"))

        sorted_data = []
        sorted_data = sorted(new_data, key=lambda x: x.start_time)
        self.work_beacon_context(sorted_data, input_streams, user_id)



    def home_beacon_context(self, beaconhomestream:list, beacon_stream_id:str,
                            beacon_stream_name:str, user_id:str):
        """
        produces datapoint sample as 1 if around home beacon else 0
        
        Algorithm::
            data = window beaconstream 
            if values in a minute window in data
                around beacon:1
            else
                not around beacon:0

        :param List(Datapoint) beaconhomestream : Input list
        :param str beacon_stream_id: stream name representing workbeacon1
        :param str beacon_stream_name: stream name representing workbeacon2
        :param str user_id: id of user
       
        """
        input_streams = []
        input_streams.append(
            {"identifier": beacon_stream_id, "name": beacon_stream_name})
        
        if (len(beaconhomestream) > 0):
            beaconstream = beaconhomestream
            windowed_data = window(beaconstream, self.window_size, True)
            new_data = []

            for i, j in windowed_data:
                if (len(windowed_data[i, j]) > 0):
                    windowed_data[i, j] = 1

                else:
                    windowed_data[i, j] = 0

            data = merge_consective_windows(windowed_data)
            for items in data:
                if items.sample is not None and items.sample!="":
                    new_data.append(DataPoint(start_time=items.start_time,
                                              end_time=items.end_time,
                                              offset=beaconhomestream[0].offset,
                                              sample=items.sample))

            try:
                
                self.store_stream(filepath="home_beacon_context.json",
                                  input_streams= input_streams,
                                  user_id=user_id,
                                  data=new_data, localtime= True)
                self.CC.logging.log('%s %s home_beacon_context stored %d ' 
                                    'DataPoints for user %s ' 
                                    % (str(datetime.datetime.now()),
                                       self.__class__.__name__,
                                       len(new_data), str(new_data)))

            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(str(traceback.format_exc()))
        else:
            self.CC.logging.log("No home beacon streams found for user %s"%
                                str(user_id))




    def work_beacon_context(self, beaconworkstream:list, input_streams:dict, user_id:str):
        """
        produces datapoint sample as 1 if around work beacon 1, 2 if around workbeacon2
        and 0 if not around work beacon
        
         Algorithm::
            data = window beaconstream 
            if [values] in a minute window in data
                if 1 in values and 2 in values:
                    around work_beacon1 (1)
                else
                    around work_beacon(values[0]):could be either 1 or 2
            else
                not around beacon:0
        
        :param List(Datapoint) beaconworkstream : Input list
        :param dict input_streams: Dict to store stream id and name for storing
        :param string user_id: id of user
        
        """
        if (len(beaconworkstream) > 0):
            beaconstream = beaconworkstream

            windowed_data = window(beaconstream, self.window_size, True)

            new_data = []
            for i, j in windowed_data:
                if (len(windowed_data[i, j]) > 0):
                    values = []
                    for items in windowed_data[i, j]:
                        values.append(items.sample)

                    if ('1' in items.sample) & ('2' in items.sample):
                        windowed_data[i, j] = 1
                    else:
                        windowed_data[i, j] = int(values[0])

                else:
                    windowed_data[i, j] = 0

            data = merge_consective_windows(windowed_data)
            for items in data:
                if items.sample is not None and items.sample!="":
                    new_data.append(DataPoint(start_time=items.start_time,
                                              end_time=items.end_time,
                                              offset=beaconworkstream[0].offset,
                                              sample=items.sample))

            try:
                
                
                self.store_stream(filepath="work_beacon_context.json",
                                  input_streams= input_streams,
                                  user_id=user_id,
                                  data=new_data, localtime=True)
                self.CC.logging.log('%s %s work_beacon_context stored %d '
                                    'DataPoints for user %s ' 
                                    % (str(datetime.datetime.now()),
                                       self.__class__.__name__,
                                       len(new_data), str(new_data)))

            except Exception as e:
                self.CC.logging.log("Exception:", str(e))
                self.CC.logging.log(str(traceback.format_exc()))
        else:
            self.CC.logging.log("No work beacon streams found for user %s"%
                                 str(user_id))




    def process(self, user:str, all_days:list):
	
	"""
        lists requried streams needed for computation.
        :param str user_id: id of user
        :param List all_days: Input list of days
        """

        self.window_size = 60
        self.beacon_homestream = "BEACON--org.md2k.beacon--BEACON--HOME"
        self.beacon_workstream1 = "BEACON--org.md2k.beacon--BEACON--WORK 1"
        self.beacon_workstream2 = "BEACON--org.md2k.beacon--BEACON--WORK 2"

        if self.CC is None:
            return

        if not user:
            return

        streams = self.CC.get_user_streams(user)
        if not len(streams):
            self.CC.logging.log('No streams found for user_id %s' % (user))
            return

        for day in all_days:
            self.CC.logging.log('%s %s started processing for user %s day %s' 
                                 % (str(datetime.datetime.now()),
                                    self.__class__.__name__,
                                    str(user), 
                                    str(day)))

            self.mark_beacons(streams, self.beacon_homestream, user, day)
            self.merge_work_beacons(streams, self.beacon_workstream1,
                                    self.beacon_workstream2, user, day)

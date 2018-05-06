# Copyright (c) 2018, MD2K Center of Excellence
# - Vincent Tseng <wt262@cornell.edu>
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

from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint
from core.computefeature import ComputeFeatureBase

import datetime
import numpy as np

from typing import List, Callable, Any

feature_class_name = 'Cyberslacking'

stream_names = {
  'sleep': "org.md2k.data_analysis.feature.sleep",
  'app_usage_category': "org.md2k.data_analysis.feature.phone.app_usage_category",
}



class Cyberslacking(ComputeFeatureBase):
  def process(self, user_id: str, all_days: List[str]):
    """
    Main processing function inherited from ComputerFeatureBase
    :param str user_id: UUID of the user
    :param List(str) all_days: List of days with format 'YYYYMMDD'
    :return:
    """
    if self.CC is not None:
      streams = self.CC.get_user_streams(user_id)
      self.CC.logging.log("Processing Cyberslacking")
      self.process_cyberslacking(user_id, all_days)


  def process_cyberslacking(self, user_id: str, all_days: List[str]):
    """
    Args:
      all_days: List of all days for the processing in the format 'YYYYMMDD'.

    Returns:
    """
  
    user_streams = self.CC.get_user_streams(user_id)
    
    for stream_name, stream_metadata in user_streams.items():
      if stream_name == stream_names['sleep']:
        self.stream_metadata_sleep = stream_metadata
      elif stream_name == stream_names['app_usage_category']:
        self.stream_metadata_app_usage_category = stream_metadata

    for day in all_days:
      self.process_volume(user_id, day, query_app_type='all_apps')
      self.process_volume(user_id, day, query_app_type='social')
      self.process_volume(user_id, day, query_app_type='communication')
      self.process_burstiness(user_id, day, query_app_type='all_apps')
      self.process_burstiness(user_id, day, query_app_type='social')
      self.process_burstiness(user_id, day, query_app_type='communication')
      self.process_interval(user_id, day, query_app_type='all_apps')
      self.process_interval(user_id, day, query_app_type='social')
      self.process_interval(user_id, day, query_app_type='communication')


  def process_volume(self, user_id: str, start_time: str, end_time: str = None, query_app_type='all_apps'):
    """ The total number of technolgoy-mediated social interactions beteen wake and sleep.
    Args:
      start_time: day for which volume is computed; in the format 'YYYYMMDD'.

    Returns:
      interactions: A dictionary of interactions counts for social and communication apps. The values are 'None' if there is no valid wake and sleep time.
    """

    start_time = datetime.datetime.strptime(start_time, '%Y%m%d')

    if end_time is None:
      end_time = start_time + datetime.timedelta(days=1)
    else:
      end_time = datetime.datetime.strptime(end_time, '%Y%m%d')


    stream_name_sleep = stream_names['sleep'] 
    stream_id_sleep = self.CC.get_stream_id(user_id=user_id, stream_name=stream_name_sleep)

    volume_data_points = list()

    query_day = start_time

    while query_day < end_time: 
      wake_time = None
      sleep_time = None
      offset = None
      # Number of interactions with social and communication apps
      interactions = {'social': None, 'communication': None}

      # Check all the data streams for wake-up
      data_stream_wake = None
      for stream_id in stream_id_sleep:
        data_stream = self.CC.get_stream(user_id=user_id, stream_id=stream_id['identifier'], day=(query_day+datetime.timedelta(days=-1)).strftime("%Y%m%d"), localtime=True)
        if data_stream_wake is None:
           data_stream_wake = data_stream
        else:
          if len(data_stream.data) > 0 and len(data_stream_wake.data) > 0 and data_stream.data[0]._sample[0] > data_stream_wake.data[0]._sample[0]:
            wake_data_stream = data_stream
      if len(data_stream_wake.data) > 0:
        wake_time = data_stream_wake.data[0]._sample[2]

      # Check all the data streams for sleep
      data_stream_sleep = None
      for stream_id in stream_id_sleep:
        data_stream = self.CC.get_stream(user_id=user_id, stream_id=stream_id['identifier'], day=query_day.strftime("%Y%m%d"), localtime=True)
        if data_stream_sleep is None:
           data_stream_sleep = data_stream
        else:
          if len(data_stream.data) > 0 and len(data_stream_sleep.data) > 0 and data_stream.data[0]._sample[0] > data_stream_sleep.data[0]._sample[0]:
            data_stream_sleep = data_stream
      if len(data_stream_sleep.data) > 0:
        sleep_time = data_stream_sleep.data[0].sample[1]
        offset = data_stream_sleep.data[0].offset

    
      if wake_time != None and sleep_time != None and sleep_time > wake_time:
        stream_name_app_usage_category = stream_names['app_usage_category']
        stream_id_app_usage_category = self.CC.get_stream_id(user_id=user_id, stream_name=stream_name_app_usage_category)

        for key, value in interactions.items():
          interactions[key] = 0

        # Aggregate stream data from multiple stream sources
        stream_data_app_usage_category = list()
        for stream_id in stream_id_app_usage_category:
          stream_data_app_usage_category += self.CC.get_stream(user_id=user_id, stream_id=stream_id_app_usage_category[0]['identifier'], day=query_day.strftime("%Y%m%d"), localtime=True).data
        if len(stream_data_app_usage_category) > 0:
          stream_data_app_usage_category.sort(key=lambda x: x._start_time)

        for datapoint in stream_data_app_usage_category:	
          if wake_time <= datapoint._start_time <= sleep_time:
            app_category = datapoint.sample[1]
            if app_category != None and app_category.lower() in interactions:
              interactions[app_category.lower()] += 1

      if query_app_type == 'all_apps':
        if interactions['social'] is not None and interactions['communication'] is not None:
          volume_data_points.append(DataPoint(start_time=wake_time, end_time=sleep_time, offset=offset, sample=[interactions['social'] + interactions['communication']]))
        elif interactions['social'] is not None:
          volume_data_points.append(DataPoint(start_time=wake_time, end_time=sleep_time, offset=offset, sample=[interactions['social']]))
        elif interactions['communication'] is not None:
          volume_data_points.append(DataPoint(start_time=wake_time, end_time=sleep_time, offset=offset, sample=[interactions['communication']]))
        #else:
        #  volume_data_points.append(DataPoint(start_time=wake_time, end_time=sleep_time, offset=offset, sample=[None]))
        
      elif query_app_type == 'social' and interactions[query_app_type] is not None:
        volume_data_points.append(DataPoint(start_time=wake_time, end_time=sleep_time, offset=offset, sample=[interactions['social']]))
      elif query_app_type == 'communication' and interactions[query_app_type] is not None:
        volume_data_points.append(DataPoint(start_time=wake_time, end_time=sleep_time, offset=offset, sample=[interactions['communication']]))

      query_day += datetime.timedelta(days=1)

    
    
    if len(volume_data_points) > 0:
      print(volume_data_points)
      self.store_stream(filepath= "cyberslacking_volume_%s.json" %query_app_type,
                        input_streams=[self.stream_metadata_sleep, self.stream_metadata_app_usage_category],
                        user_id=user_id, data=volume_data_points, localtime=True)



  def process_burstiness(self, user_id, start_time, end_time=None, query_app_type='all_apps'):
    """ The maximum number of interactions in any single hour between wake and sleep.
    Args:

    Returns:
    """
    start_time = datetime.datetime.strptime(start_time, '%Y%m%d')

    if end_time is None:
      end_time = start_time + datetime.timedelta(days=1)
    else:
      end_time = datetime.datetime.strptime(end_time, '%Y%m%d')

    stream_name_sleep = stream_names['sleep'] 
    stream_id_sleep = self.CC.get_stream_id(user_id=user_id, stream_name=stream_name_sleep)

    burstiness_data_points = list()

    query_time = start_time

    while query_time < end_time:
      wake_time = None
      sleep_time = None
      offset = None
      # Number of interactions with social and communication apps in each hour
      hourly_interactions = {'social': {}, 'communication': {}}

      # Check all the data streams for wake-up
      data_stream_wake = None
      for stream_id in stream_id_sleep:
        data_stream = self.CC.get_stream(user_id=user_id, stream_id=stream_id['identifier'], day=(query_time+datetime.timedelta(days=-1)).strftime("%Y%m%d"), localtime=True)
        if data_stream_wake is None:
           data_stream_wake = data_stream
        else:
          if len(data_stream.data) > 0 and len(data_stream_wake.data) > 0 and data_stream.data[0]._sample[0] > data_stream_wake.data[0]._sample[0]:
            wake_data_stream = data_stream
      if len(data_stream_wake.data) > 0:
        wake_time = data_stream_wake.data[0]._sample[2]

      # Check all the data streams for sleep
      data_stream_sleep = None
      for stream_id in stream_id_sleep:
        data_stream = self.CC.get_stream(user_id=user_id, stream_id=stream_id['identifier'], day=query_time.strftime("%Y%m%d"), localtime=True)
        if data_stream_sleep is None:
           data_stream_sleep = data_stream
        else:
          if len(data_stream.data) > 0 and len(data_stream_sleep.data) > 0 and data_stream.data[0]._sample[0] > data_stream_sleep.data[0]._sample[0]:
            data_stream_sleep = data_stream
      if len(data_stream_sleep.data) > 0:
        sleep_time = data_stream_sleep.data[0].sample[1]
        offset = data_stream_sleep.data[0].offset

      if wake_time != None and sleep_time != None and sleep_time > wake_time:
        stream_name_app_usage_category = stream_names['app_usage_category']
        stream_id_app_usage_category = self.CC.get_stream_id(user_id=user_id, stream_name=stream_name_app_usage_category)

        data_stream_app_usage_category = self.CC.get_stream(user_id=user_id, stream_id=stream_id_app_usage_category[0]['identifier'], day=query_time.strftime("%Y%m%d"), localtime=True)

        # Aggregate stream data from multiple stream sources
        stream_data_app_usage_category = list()
        for stream_id in stream_id_app_usage_category:
          stream_data_app_usage_category += self.CC.get_stream(user_id=user_id, stream_id=stream_id_app_usage_category[0]['identifier'], day=query_time.strftime("%Y%m%d"), localtime=True).data
        if len(stream_data_app_usage_category) > 0:
          stream_data_app_usage_category.sort(key=lambda x: x._start_time)

        for datapoint in stream_data_app_usage_category:	
          if wake_time <= datapoint._start_time <= sleep_time:
            app_category = datapoint.sample[1]
            if app_category != None and app_category.lower() in hourly_interactions:
              app_category = app_category.lower()
              # Hours elapsed after wake-up
              hour = int((datapoint._start_time - wake_time).total_seconds()/3600)
              if hour in hourly_interactions[app_category]:
                hourly_interactions[app_category][hour] += 1
              else:
                hourly_interactions[app_category][hour] = 1
   
      # Maximum number of interactions between wake and sleep
      max_hourly_interactions = dict()
      max_hourly_interaction = None
      for k, v in hourly_interactions.items():
        if len(v.values()) > 0:
          max_hourly_interactions[k] = max(v.values())
        else:
          max_hourly_interactions[k] = None

      if query_app_type == 'all_apps':
        combined_hourly_interactions = dict()
        # 24 hours
        for h in range(24):
          combined_hourly_interactions[h] = 0
          for app in hourly_interactions.keys():
            if h in hourly_interactions[app]:
              combined_hourly_interactions[h] += hourly_interactions[app][h]
        if max(combined_hourly_interactions.values()) > 0:
          max_hourly_interaction = max(combined_hourly_interactions.values())      

      elif query_app_type == 'social' or query_app_type == 'communication':
        if len(hourly_interactions[query_app_type]) > 0:
          max_hourly_interaction = max(hourly_interactions[query_app_type].values())

      if max_hourly_interaction is not None:
        burstiness_data_points.append(DataPoint(start_time=wake_time, end_time=sleep_time, offset=offset, sample=[max_hourly_interaction]))

      query_time += datetime.timedelta(days=1)

    if len(burstiness_data_points) > 0:
      print(burstiness_data_points)
      self.store_stream(filepath= "cyberslacking_burstiness_%s.json" %query_app_type,
                      input_streams=[self.stream_metadata_sleep, self.stream_metadata_app_usage_category],
                      user_id=user_id, data=burstiness_data_points, localtime=True)

  

  def process_interval(self, user_id, start_time, end_time=None, query_app_type='all_apps'):
    """ The minimum, maximum and average amount of time that passes between each successive interactions between wake and sleep.
    Args:

    Returns:
    """
    start_time = datetime.datetime.strptime(start_time, '%Y%m%d')

    if end_time is None:
      end_time = start_time + datetime.timedelta(days=1)
    else:
      end_time = datetime.datetime.strptime(end_time, '%Y%m%d')

    stream_name_sleep = stream_names['sleep'] 
    stream_id_sleep = self.CC.get_stream_id(user_id=user_id, stream_name=stream_name_sleep)

    interval_data_points = list()

    query_time = start_time

    while query_time < end_time:
      wake_time = None
      sleep_time = None
      offset = None
      # The time of each interaction.
      interaction_logs = {'social': [], 'communication': []}

      # Check all the data streams for wake-up
      data_stream_wake = None
      for stream_id in stream_id_sleep:
        data_stream = self.CC.get_stream(user_id=user_id, stream_id=stream_id['identifier'], day=(query_time+datetime.timedelta(days=-1)).strftime("%Y%m%d"), localtime=True)
        if data_stream_wake is None:
           data_stream_wake = data_stream
        else:
          if len(data_stream.data) > 0 and len(data_stream_wake.data) > 0 and data_stream.data[0]._sample[0] > data_stream_wake.data[0]._sample[0]:
            wake_data_stream = data_stream
      if len(data_stream_wake.data) > 0:
        wake_time = data_stream_wake.data[0]._sample[2]

      # Check all the data streams for sleep
      data_stream_sleep = None
      for stream_id in stream_id_sleep:
        data_stream = self.CC.get_stream(user_id=user_id, stream_id=stream_id['identifier'], day=query_time.strftime("%Y%m%d"), localtime=True)
        if data_stream_sleep is None:
           data_stream_sleep = data_stream
        else:
          if len(data_stream.data) > 0 and len(data_stream_sleep.data) > 0 and data_stream.data[0]._sample[0] > data_stream_sleep.data[0]._sample[0]:
            data_stream_sleep = data_stream
      if len(data_stream_sleep.data) > 0:
        sleep_time = data_stream_sleep.data[0].sample[1]
        offset = data_stream_sleep.data[0].offset

      if wake_time != None and sleep_time != None and sleep_time > wake_time:
        stream_name_app_usage_category = stream_names['app_usage_category']
        stream_id_app_usage_category = self.CC.get_stream_id(user_id=user_id, stream_name=stream_name_app_usage_category)

        data_stream_app_usage_category = self.CC.get_stream(user_id=user_id, stream_id=stream_id_app_usage_category[0]['identifier'], day=query_time.strftime("%Y%m%d"), localtime=True)

        # Aggregate stream data from multiple stream sources
        stream_data_app_usage_category = list()
        for stream_id in stream_id_app_usage_category:
          stream_data_app_usage_category += self.CC.get_stream(user_id=user_id, stream_id=stream_id_app_usage_category[0]['identifier'], day=query_time.strftime("%Y%m%d"), localtime=True).data
        if len(stream_data_app_usage_category) > 0:
          stream_data_app_usage_category.sort(key=lambda x: x._start_time)

        for datapoint in stream_data_app_usage_category:	
          if wake_time <= datapoint._start_time <= sleep_time:
            app_category = datapoint.sample[1]
            if app_category != None and app_category.lower() in interaction_logs:
              app_category = app_category.lower()
              interaction_logs[app_category].append(datapoint._start_time)

      query_time += datetime.timedelta(days=1)

      interaction_intervals = dict()
      for k,v in interaction_logs.items():
        intervals = [(v[i]-v[i-1]).total_seconds() for i in range(1, len(v))]
        intervals = [x for x in intervals if x != 0]
        interaction_intervals[k] = intervals

      if query_app_type == 'all_apps':
        combined_intervals = interaction_intervals['social'] + interaction_intervals['communication']
        if len(combined_intervals) > 0:
          interval_data_points.append(DataPoint(start_time=wake_time, 
                                                end_time=sleep_time, 
                                                offset=offset,
                                                sample=[np.mean(combined_intervals), np.std(combined_intervals), np.nanmin(combined_intervals), np.nanmax(combined_intervals)]))

      elif query_app_type == 'social' or query_app_type == 'communication':
        if len(interaction_intervals[query_app_type]) > 0:
          interval_data_points.append(DataPoint(start_time=wake_time, 
                                                end_time=sleep_time,
                                                offset=offset, 
                                                sample=[np.mean(interaction_intervals[query_app_type]), np.std(interaction_intervals[query_app_type]), np.nanmin(interaction_intervals[query_app_type]), np.nanmax(interaction_intervals[query_app_type])]))

      query_time += datetime.timedelta(days=1)

    if len(interval_data_points) > 0:
      print(interval_data_points)
      self.store_stream(filepath= "cyberslacking_interval_%s.json" %query_app_type,
                      input_streams=[self.stream_metadata_sleep, self.stream_metadata_app_usage_category],
                      user_id=user_id, data=interval_data_points, localtime=True)




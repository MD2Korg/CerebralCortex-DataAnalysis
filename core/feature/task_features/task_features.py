# Copyright (c) 2018, MD2K Center of Excellence
# -Mithun Saha <msaha1@memphis.edu>
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

from core.feature.task_features.utils import *
from core.computefeature import ComputeFeatureBase
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet

feature_class_name = 'TaskFeatures'


class TaskFeatures(ComputeFeatureBase):
    """
    Detects start and end sessions of postures like sitting and standing
    in office context and around office beacon context in a day. Also detects
    activity sessions like walking against the same office and office beacon
    context for the same day. Computes fraction of posture and activity time against
    total office and office beacon context time and converts them to minutes per
    hour.

    Algorithm::

        step 1: For each user collects stream data of all stream ids of one day
        step 2: Checks and removes duplicate data points
        step 3: Converts stream data of one day into dictionary of lists of
                start and end times for each stream. The keys of interest in the
                dictionaries are sitting,standing,walking,work and 1.
        step 4: Finds overlapping start and end times of sitting,standing,walking in
                work and around office beacon context. Converts each such finding
                into a DataPoint.Sums difference of these start and end times to find
                total time spent in sitting,standing,walking in work in office and around
                office beacon context.
        step 5: Stores each such DataPoint.
        step 6: Finds fraction of time spent in sitting,standing,walking in mintues
                per hour in comaparison with total time sent in work in office and
                around office beacon in a day.Converts each such finding in a DataPoint.
        Step 7: Stores each such datapoint

    """

    def get_day_data(self, stream_name: str, user_id: str, day: str)->List[DataPoint]:
        """
        This function collects participant data of all stream ids for a day

        :param str stream_name: Name of the stream
        :param str user_id: UUID of the stream owner
        :param str day: The day (YYYYMMDD) on which to operate
        :return: Combined stream data if there are multiple stream id
        :rtype: List(DataPoint)
        """

        day_data = []
        stream_ids = self.CC.get_stream_id(user_id, stream_name)
        for stream_id in stream_ids:
            data_stream = self.CC.get_stream(stream_id["identifier"],user_id,
                                             day,localtime=True)

            if data_stream is not None and len(data_stream.data) > 0:
                day_data.extend(data_stream.data)

        day_data.sort(key=lambda x: x.start_time)

        return day_data

    def process(self, user: str, all_days: list):
        """
        This function collects user data of individual streams like posture,
        activity, gps locations and office beacon. Creates dictionaries of time intervals
        for postures like sitting, standing; activity like walking; gps semantic
        location like office; beacon type like office. Then finds overlapping time
        intervals of sitting,standing and walking in office and office beacon context.
        Finally calculates the proportion of sitting,standing,walking time in
        office and office beacon context and converts them to minutes per hour.

        :param str user: UUID
        :param list all_days: list of days on which to operate

        """

        if self.CC is None:
            return

        if not user:
            return

        streams = self.CC.get_user_streams(user)

        if not streams:
            self.CC.logging.log(
                "Task features - no streams found for user: %s" %
                (user))
            return

        for day in all_days:
            posture_with_time = {}
            activity_with_time = {}
            office_with_time = {}
            beacon_with_time = {}
            unique_data_set = set()
            offset = 0

            sitting_office_interval = []
            standing_office_interval = []

            sitting_officebeacon_interval = []
            standing_officebeacon_interval = []

            sitting_office_time = []
            standing_office_time = []

            sitting_officebeacon_time = []
            standing_officebeacon_time = []

            # gets all posture datapoints
            get_all_data = self.get_day_data(posture_stream_name, user, day)
            if len(get_all_data) != 0: #creates a dictionary of start,end times
                unique_data_set = set(get_all_data)#removes duplicate datapoints
                get_all_data = list(unique_data_set)
                posture_with_time = process_data(get_all_data)
                offset = get_all_data[0].offset

            # gets all activity datapoints
            get_all_data = self.get_day_data(activity_stream_name, user, day)
            if len(get_all_data) != 0: #creates a dictionary of start,end times
                unique_data_set = set(get_all_data)#removes duplicate datapoints
                get_all_data = list(unique_data_set)
                activity_with_time = process_data(get_all_data)
                offset = get_all_data[0].offset

            # gets all office datapoints
            get_all_data = self.get_day_data(office_stream_name, user, day)
            if len(get_all_data) != 0: #creates a dictionary of start,end times
                unique_data_set = set(get_all_data)
                get_all_data = list(unique_data_set)
                office_with_time = process_data(get_all_data)

            # gets all beacon datapoints
            get_all_data = self.get_day_data(beacon_stream_name, user, day)
            if len(get_all_data) != 0:#creates a dictionary of start,end times
                unique_data_set = set(get_all_data)
                get_all_data = list(unique_data_set)
                beacon_with_time = process_data(get_all_data)

            target_total_time, posture_office = output_stream(posture_with_time,
                                                              office_with_time,
                                                              offset)
            if len(posture_office)> 0:
                posture_office.sort(key = lambda x: x.start_time)

                # newly added block
                for v in posture_office:
                    if v.sample[0] == 'sitting':
                        sitting_office_interval.append(v)
                    elif v.sample[0] == 'standing':
                        standing_office_interval.append(v)

                self.store_stream(filepath='sitting_interval_office_context_daily.json',
                                  input_streams=[
                                      streams[posture_stream_name],
                                      streams[office_stream_name]],
                                  user_id=user,
                                  data=sitting_office_interval,localtime=True)

                self.store_stream(filepath='standing_interval_office_context_daily.json',
                                  input_streams=[
                                      streams[posture_stream_name],
                                      streams[office_stream_name]],
                                  user_id=user,
                                  data=standing_office_interval,localtime=True)


                posture_office_fraction = target_in_fraction_of_context(
                    target_total_time,
                    office_with_time, offset, 'work')

                for v in posture_office_fraction:
                    if v.sample[0] == 'sitting':
                        sitting_office_time.append(v)
                    elif v.sample[0] == 'standing':
                        standing_office_time.append(v)

                self.store_stream(
                    filepath='sitting_office_context_totaltime_and_fraction_per_hour.json',
                    input_streams=[
                        streams[posture_stream_name],
                        streams[office_stream_name]],
                    user_id=user,
                    data=sitting_office_time,localtime=True)

                self.store_stream(
                    filepath='standing_office_context_totaltime_and_fraction_per_hour.json',
                    input_streams=[
                        streams[posture_stream_name],
                        streams[office_stream_name]],
                    user_id=user,
                    data=standing_office_time,localtime=True)


            target_total_time, activity_office = output_stream(
                activity_with_time,
                office_with_time, offset)

            if len(activity_office)> 0:
                activity_office.sort(key = lambda x: x.start_time)

                self.store_stream(filepath='walking_interval_office_context_daily.json',
                                  input_streams=[
                                      streams[activity_stream_name],
                                      streams[office_stream_name]],
                                  user_id=user,
                                  data=activity_office,localtime=True)

                activity_office_fraction = target_in_fraction_of_context(
                    target_total_time, office_with_time,
                    offset, 'work')

                self.store_stream(
                    filepath='walking_office_context_totaltime_and_fraction_per_hour.json',
                    input_streams=[
                        streams[activity_stream_name],
                        streams[office_stream_name]],
                    user_id=user,
                    data=activity_office_fraction,localtime=True)


            target_total_time, posture_beacon = output_stream(posture_with_time,
                                                              beacon_with_time,
                                                              offset)
            if len(posture_beacon)> 0:
                posture_beacon.sort(key = lambda x: x.start_time)

                for v in posture_beacon:
                    if v.sample[0] == 'sitting':
                        sitting_officebeacon_interval.append(v)
                    elif v.sample[0] == 'standing':
                        standing_officebeacon_interval.append(v)

                self.store_stream(filepath='sitting_interval_officebeacon_context_daily.json',
                                  input_streams=[
                                      streams[posture_stream_name],
                                      streams[beacon_stream_name]],
                                  user_id=user,
                                  data=sitting_officebeacon_interval,localtime=True)

                self.store_stream(filepath='standing_interval_officebeacon_context_daily.json',
                                  input_streams=[
                                      streams[posture_stream_name],
                                      streams[beacon_stream_name]],
                                  user_id=user,
                                  data=standing_officebeacon_interval,localtime=True)


                posture_beacon_fraction = target_in_fraction_of_context(
                    target_total_time, beacon_with_time,
                    offset, '1')

                for v in posture_beacon_fraction:
                    if v.sample[0] == 'sitting':
                        sitting_officebeacon_time.append(v)
                    elif v.sample[0] == 'standing':
                        standing_officebeacon_time.append(v)

                self.store_stream(
                    filepath='sitting_officebeacon_context_totaltime_and_fraction_per_hour.json',
                    input_streams=[
                        streams[posture_stream_name],
                        streams[beacon_stream_name]],
                    user_id=user,
                    data=sitting_officebeacon_time,localtime=True)

                self.store_stream(
                    filepath='standing_officebeacon_context_totaltime_and_fraction_per_hour.json',
                    input_streams=[
                        streams[posture_stream_name],
                        streams[beacon_stream_name]],
                    user_id=user,
                    data=standing_officebeacon_time,localtime=True)


            target_total_time, activity_beacon = output_stream(
                activity_with_time,
                beacon_with_time, offset)

            if len(activity_beacon)> 0:
                activity_beacon.sort(key = lambda x: x.start_time)

                self.store_stream(filepath='walking_interval_officebeacon_context_daily.json',
                                  input_streams=[
                                      streams[activity_stream_name],
                                      streams[beacon_stream_name]],
                                  user_id=user,
                                  data=activity_beacon,localtime=True)

                activity_beacon_fraction = target_in_fraction_of_context(
                    target_total_time, beacon_with_time,
                    offset, '1')
                print(activity_beacon_fraction)

                self.store_stream(
                    filepath='walking_officebeacon_context_totaltime_and_fraction_per_hour.json',
                    input_streams=[
                        streams[activity_stream_name],
                        streams[beacon_stream_name]],
                    user_id=user,
                    data=activity_beacon_fraction,localtime=True)

        self.CC.logging.log(
            "Finished processing Task features for user: %s" % (user))

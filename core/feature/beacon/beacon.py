import os
import json
import uuid
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


    def mark_beacons(self, streams, stream_name, user_id, day):
        """
        fetches datastream for home beacons
        :param streams:
        :param stream_name:
        :param user_id:
        :param day:
        :return:
        """
        if stream_name in streams:
            beacon_stream_id = streams[stream_name]["identifier"]
            beacon_stream_name = streams[stream_name]["name"]

            stream = self.CC.get_stream(
                beacon_stream_id, user_id=user_id, day=day,localtime = False)

            if (len(stream.data) > 0):
                if (stream_name ==
                        'BEACON--org.md2k.beacon--BEACON--HOME'):
                    self.home_beacon_context(
                        stream.data, beacon_stream_id, beacon_stream_name,
                        user_id)



    def merge_work_beacons(self, streams, stream1_name, stream2_name,
                           user_id,  day):
        """
        merges datapoints of work1 and work2 beacons for a particular day
        :param streams:
        :param stream1_name: workbeacon1
        :param stream2_name: workbeacon2
        :param user_id:
        :param day:
        :return: new data stream merging work1 and work2
        """

        input_streams = []

        if stream1_name in streams:

            beacon_stream_id1 = streams[stream1_name]["identifier"]
            beacon_stream_name1 = streams[stream1_name]["name"]
            input_streams.append(
                {"identifier": beacon_stream_id1, "name": beacon_stream_name1})
            work1_stream = self.CC.get_stream(
                beacon_stream_id1, user_id=user_id, day=day,localtime = False)
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
                beacon_stream_id2, user_id=user_id, day=day,localtime = False)
            if (len(work2_stream.data) > 0):
                for items in work2_stream.data:
                    new_data.append(DataPoint(start_time=items.start_time,
                                              end_time=items.end_time,
                                              offset=items.offset, sample="2"))

        sorted_data = []
        sorted_data = sorted(new_data, key=lambda x: x.start_time)
        self.work_beacon_context(sorted_data, input_streams, user_id)



    def home_beacon_context(self, beaconhomestream, beacon_stream_id,
                            beacon_stream_name, user_id):
        """
        produces datapoint sample as 1 if around home beacon else 0
        :param beaconhomestream:
        :param beacon_stream_id:
        :param beacon_stream_name:
        :param user_id:
        :return: new stream (start_time,end_time,offset,sample=[0 or 1]
        """
        input_streams = []
        new_data = []
        input_streams.append(
            {"identifier": beacon_stream_id, "name": beacon_stream_name})
        if (len(beaconhomestream) > 0):
            beaconstream = beaconhomestream
            windowed_data = window(beaconstream, self.window_size, True)


            for i, j in windowed_data:
                if (len(windowed_data[i, j]) > 0):
                    windowed_data[i, j] = "1"

                else:
                    windowed_data[i, j] = "0"

            data = merge_consective_windows(windowed_data)
            for items in data:
                new_data.append(DataPoint(start_time=items.start_time,
                                          end_time=items.end_time,
                                          offset=beaconhomestream[0].offset,
                                          sample=items.sample))

        try:

            self.store_stream(filepath="home_beacon_context.json",
                              input_streams= input_streams,
                              user_id=user,
                              data=new_data)

        except Exception as e:
            self.CC.logging.log("Exception:", str(e))




    def work_beacon_context(self, beaconworkstream, input_streams, user_id):

        """
        produces datapoint sample as 1 or 2 if around work beacons else 0
        :param beaconworkstream:
        :param input_streams:
        :param user_id:
        :return: stream with (start_time,end_time,offset,sample= 0 or 1]
        based on context of work_beacon 1 or work_beacon 2
        """
        new_data = []
        if (len(beaconworkstream) > 0):
            beaconstream = beaconworkstream

            windowed_data = window(beaconstream, self.window_size, True)


            for i, j in windowed_data:
                if (len(windowed_data[i, j]) > 0):
                    values = []
                    for items in windowed_data[i, j]:
                        values.append(items.sample)

                    if ('1' in items.sample) & ('2' in items.sample):
                        windowed_data[i, j] = "1"
                    else:
                        windowed_data[i, j] = values[0]

                else:
                    windowed_data[i, j] = "0"


            data = merge_consective_windows(windowed_data)
            for items in data:
                new_data.append(DataPoint(start_time=items.start_time,
                                          end_time=items.end_time,
                                          offset=beaconworkstream[0].offset,
                                          sample=items.sample))

        try:

            self.store_stream(filepath="work_beacon_context.json",
                              input_streams= input_streams,
                              user_id=user,
                              data=new_data)

        except Exception as e:
            self.CC.logging.log("Exception:", str(e))



    def process(self, user, all_days):

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
            self.mark_beacons(streams, self.beacon_homestream, user, day)
            self.merge_work_beacons(streams, self.beacon_workstream1,
                                    self.beacon_workstream2, user, day)

import os
import json
import uuid
from cerebralcortex.core.util.data_types import DataPoint
from core.computefeature import ComputeFeatureBase
from core.signalprocessing.window import window
# FIXME - line length 80
# FIXME - use google python style
feature_class_name = 'BeaconFeatures'

class BeaconFeatures(ComputeFeatureBase):
    # FIXME document
    def __init__(self):
        self.window_size = 60
        self.beacon_homestream = "BEACON--org.md2k.beacon--BEACON--HOME"
        self.beacon_workstream = "BEACON--org.md2k.beacon--BEACON--WORK 1"  # TODO: add workplace 2

    def mark_beacons(self, streams, stream_name, user_id, day):
        # FIXME document
        if stream_name in streams:
            beacon_stream_id = streams[stream_name]["identifier"]
            beacon_stream_name = streams[stream_name]["name"]

            stream = self.CC.get_stream(beacon_stream_id, user_id=user_id, day=day)

            if (len(stream.data) > 0):
                if (stream_name ==
                    'BEACON--org.md2k.beacon--BEACON--HOME'):#FIXME
                    self.home_beacon_context(
                        stream.data, beacon_stream_id, beacon_stream_name, user_id)
                else:
                    self.work_beacon_context(
                        stream.data, beacon_stream_id, beacon_stream_name, user_id)

    def home_beacon_context(self, beaconhomestream, beacon_stream_id, beacon_stream_name, user_id):
        # FIXME document
        input_streams = []
        input_streams.append({"identifier": beacon_stream_id, "name": beacon_stream_name})
        if (len(beaconhomestream) > 0):
            beaconstream = beaconhomestream
            windowed_data = window.window(beaconstream, self.window_size, True)
        new_data = []
        for i, j in windowed_data:
            if (len(windowed_data[i, j]) > 0):
                new_data.append(DataPoint(start_time=i, end_time=j,
                                          offset=beaconstream[0].offset, sample="not around home beacon"))
            else:
                new_data.append(DataPoint(start_time=i, end_time=j,
                                          offset=beaconstream[0].offset, sample="around home beacon"))
        try:
            self.store_data("metadata/home_beacon_context.json",
                            input_streams, user_id, new_data,
                            "HOME_BEACON_CONTEXT")#FIXME
        except Exception as e:
            print("Exception:", str(e))

    def work_beacon_context(self, beaconworkstream, beacon_stream_id, beacon_stream_name, user_id):
        # FIXME document
        input_streams = []
        input_streams.append({"identifier": beacon_stream_id, "name": beacon_stream_name})
        if (len(beaconworkstream) > 0):
            beaconstream = beaconworkstream
            windowed_data = window.window(beaconstream, self.window_size, True)
        new_data = []
        for i, j in windowed_data:
            if (len(windowed_data[i, j]) > 0):
                new_data.append(DataPoint(start_time=i, end_time=j,
                                          offset=beaconstream[0].offset,
                                          sample="not around work
                                          beacon"))#FIXME
            else:
                new_data.append(DataPoint(start_time=i, end_time=j,
                                          offset=beaconstream[0].offset,
                                          sample="around work beacon"))#FIXME

        try:
            self.store_data("metadata/home_beacon_context.json",#FIXME
                            input_streams, user_id, new_data,
                            "WORK_BEACON_CONTEXT")#FIXME
        except Exception as e:
            print("Exception:", str(e))

    def store_data(self, filepath, input_streams, user_id, data, str_sufix):
        # FIXME document
        output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(filepath + user_id + str_sufix)))
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        newfilepath = os.path.join(cur_dir, filepath)
        with open(newfilepath, "r") as f:
            metadata = f.read()
            metadata = json.loads(metadata)
            metadata["execution_context"]["processing_module"]["input_streams"] = input_streams
            metadata["identifier"] = str(output_stream_id)
            metadata["owner"] = str(user_id)

            self.store(identifier=output_stream_id, owner=user_id, name=metadata["name"],
                       data_descriptor=metadata["data_descriptor"],
                       execution_context=metadata["execution_context"], annotations=metadata["annotations"],
                       stream_type="datastream", data=data)

    def process(self, user, all_days):
        # FIXME document
        if self.CC is None:
            return

        if not user:
            return

        streams = self.CC.get_user_streams(user)
        if not len(streams):
            self.CC.logging.log('No streams found for user_id %s' % (user))
            return

        for day in all_days:
            self.mark_beacons(self, streams, self.beacon_homestream, user, day)
            self.mark_beacons(self, streams, self.beacon_workstream, user, day)

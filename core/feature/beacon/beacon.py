from cerebralcortex.core.util.data_types import DataPoint
from core.computefeature import ComputeFeatureBase
from core.signalprocessing.window import window

feature_class_name = 'BeaconFeatures'


class BeaconFeatures(ComputeFeatureBase):

    def __init__(self):
        self.window_size = 60

    def mark_beacons(self, streams, stream_name, user_id, day):
        if stream_name in streams:
            beacon_stream_id = streams[stream_name]["identifier"]
            beacon_stream_name = streams[stream_name]["name"]

            stream = self.CC.get_stream(beacon_stream_id, user_id=user_id, day=day)

            if (len(stream.data) != 0):
                if (stream_name == 'BEACON--org.md2k.beacon--BEACON--HOME'):
                    self.home_beacon_context(
                        stream.data, beacon_stream_id, beacon_stream_name, user_id)
                else:
                    self.work_beacon_context(
                        stream.data, beacon_stream_id, beacon_stream_name, user_id)

    def home_beacon_context(self, beaconhomestream, beacon_stream_id, beacon_stream_name, user_id):
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
                            input_streams, user_id, new_data, "HOME_BEACON_CONTEXT", self)
        except Exception as e:
            print("Exception:", str(e))

    def work_beacon_context(self, beaconworkstream, beacon_stream_id, beacon_stream_name, user_id):
        input_streams = []
        input_streams.append({"identifier": beacon_stream_id, "name": beacon_stream_name})
        if (len(beaconworkstream) > 0):
            beaconstream = beaconworkstream
            windowed_data = window.window(beaconstream, self.window_size, True)
        new_data = []
        for i, j in windowed_data:
            if (len(windowed_data[i, j]) > 0):
                new_data.append(DataPoint(start_time=i, end_time=j,
                                          offset=beaconstream[0].offset, sample="not around work beacon"))
            else:
                new_data.append(DataPoint(start_time=i, end_time=j,
                                          offset=beaconstream[0].offset, sample="around work beacon"))

        try:
            self.store_data("metadata/home_beacon_context.json",
                            input_streams, user_id, new_data, "WORK_BEACON_CONTEXT", self)
        except Exception as e:
            print("Exception:", str(e))

    def process(self, user, all_days):
        if self.CC is not None:
            if user:
                streams = self.CC.get_user_streams(user)
                if not len(streams):
                    self.CC.logging.log('No streams found for user_id %s' % (user))
                    return

                for day in all_days:
                    beacon_homestream = "BEACON--org.md2k.beacon--BEACON--HOME"
                    beacon_workstream = "BEACON--org.md2k.beacon--BEACON--WORK 1"  # TODO: add workplace 2
                    self.mark_beacons(self, streams, beacon_homestream, user, day)
                    self.mark_beacons(self, streams, beacon_workstream, user, day)

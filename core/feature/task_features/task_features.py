from core.feature.task_features.utils import *
from core.computefeature import ComputeFeatureBase
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet

feature_class_name = 'TaskFeatures'


class TaskFeatures(ComputeFeatureBase):
    """
....
    """

    # def __init__(self):
    #     CC_CONFIG_PATH = '/home/md2k/cc_configuration.yml'
    #     self.CC = CerebralCortex(CC_CONFIG_PATH)

    def get_day_data(self, stream_name, user_id, day):
        day_data = []
        stream_ids = self.CC.get_stream_id(user_id, stream_name)
        for stream_id in stream_ids:
            data_stream = self.CC.get_stream(stream_id["identifier"],
                                             day=day,
                                             user_id=user_id,
                                             data_type=DataSet.COMPLETE)
            if data_stream is not None and len(data_stream.data) > 0:
                day_data.extend(data_stream.data)

        day_data.sort(key=lambda x: x.start_time)

        return day_data

    def process(self, user, all_days):
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

            # gets all posture datapoints
            get_all_data = self.get_day_data(posture_stream_name,
                                             user, day)
            if len(get_all_data) != 0:  # creates a dictionary of start and end times
                posture_with_time = process_data(get_all_data)

            # gets all activity datapoints
            get_all_data = self.get_day_data(activity_stream_name, user, day)

            if len(get_all_data) != 0:  # creates a dictionary of start and end times
                activity_with_time = process_data(get_all_data)

            # gets all office datapoints
            get_all_data = self.get_day_data(office_stream_name, user, day)
            if len(get_all_data) != 0:  # creates a dictionary of start and end times
                office_with_time = process_data(get_all_data)

            # gets all beacon datapoints
            get_all_data = self.get_day_data(beacon_stream_name, user, day)

            # beacon stream does not have timezone information.so timezone
            # information is incorportaed for uniformity with other streams
            if len(get_all_data) != 0:
                updatedlist = []
                for dp in get_all_data:
                    st = dp.start_time
                    st = st.replace(tzinfo=pytz.utc)
                    et = dp.end_time
                    et = et.replace(tzinfo=pytz.utc)
                    ndp = DataPoint(start_time=st, end_time=et,
                                    offset=dp.offset, sample=dp.sample)
                    updatedlist.append(ndp)

                beacon_with_time = process_data(updatedlist)

            # get the time offset for the dataset
            offset = 0
            if len(get_all_data) > 0:
                offset = get_all_data[0].offset

            target_total_time, posture_office = output_stream(posture_with_time,
                                                              office_with_time,
                                                              offset,
                                                              'office')
            if len(posture_office)> 0:
                self.store_stream(filepath='posture_office_context_daily.json',
                                  input_streams=[
                                      streams[posture_stream_name],
                                      streams[beacon_stream_name],
                                      streams[office_stream_name]],
                                  user_id=user,
                                  data=posture_office)

                posture_office_fraction = target_in_fraction_of_context(
                    target_total_time,
                    office_with_time, offset, 'Work')

                self.store_stream(
                    filepath='posture_office_context_fraction_hourly.json',
                    input_streams=[
                        streams[posture_stream_name],
                        streams[office_stream_name]],
                    user_id=user,
                    data=posture_office_fraction)

            target_total_time, activity_office = output_stream(
                activity_with_time,
                office_with_time, offset,
                'office')
            if len(activity_office)> 0:
                self.store_stream(filepath='activity_office_context_daily.json',
                                  input_streams=[
                                      streams[activity_stream_name],
                                      streams[office_stream_name]],
                                  user_id=user,
                                  data=activity_office)

                activity_office_fraction = target_in_fraction_of_context(
                    target_total_time, office_with_time,
                    offset, 'Work')
                self.store_stream(
                    filepath='activity_office_context_fraction_hourly.json',
                    input_streams=[
                        streams[activity_stream_name],
                        streams[office_stream_name]],
                    user_id=user,
                    data=activity_office_fraction)

            target_total_time, posture_beacon = output_stream(posture_with_time,
                                                              beacon_with_time,
                                                              offset,
                                                              'beacon')
            if len(posture_beacon)> 0:
                self.store_stream(filepath='posture_beacon_context_daily.json',
                                  input_streams=[
                                      streams[posture_stream_name],
                                      streams[beacon_stream_name]],
                                  user_id=user,
                                  data=posture_beacon)

                posture_beacon_fraction = target_in_fraction_of_context(
                    target_total_time, beacon_with_time,
                    offset, '1')
                self.store_stream(
                    filepath='posture_beacon_context_fraction_hourly.json',
                    input_streams=[
                        streams[posture_stream_name],
                        streams[beacon_stream_name]],
                    user_id=user,
                    data=posture_beacon_fraction)

            target_total_time, activity_beacon = output_stream(
                activity_with_time,
                beacon_with_time, offset,
                'beacon')
            if len(activity_beacon)> 0:
                self.store_stream(filepath='activity_beacon_context_daily.json',
                                  input_streams=[
                                      streams[activity_stream_name],
                                      streams[beacon_stream_name]],
                                  user_id=user,
                                  data=activity_beacon)
                activity_beacon_fraction = target_in_fraction_of_context(
                    target_total_time, beacon_with_time,
                    offset, '1')
                self.store_stream(
                    filepath='activity_beacon_context_fraction_hourly.json',
                    input_streams=[
                        streams[activity_stream_name],
                        streams[beacon_stream_name]],
                    user_id=user,
                    data=activity_beacon_fraction)

        self.CC.logging.log(
            "Finished processing Task features for user: %s" % (user))

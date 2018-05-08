# Copyright (c) 2018, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
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

import uuid
from datetime import timedelta
import argparse
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from core.computefeature import ComputeFeatureBase
from core.feature.context.context_interaction import ContextInteraction
from core.feature.context.context_where import ContextWhere
from core.feature.context.context_activity_engaged import ContextActivityEngaged
feature_class_name = 'Context'


class Context(ComputeFeatureBase, ContextInteraction, ContextWhere, ContextActivityEngaged):
    """
    Detect whether a person was interacting with other people immediately before filling qualtrics survey.
    """

    def get_day_data(self, user_id, stream_name, day):
        """
        get list of DataPoint for the stream name

        :param string stream_name: Name of the stream
        :param string user_id: UID of the user
        :param string day: YYYYMMDD
        :return:
        """

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
        return {"data":day_data, "stream_name":stream_name,"stream_ids": stream_ids}




    def get_time_window_before_survey(self, user_id: uuid, day: str) -> dict:
        """
        Returns five minutes start/end-time window before starting qualtrics survey
        :param user_id:
        :param day:
        :return:
        """
        qualtrics_stream_name = "org.md2k.data_qualtrics.context.v15.d"
        data = self.get_day_data(user_id, qualtrics_stream_name, day)["data"]

        if len(data) > 0:
            survey_start_time = data[0].start_time
            offset = data[0].offset
            time_window_before_survey = survey_start_time - timedelta(
                minutes=5)  # 5 minutes window before starting the survey
            return {"start_time": time_window_before_survey, "end_time": survey_start_time, "offset":offset}

    def process(self, user, all_days):
        '''
        Entry point for the driver to execute this feature
        :param user:
        :param all_days:
        :return:
        '''

        for day in all_days:
            before_survey_time = self.get_time_window_before_survey(user, day)

            # For interaction - Q1
            phone_app_cat_usage = self.get_day_data(user, "org.md2k.data_analysis.feature.phone.app_category_interval", day) # 1
            call_duration_cu = self.get_day_data(user, "CU_CALL_DURATION--edu.dartmouth.eureka", day) # 1
            voice_feature = self.get_day_data(user, "TODO", day)  # TODO: wait for Robin # 2

            # compute interaction context activity engaged - Q2
            location_from_model = self.get_day_data(user, "org.md2k.data_analysis.gps_episodes_and_semantic_location_from_model", day)
            physical_activity_wrist_sensor = self.get_day_data(user, "org.md2k.data_analysis.feature.body_posture.wrist.accel_only.10_second", day)
            #phone_app_cat_usage = self.get_day_data(user, "org.md2k.data_analysis.feature.phone.app_category_interval", day)
            places = self.get_day_data(user, "org.md2k.data_analysis.gps_episodes_and_semantic_location_from_places", day)
            phone_physical_activity = self.get_day_data(user, "ACTIVITY_TYPE--org.md2k.phonesensor--PHONE", day)

            # Context where - Q3
            # location_from_model, places, phone_physical_activity


            #self.get_context_interaction(before_survey_time,user,phone_app_cat_usage, call_duration_cu, voice_feature)
            #self.get_activity_engaged(before_survey_time,user,location_from_model,call_duration_cu,phone_app_cat_usage,places,phone_physical_activity,physical_activity_wrist_sensor)
            self.get_context_where(before_survey_time,user,location_from_model, places, phone_physical_activity)

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description='CerebralCortex '
#                                                  'Feature Processing Driver')
#     parser.add_argument("-c", "--cc-config", help="Path to file containing the "
#                                                   "CerebralCortex configuration", required=False)
#     args = vars(parser.parse_args())
#     if args['cc_config']:
#         cc_config_path = args['cc_config']
#     else:
#         cc_config_path = "cc_configuration.yml"
#
#     CC = CerebralCortex(cc_config_path)
# ------------------------------ QUESTIONS MAPPING TO STREAM NAMES --------------- #
# Question - 1
# In person (8)
# electronically (9 and Robin stream)

# Question - 2
# work and work related activites (2)
# phone calls (4)
# mail, email, social media (5) (check if user was at home first, (2))
# leasure and/sport (3)
# Purchasing goods or services (3)
# Eating and/or drinking (3)
# Household activities (7)
# caring and helping household care (10)
# personal care activities (7)
# Educational acitivities (3)
# Organizational, civic, and or religious activities (3)
# Travel or commuting (7)
# Other

# Questions - 3
# Home (2)
# Work (2)
# Indoor (3)
# Outdoor (3)
# Vehicle (7)
# Other

# 1 - gps_episodes_and_semantic_location_user_marked | home/work where user marked it not the sensor (not for all user)
# 2 - gps_episodes_and_semantic_location_from_model | same as above but for all user (org.md2k.data_analysis.gps_episodes_and_semantic_location_from_model)
# 3 - gps_episodes_and_semantic_location_from_places | various places (for all users) (org.md2k.data_analysis.gps_episodes_and_semantic_location_from_places)
# 4 - average_call_duration_hourly (org.md2k.data_analysis.feature.phone.call_duration.hour.average)
# 5 - app_usage_intervals (org.md2k.data_analysis.feature.phone.app_category_interval)
# 6 - activity_type_10seconds_window (org.md2k.data_analysis.feature.activity.wrist.10_seconds)
# 7 - ACTIVITY_TYPE--org.md2k.phonesensor--PHONE
# 8 - org.md2k.data_analysis.feature.phone.app_category_interval
# 9 - CU_CALL_DURATION--edu.dartmouth.eureka
# 10 - org.md2k.data_analysis.feature.posture

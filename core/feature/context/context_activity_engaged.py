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

from cerebralcortex.core.datatypes.datapoint import DataPoint
from core.feature.context.util import get_home_work_location, get_phone_physical_activity_data, get_places, is_on_sms, \
    is_on_phone, is_on_social_app,get_physical_activity_wrist_sensor


class ContextActivityEngaged():
    """
    Detect what activity a person was engaged in immediately before filling qualtrics survey
    """

    def get_activity_engaged(self, before_survey_time, user, location_from_model, call_duration_cu, sms,
                             phone_app_cat_usage, places, phone_physical_activity, physical_activity_wrist_sensor):
        # location_from_model - [DataPoint(2017-11-05 15:36:14.527000+00:00, 2017-11-06 12:32:54.605000+00:00, -21600000, home)] (sample=home, work, undefined)
        # call duration - [DataPoint(2017-11-05 14:30:55.689000+00:00, None, -21600000, 53.0)]
        # phone category sample - [DataPoint(2018-01-15 22:45:24.203000+00:00, 2018-01-15 22:50:25.303000+00:00, 0, Communication)]
        # places - [DataPoint(2017-11-20 00:43:49.698000+00:00, 2017-11-20 12:11:13.932000+00:00, -21600000, ['yes', 'no', 'no', 'no', 'no', 'no'])] - restaurant,school,worshi,entertainment,store,sports_arena
        # phone physical activity - [DataPoint(2017-12-14 23:06:22.729000+00:00, None, -18000000, [0.0, 100.0])] - [type -confidence]
        # physical activity wrist - DataPoint(2017-12-02 21:37:51.937000+00:00, 2017-12-02 21:38:01.921000+00:00, 25200000, standing)

        start_data_time = before_survey_time.get("start_time", None)
        end_data_time = before_survey_time.get("end_time", None)
        offset = before_survey_time.get("offset", None)


        location_data = get_home_work_location(location_from_model.get("data", []), start_data_time)
        places_data = get_places(places.get("data", []), start_data_time, end_data_time)
        phone_physical_activity_val = get_phone_physical_activity_data(phone_physical_activity.get("data", []),
                                                                       start_data_time, end_data_time)

        religious_place = 0
        educational_place = 0
        leisure_sport_place = 0
        shops_place = 0
        restaurant_place = 0

        on_sms = is_on_sms(sms.get("data",[]), start_data_time, end_data_time)
        on_phone = is_on_phone(call_duration_cu.get("data",[]), start_data_time, end_data_time)
        on_social_app = is_on_social_app(phone_app_cat_usage.get("data",[]), start_data_time,end_data_time)
        activity_wrist_sensor = get_physical_activity_wrist_sensor(physical_activity_wrist_sensor.get("data",[]), start_data_time, end_data_time)
        if len(places_data) > 0:
            for plc in places_data:
                if plc[2] == "yes":
                    religious_place += 1
                elif plc[1] == "yes":
                    educational_place += 1
                elif plc[5] == "yes" or plc[3] == "yes":
                    leisure_sport_place += 1
                elif plc[4] == "yes":
                    shops_place += 1
                elif plc[0] == "yes":
                    restaurant_place += 1

        if religious_place > 0:
            sample = [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0]
        elif educational_place > 0:
            sample = [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0]
        elif shops_place > 0:
            sample = [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]
        elif leisure_sport_place > 0:
            sample = [0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        elif restaurant_place > 0:
            sample = [0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]
        elif location_data != "work" and (
                on_sms or on_phone or on_social_app):
            sample = [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        elif phone_physical_activity_val == 6:  # in vehicle
            sample = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]
        elif location_data == "work":
            sample = [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        elif location_data == "home" and (
                phone_physical_activity_val == 1 or phone_physical_activity_val == 3 or phone_physical_activity_val == 4):
            sample = [0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0]
        elif location_data == "home" and (activity_wrist_sensor=="lying" or activity_wrist_sensor=="sitting"):
            sample = [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]
        else:
            sample = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]

        dp = DataPoint(start_time=start_data_time, end_time=end_data_time, offset=offset, sample=sample)

        print(dp)

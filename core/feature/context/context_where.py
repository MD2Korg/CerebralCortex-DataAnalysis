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
from core.feature.context.util import get_home_work_location, get_phone_physical_activity_data,get_places


class ContextWhere():
    """
    Detect where a person was before filling qualtrics survey
    """

    def get_context_where(self, before_survey_time, user,location_from_model, places, phone_physical_activity):
        # location_from_model - [DataPoint(2017-11-05 15:36:14.527000+00:00, 2017-11-06 12:32:54.605000+00:00, -21600000, home)] (sample=home, work, undefined)
        # places - [DataPoint(2017-11-20 00:43:49.698000+00:00, 2017-11-20 12:11:13.932000+00:00, -21600000, ['yes', 'no', 'no', 'no', 'no', 'no'])] - restaurant,school,worshi,entertainment,store,sports_arena
        # phone physical activity - [DataPoint(2017-12-14 23:06:22.729000+00:00, None, -18000000, [0.0, 100.0])] - [type -confidence]

        start_data_time = before_survey_time.get("start_time",None)
        end_data_time = before_survey_time.get("end_time", None)
        offset = before_survey_time.get("offset", None)
        outdoor = 0
        indoor = 0
        location_data = get_home_work_location(location_from_model.get("data", []), start_data_time)
        places_data = get_places(places.get("data", []), start_data_time)
        phone_physical_activity_val = get_phone_physical_activity_data(phone_physical_activity.get("data", []), start_data_time, end_data_time)

        if len(places_data)>0:
            for plc in places_data:
                if plc[5]=="yes":
                    outdoor +=1
                else:
                    indoor +=1
        if phone_physical_activity_val==6: # in vehicle
            sample = [0,0,0,0,1,0]
        elif location_data=="home":
            sample = [1,0,0,0,0,0]
        elif location_data=="work":
            sample = [0,1,0,0,0,0]
        elif outdoor>0 or phone_physical_activity_val==1 or phone_physical_activity_val==3 or phone_physical_activity_val==4:
            sample = [0,0,0,1,0,0]
        elif indoor>0:
            sample = [0,0,1,0,0,0]
        else:
            sample = [0,0,0,0,0,1]

        dp = DataPoint(start_time=start_data_time,end_time=end_data_time, offset=offset, sample=sample)

        print(dp)
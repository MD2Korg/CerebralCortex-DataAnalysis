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
from core.feature.context.util import is_talking, is_on_phone, is_on_social_app


class ContextInteraction():
    """
    Detect whether a person was interacting with other people immediately before filling qualtrics survey.
    """

    def get_context_interaction(self, before_survey_time, user, phone_app_cat_usage, call_duration_cu, voice_feature):
        # category sample - [DataPoint(2018-01-15 22:45:24.203000+00:00, 2018-01-15 22:50:25.303000+00:00, 0, Communication)]
        # call duration - [DataPoint(2017-11-05 14:30:55.689000+00:00, None, -21600000, 53.0)]
        # voice feature - 1 for voice and 0 for no voice - per minute

        start_data_time = before_survey_time.get("start_time", None)
        end_data_time = before_survey_time.get("end_time", None)
        offset = before_survey_time.get("offset", None)

        talking = is_talking(voice_feature.get("data", []), start_data_time, end_data_time)
        on_phone = is_on_phone(call_duration_cu.get("data", []), start_data_time, end_data_time)
        on_social_app = is_on_social_app(phone_app_cat_usage.get("data", []), start_data_time, end_data_time)

        if on_social_app:
            sample = [0, 1, 0]
        elif talking or on_phone:
            sample = [1, 0, 0]
        else:
            sample = [0, 0, 1]

        dp = DataPoint(start_time=start_data_time, end_time=end_data_time, offset=offset, sample=sample)

        print(dp)

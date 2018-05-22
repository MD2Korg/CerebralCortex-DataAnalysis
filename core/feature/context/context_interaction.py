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

from cerebralcortex.core.datatypes.datapoint import DataPoint
from core.feature.context.util import is_talking, is_on_phone, is_on_social_app, get_input_streams


class ContextInteraction():
    """
    Detect whether a person was interacting with other people immediately before filling qualtrics survey.
    """

    def get_context_interaction(self, before_survey_time: dict, user: uuid, phone_app_cat_usage: dict,
                                call_duration_cu: dict,
                                voice_feature: dict):
        """
        Compute a user interaction right before (10 minutes) (s)he started qualtrics context survey
        :param before_survey_time:
        :param user:
        :param phone_app_cat_usage:
        :param call_duration_cu:
        :param voice_feature:
        """

        # Metadata is not accurate, that's why I put sample output of all input streams here
        # category sample - [DataPoint(2018-01-15 22:45:24.203000+00:00, 2018-01-15 22:50:25.303000+00:00, 0, Communication)]
        # call duration - [DataPoint(2017-11-05 14:30:55.689000+00:00, None, -21600000, 53.0)]
        # voice feature - 1 for voice and 0 for no voice - per minute

        start_data_time = before_survey_time.get("start_time", None)
        end_data_time = before_survey_time.get("end_time", None)
        offset = before_survey_time.get("offset", None)

        talking = is_talking(voice_feature.get("data", []), start_data_time, end_data_time)
        on_phone = is_on_phone(call_duration_cu.get("data", []), start_data_time, end_data_time)
        on_social_app = is_on_social_app(phone_app_cat_usage.get("data", []), start_data_time, end_data_time)
        sample = [0, 0, 0]

        if on_social_app:
            sample[1] = 1
        elif talking or on_phone:
            sample[0] = 1
        else:
            sample[2] = 1

        dp = [DataPoint(start_time=start_data_time, end_time=end_data_time, offset=offset, sample=sample)]

        input_streams = []
        input_streams.extend(get_input_streams(phone_app_cat_usage))
        input_streams.extend(get_input_streams(call_duration_cu))
        input_streams.extend(get_input_streams(voice_feature))

        self.store_stream(filepath="context_interaction.json",
                          input_streams=input_streams,
                          user_id=user,
                          data=dp, localtime=False)

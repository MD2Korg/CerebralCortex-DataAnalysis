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

from core.computefeature import ComputeFeatureBase

#feature_class_name = 'ContextActivityEngaged'


class ContextActivityEngaged():
    """
    Detect what activity a person was engaged in immediately before filling qualtrics survey
    """

    def get_activity_engaged(self,before_survey_time,user,location_from_model,call_duration_cu,phone_app_cat_usage,places,phone_physical_activity,physical_activity_wrist_sensor):
        # location_from_model - [DataPoint(2017-11-05 15:36:14.527000+00:00, 2017-11-06 12:32:54.605000+00:00, -21600000, home)] (sample=home, work, undefined)
        # call duration - [DataPoint(2017-11-05 14:30:55.689000+00:00, None, -21600000, 53.0)]
        # category sample - [DataPoint(2018-01-15 22:45:24.203000+00:00, 2018-01-15 22:50:25.303000+00:00, 0, Communication)]
        # places - [DataPoint(2017-11-20 00:43:49.698000+00:00, 2017-11-20 12:11:13.932000+00:00, -21600000, ['yes', 'no', 'no', 'no', 'no', 'no'])] - restaurant,school,worshi,entertainment,store,sports_arena
        # phone physical activity - [DataPoint(2017-12-14 23:06:22.729000+00:00, None, -18000000, [0.0, 100.0])] - [type -confidence]
        # physical activity wrist - DataPoint(2017-12-02 21:37:51.937000+00:00, 2017-12-02 21:38:01.921000+00:00, 25200000, standing)
        pass
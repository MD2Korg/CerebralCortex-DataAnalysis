# Copyright (c) 2017, MD2K Center of Excellence
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
import syslog

# Initialize logging
syslog.openlog(ident="CerebralCortex-ExampleFeature")

# This variable must match the class name
feature_class_name = 'ExampleFeature'


class ExampleFeature(ComputeFeatureBase):
    """
    Class specific documentation goes here.
    """

    def helper_function(self, user_id: str, day: str):
        """
        This method performs relevant computations for computing a feature

        :param str user_id: User identifier in UUID format
        :param str day: Date string (YYYYMMDD) for which participant day to operate on
        """

        # store your results by calling the store() method in ComputeFeatureBase
        pass

    def process(self, user_id: str, all_days: list):
        """
        This is the main entry point for feature computation and is called by the main driver application

        :param str user_id: User identifier in UUID format
        :param list all_days: List of all days to run this feature over
        """
        syslog.syslog("Processing ExampleFeature")
        # Get data streams
        # Apply admission control on your data streams
        # process your data streams, optionally you may define other helper_functions ()
        #     to make your code more readable

        for day in all_days:
            self.helper_function(user_id, day)

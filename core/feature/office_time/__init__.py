# Copyright (c) 2018, MD2K Center of Excellence
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
"""Office time features

Features:
    1. Working days from GPS
    2. Working days from Beacon
    3. Office staying time from GPS
    4. Office staying time from Beacon
    5. Expected Arrival time from GPS
        - Conservative
            - after_expected_conservative_time
            - before_expected_conservative_time
            - in_expected_conservative_time
        - Liberal
            - after_expected_liberal_time
            - before_expected_liberal_time
            - in_expected_liberal_time
    6. Expected Arrival time from Beacon
        - Conservative
            - after_expected_conservative_time
            - before_expected_conservative_time
            - in_expected_conservative_time
        - Liberal
            - after_expected_liberal_time
            - before_expected_liberal_time
            - in_expected_liberal_time
    7. Expected Staying time from GPS
        - Conservative
            - after_expected_conservative_time
            - before_expected_conservative_time
            - in_expected_conservative_time
        - Liberal
            - after_expected_liberal_time
            - before_expected_liberal_time
            - in_expected_liberal_time
    8. Expected staying time from Beacon
        - Conservative
            - after_expected_conservative_time
            - before_expected_conservative_time
            - in_expected_conservative_time
        - Liberal
            - after_expected_liberal_time
            - before_expected_liberal_time
            - in_expected_liberal_timezone pressure sensor data yield

Notes:
    Algorithm here

References:
    1.
"""
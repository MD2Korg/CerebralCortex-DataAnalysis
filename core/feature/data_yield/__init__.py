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
"""Calculates the motionsenseHRV data yield

Calculates the the motionsenseHRV data yield for each minute of the day when data is present
rendering a list of datapoints. each DataPoint contains a boolean decision indicating if the
sensor was worn or not

Notes:
    Input:
        MotionsenseHRV or MotionsenseHRV+ raw datastream

    Steps:
        1. Decode the raw datastream to get the ppg signal
        2. Windowing of PPG signals on a window size of 60 secs
        3. For every 10 seconds of the 60 seconds window determine if the sensor was worn or
        not rendering at most 6 decisions for a single minute length window
        4. Take the majority of the decisions in the minute length window as the final decision
        of the window
        5. store the datastream

    Output:
        Datastream containing a list of DataPoints where each DataPoint represents a minute of
        the day where sensor data is present and the sample is a boolean value representing if
        the sensor was worn or not

References:
    1.
"""
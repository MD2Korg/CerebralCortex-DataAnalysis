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
"""MotionSenseHRV Decoder

Takes as input raw datastreams from motionsenseHRV and decodes them
to get the Accelerometer, Gyroscope, PPG, Sequence number timeseries.
Last of all it does timestamp correction on all the timeseries and saves them.

Notes:
    Input:
        Raw datastream of motionsenseHRV and motionsenseHRV+
        Each DataPoint contains a 20 byte array that was transmitted to the mobile phone by the sensors itself

    Steps:
        1. Decode accelerometer,gyroscope,ppg and sequence number timeseries from the raw datastreams
        2. Timestamp correction based on the sequence number timeseries
        3. Store the timestamp corrected timeseries

    Output:
        motionsenseHRV decoded datastream. Each DataPoint sample contains a list of 9 values with the first 3
        corresponding to accelerometer, next three corresponding to gyroscope and the last three are Red, Infrered,
        Green channels of PPG

References:
    1.
"""
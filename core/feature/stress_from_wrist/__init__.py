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
"""Computes stress from wrist sensor data

This class extracts all the RR-interval data that is present for a person on a specific day.
It calculates the necessary features from the rr-interval data and assigns each minute of the day as
Stress/Not stress state.

Notes:
    Input:
        RR interval datastream.
        Each DataPoint contains the following items:
            1. A list of RR-interval array. Each entry in the list corresponds to a realization
            of the position of R peaks in that minute
            2. Standard Deviation of Heart Rate within the minute
            3. A list corresponding to the heart rate values calculated from variable realizations
            of the RR interval on a sliding window of window size = 8 second and window offset = 2 second.
    Steps:
        1. Extract all the RR interval data for a user on a specific day
        2. Extract all the 16 features per minute of the RR interval data
        3. Standardize each feature row and output stress/not stress state

    Output:
        A datastream containing a list of DataPoints, each DataPoint represents one minute where sample=0
        means the user was not stressed and sample=1 means the person was stressed

    List of Features:
        1. 82nd percentile
        2. 18th percentile
        3. mean
        4. median
        5. standard deviation
        6. inter quartile deviation
        7. skewness
        8. kurtosis
        9. Energy in very low frequency range
        10. Energy in very high frequency range
        11. Energy in low frequency range
        12. Ration of low to high frequency energy
        13. quartile deviation
        14. heart rate
        15. median of inter percentile difference
        16. standard deviation of inter percentile difference

References:
    1. K. Hovsepian, M. al’Absi, E. Ertin, T. Kamarck, M. Nakajima, and S. Kumar,
    "cStress: Towards a Gold Standard for Continuous Stress Assessment in the Mobile Environment,"
    ACM UbiComp, pp. 493-504, 2015.
"""

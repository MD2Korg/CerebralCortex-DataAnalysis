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
"""Respiration Cycle Statistics

Combines Respiration raw datastream with respiration baseline datastream and identifies the respiration
cycles through identification of peak and valley DataPoints. Then for each respiration cycle a set of 21
features are calculated.

Notes:
    Input:
        1. Respiration Raw datastream
        2. Respiration baseline datastream

    Steps:
        1. Combine the respiration raw with respiration baseline datastream on a DataPoint by DataPoint basis
        2. Filter the combined respiration raw and baseline datastream to get rid of the signal at
        times when the person was not wearing the sensor suite
        3. Identify the Respiration Cycles by getting the peak and valley points
        4. Compute the features

    Output:
        A list of DataPoints with each DataPoint representing a respiration cycle and containing 21 different features
        calculated from it.
        The features are:
            1.  inspiration_duration
            2.  expiration_duration
            3.  respiration_duration
            4.  inspiration_expiration_duration_ratio
            5.  stretch
            6.  inspiration_velocity
            7.  expiration_velocity
            8.  skewness
            9.  kurtosis
            10.  entropy
            11.  inspiration_expiration_velocity_ratio
            12.  inspiration_expiration_area_ratio
            13.  expiration_respiration_duration_ratio
            14.  respiration_area_inspiration_duration_ratio
            15.  power_.05-.2_Hz
            16.  power_.201-.4_Hz
            17.  power_.401-.6_Hz
            18.  power_.601-.8_Hz
            19.  power_.801-1_Hz
            20.  correlation_previous_cycle
            21.  correlation_next_cycle

References:
    1. "Rummana Bari, Roy J. Adams, Md. Mahbubur Rahman, Megan Battles Parsons, Eugene H. Buder, and Santosh Kumar. 2018.
        rConverse: Moment by Moment Conversation Detection Using a Mobile Respiration Sensor.
        Proc. ACM Interact. Mob. Wearable Ubiquitous Technol. 2, 1, Article 2 (March 2018), 27 pages.
        DOI: https://doi.org/10.1145/3191734"

    2. K. Hovsepian, M. al’Absi, E. Ertin, T. Kamarck, M. Nakajima, and S. Kumar,
        "cStress: Towards a Gold Standard for Continuous Stress Assessment in the Mobile Environment,"
        ACM UbiComp, pp. 493-504, 2015.
"""
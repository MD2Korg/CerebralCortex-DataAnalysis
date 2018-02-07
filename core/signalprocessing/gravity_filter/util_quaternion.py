# Copyright (c) 2018, MD2K Center of Excellence
# - Sayma Akther <sakther@memphis.edu>
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

import math

def degree_to_radian(deg):
    return math.pi * deg / 180.0
    
def Quatern_Conjugate(q):
    quaternion_conj = [-q[0], -q[1], -q[2], q[3]]
    return quaternion_conj

def Quaternion_multiplication(Q2,Q1):
# be aware quaternion multiplication does not commute
# here the code is calculating Q2 * Q1
    Q3 = [0]*4
    Q3[0] = Q1[3]*Q2[0] + Q1[0]*Q2[3] - Q1[1]*Q2[2] + Q1[2]*Q2[1]
    Q3[1] = Q1[3]*Q2[1] + Q1[0]*Q2[2] + Q1[1]*Q2[3] - Q1[2]*Q2[0]
    Q3[2] = Q1[3]*Q2[2] - Q1[0]*Q2[1] + Q1[1]*Q2[0] + Q1[2]*Q2[3]
    Q3[3] = Q1[3]*Q2[3] - Q1[0]*Q2[0] - Q1[1]*Q2[1] - Q1[2]*Q2[2]
    return Q3
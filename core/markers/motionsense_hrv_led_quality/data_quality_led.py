# Copyright (c) 2017, MD2K Center of Excellence
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

import numpy as np
from scipy import signal
from cerebralcortex.core.datatypes.datapoint import DataPoint
def data_quality_led(windowed_data):
    """
    
    :param windowed_data: a datastream with a collection of windows 
    :return: a list of window labels
    """
    window_list = windowed_data
    dps = []
    for key ,window in window_list.items():
        quality_results = compute_quality(window)
        dps.append(DataPoint(key[0], key[1], quality_results))

    return dps

def isDatapointsWithinRange(red,infrared,green):
    red = np.asarray(red, dtype=np.float32)
    infrared = np.asarray(infrared, dtype=np.float32)
    green = np.asarray(green, dtype=np.float32)
    a =  len(np.where((red >= 30000)& (red<=170000))[0]) < .64*len(red)
    b = len(np.where((infrared >= 140000)& (infrared<=230000))[0]) < .64*len(infrared)
    c = len(np.where((green >= 3000)& (green<=20000))[0]) < .64*len(green)
    if a and b and c:
        return False
    return True

def bandpassfilter(x,fs):
    """
    
    :param x: a list of samples 
    :param fs: sampling frequency
    :return: filtered list
    """
    x = signal.detrend(x)
    b = signal.firls(129,[0,0.6*2/fs,0.7*2/fs,3*2/fs,3.5*2/fs,1],[0,0,1,1,0,0],[100*0.02,0.02,0.02])
    return signal.convolve(x,b,'valid')


def compute_quality(window):
    """
    
    :param window: a window containing list of datapoints 
    :return: an integer reptresenting the status of the window 0= attached, 1 = not attached
    """
    if len(window)==0:
        return 1 #not attached
    red = [i.sample[0] for i in window]
    infrared = [i.sample[1] for i in window]
    green = [i.sample[2] for i in window]

    if not isDatapointsWithinRange(red,infrared,green):
        return 1

    if np.mean(red) < 5000 and np.mean(infrared) < 5000 and np.mean(green)<5000:
        return 1

    if not (np.mean(red)>np.mean(green) and np.mean(infrared)>np.mean(red)):
        return 1

    diff = 50000
    if np.mean(red)>140000:
        diff = 15000

    if not (np.mean(red) - np.mean(green) > 50000 and np.mean(infrared) - np.mean(red) >diff):
        return 1

    if np.std(bandpassfilter(red,25)) <= 5 and np.std(bandpassfilter(infrared,25)) <= 5 and np.std(bandpassfilter(green,25)) <= 5:
        return 1

    return 0


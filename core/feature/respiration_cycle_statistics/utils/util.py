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

from scipy import signal,interpolate
from cerebralcortex.core.datatypes.datapoint import DataPoint
from enum import Enum
import scipy
import numpy as np
from copy import deepcopy
from scipy import stats
from typing import List

respiration_raw_autosenseble = \
    "RESPIRATION--org.md2k.autosenseble--AUTOSENSE_BLE--CHEST"
respiration_baseline_autosenseble = \
    "RESPIRATION_BASELINE--org.md2k.autosenseble--AUTOSENSE_BLE--CHEST"
Fs = 25

def admission_control(data:List[DataPoint])->List[DataPoint]:
    final_data = []
    for dp in data:
        if not isinstance(dp.sample,list) and dp.sample>=0 and dp.sample<=4095:
            final_data.append(dp)
    return final_data


class Quality(Enum):
    ACCEPTABLE = 1
    UNACCEPTABLE = 0

def recover_rip_rawwithmeasuredref(RIP:list, ref:list,
                                   Fs)-> np.ndarray:
    """
    Recovers Respiration final signal from baseline and raw
    :param RIP: respiration raw
    :param ref: respiration baseline
    :param Fs: sampling frequency
    :return: recovered final respiration signal as a combination of respiration
    raw and baseline
    """
    m = np.mean(RIP)
    ref_synn_noM = -(ref-np.mean(ref))
    RIP_synn_noM = np.array(RIP)
    RIP_synn_noM = RIP_synn_noM - m
    R_36 = 10000
    R_37 = 10000
    R_40 = 604000
    G1 = R_40/(R_36+R_37)
    G_ref = -5
    b = signal.firwin(65,0.3*2/Fs,window='bartlett')
    ref_synn_noM_LP = np.convolve(np.squeeze(ref_synn_noM),b,'same')
    RIP_raw_measuredREF = (ref_synn_noM_LP*G1)/G_ref + RIP_synn_noM.reshape(
        ref_synn_noM_LP.shape)
    return RIP_raw_measuredREF+m


def get_recovery(rip:List[DataPoint],baseline:List[DataPoint],Fs)->List[DataPoint]:
    """
    matches respiration raw with baseline signal and returns the recovered
    signal
    :param rip: respiration raw
    :param baseline:
    :param Fs:
    :return: respiration recovered signal
    """
    rip_index_dict = {rip[i].start_time.timestamp():i for i in range(len(rip))}
    baseline_index_dict = {baseline[i].start_time.timestamp():i
                           for i in range(len(baseline))}
    common_ts = np.intersect1d([i.start_time.timestamp() for i in rip],
                               [i.start_time.timestamp() for i in baseline])
    baseline_ind = np.array([baseline_index_dict[i] for i in common_ts])
    rip_ind = np.array([rip_index_dict[i] for i in common_ts])
    rip = np.array(rip)[rip_ind]
    baseline = np.array(baseline)[baseline_ind]
    recovered = \
        recover_rip_rawwithmeasuredref(
            [i.sample for i in rip],[i.sample for i in baseline],
            Fs)
    recovered_dp_list = \
        [DataPoint.from_tuple(start_time=rip[i].start_time,sample=recovered[i],
                              offset=rip[i].offset)
         for i in range(len(recovered))]
    return recovered_dp_list

def smooth(y:list, box_pts:int)->np.ndarray:
    """
    smooths a 1d signal through convolution
    :param y: signal
    :param box_pts: number of samples in moving average
    :return: smoothed signal
    """
    box = np.ones(box_pts)/box_pts
    y_smooth = np.convolve(y, box, mode='same')
    return y_smooth

def smooth_detrend(sample,ts,window_size=300,no_of_samples=13):
    """
    piecewise detrending of signal by subtracting the mean of every non
    overlapping 300 seconds
    :param sample:
    :param ts:
    :param window_size:
    :param no_of_samples:
    :return:
    """
    sample = smooth(sample,no_of_samples)
    sample_final = []
    initial = ts[0]
    while initial<ts[-1]:
        sample_temp = np.array(sample)[np.where((ts>=initial) &
                                                (ts<(initial+window_size)))[0]]
        sample_final.extend(list(sample_temp-np.mean(sample_temp)))
        initial+=window_size
    return np.array(sample_final)

def filter_bad_rip(ts,sample,window_size=150,filter_condition=120):
    """
    Filters respiration signal to remove outliers
    :param ts:
    :param sample:
    :param window_size:
    :param filter_condition: minimum range of acceptable signal
    :return:
    """
    indexes = np.int64(np.linspace(0,len(sample)-1,len(sample)))
    initial = ts[0]
    sample_final = []
    ts_final = []
    indexes_final = []
    while initial<ts[-1]:
        ind_temp = np.where((ts>=initial) & (ts<(initial+window_size)))[0]
        sample_temp = np.array(sample)[ind_temp]
        if len(sample_temp)>0 and \
                max(sample_temp)-min(sample_temp)>filter_condition:
            sample_final.extend(list(sample[ind_temp]))
            ts_final.extend(list(ts[ind_temp]))
            indexes_final.extend(list(indexes[ind_temp]))
        initial+=window_size
    return np.array(sample_final),np.array(ts_final),np.array(indexes_final)

def get_cycle_quality(sample,ts,fs=25,acceptable_percentage = .5):
    """
    get the quality of a respiration cycle through checking the number of
    datapoints present

    :param sample:
    :param ts:
    :param fs:
    :return:
    """
    if len(sample)>=acceptable_percentage*fs*(ts[-1]-ts[0]):
        return Quality.ACCEPTABLE
    return Quality.UNACCEPTABLE

def get_covariance(data1,data2):
    """
    Get the correlation of two signals with unequal data length
    :param data1:
    :param data2:
    :return:
    """
    if len(data1) > len(data2):
        big = data1
        small = data2
    else:
        big = data2
        small = data1
    f = interpolate.interp1d(np.linspace(1,len(small),len(small)),
                             small,fill_value="extrapolate")
    small_interp = f(np.linspace(1,len(small),len(big)))
    return np.corrcoef(big,small_interp)[0,1]



def bandpower(x, fs, fmin, fmax,nfft_point=1024):
    """
    returns the power in a frequency band of a signal
    :param x:
    :param fs:
    :param fmin: low frequency
    :param fmax: high frequency
    :param nfft_point:
    :return: power in low to high frequency band
    """
    f, Pxx = scipy.signal.welch(x,
                                fs=fs,
                                window=signal.get_window('hamming',len(x)),
                                return_onesided=False)
    ind_min = scipy.argmax(f > fmin) - 1
    ind_max = scipy.argmax(f > fmax) - 1
    return scipy.trapz(Pxx[ind_min: ind_max+1], f[ind_min: ind_max+1])

def return_bandPassedSignal(sample,f1L,f1H,f2L,f2H,Fs=25):
    """
    Return bandpass filtered signal with two set of cutoffs
    :param sample:
    :param f1L:
    :param f1H:
    :param f2L:
    :param f2H:
    :param Fs:
    :return:
    """
    f=2/Fs
    delp=0.02
    dels1=0.02
    dels2=0.02
    F = [0,f1L*f,f1H*f,f2L*f,f2H*f,1]
    A = [0,0,1,1,0,0]
    w = [500/dels1,1/delp,500/dels2]
    fl = 257
    b = signal.firls(fl,F,A,w)
    sample = np.convolve(sample,b,'same')
    return sample

def return_neighbour_cycle_correlation(sample,ts,
                                       inspiration,
                                       unacceptable=-9999):
    """
    Return Neighbour Cycle correlation array of respiration cycles.
    :param sample:
    :param ts:
    :param inspiration:

    :return: modified cycle quality,correlation with previous cycle,
    correlation with next cycle
    """
    cycle_quality = []
    corr_pre_cycle = [0]*len(inspiration)
    corr_post_cycle = [0]*len(inspiration)

    corr_pre_cycle[0] = DataPoint.from_tuple(start_time=inspiration[
        0].start_time,end_time=inspiration[0].end_time,sample=1)

    corr_post_cycle[-1] = DataPoint.from_tuple(start_time=inspiration[
        -1].start_time,end_time=inspiration[-1].end_time,sample=unacceptable)

    start_time = inspiration[0].start_time.timestamp()
    end_time = inspiration[0].end_time.timestamp()
    current_cycle = sample[np.where((ts>=start_time)&(ts<end_time))[0]]

    for i,dp in enumerate(inspiration):
        start_time = dp.start_time.timestamp()
        end_time = dp.end_time.timestamp()
        sample_temp = sample[np.where((ts>=start_time)&(ts<end_time))[0]]
        ts_temp = ts[np.where((ts>=start_time)&(ts<end_time))[0]]
        cycle_quality.append(DataPoint.from_tuple(start_time=dp.start_time,end_time=dp.end_time,
                                                  sample=get_cycle_quality(sample_temp,ts_temp)))
        if i>0:
            if cycle_quality[i].sample == Quality.ACCEPTABLE and cycle_quality[i-1].sample == Quality.ACCEPTABLE:
                corr_pre_cycle[i] = DataPoint.from_tuple(start_time=dp.start_time,end_time=dp.end_time,
                                                         sample=get_covariance(sample_temp,current_cycle))
            else:
                corr_pre_cycle[i] = DataPoint.from_tuple(start_time=dp.start_time,end_time=dp.end_time,
                                                         sample=unacceptable)
            corr_post_cycle[i-1] = corr_pre_cycle[i]
        current_cycle = sample_temp
    return np.array(cycle_quality),np.array(corr_pre_cycle),np.array(corr_post_cycle)

def respiration_area_shape_velocity_calculation(sample, ts, peak,
                                                cycle_quality, fs=25,
                                                unacceptable=-9999):
    """
    Calculates the area , shape and velocity features from respiration cycles
    :param sample:
    :param ts:
    :param peak:
    :param cycle_quality:
    :param fs:
    :return: respiration cycle quality,Inspiration area,Expiration area,
    Respiration area,Insp Exp Area ratio,Inspiration velocity,
    Expiration velocity,skewness of cycle, kurtosis of cycle
    """
    a = deepcopy(cycle_quality)
    for i,dp in enumerate(a):
        a[i].sample = unacceptable
    area_Inspiration, area_Expiration, area_Respiration,area_ie_ratio =deepcopy(a), deepcopy(a), deepcopy(a), deepcopy(a)
    velocity_Inspiration, velocity_Expiration =deepcopy(a), deepcopy(a)
    shape_skew, shape_kurt=deepcopy(a), deepcopy(a)
    ts_index = {element:i for i,element in enumerate(ts)}
    for i,dp in enumerate(cycle_quality):
        if dp.sample == Quality.UNACCEPTABLE:
            continue
        valley_ind1 = ts_index[dp.start_time.timestamp()]
        valley_ind2 = ts_index[dp.end_time.timestamp()]
        peak_ind = ts_index[peak[i].start_time.timestamp()]
        if abs(valley_ind1 - peak_ind) < .05*fs or abs(peak_ind-valley_ind2) < .05*fs:
            cycle_quality[i].sample = Quality.UNACCEPTABLE
            continue
        sample_temp = sample[valley_ind1:(valley_ind2+1)] - min(sample[valley_ind1:(valley_ind2+1)])
        InspUT = np.trapz(sample_temp[:peak_ind-valley_ind1+1])
        subtractPoint = min(sample_temp[:peak_ind-valley_ind1+1])
        InspLT = np.trapz([0,peak_ind-valley_ind1],[subtractPoint,subtractPoint])
        InspArea = InspUT - InspLT
        if InspArea < 0:
            cycle_quality[i].sample = Quality.UNACCEPTABLE
            continue
        ExpUT = np.trapz(sample_temp[(peak_ind-valley_ind1): (valley_ind2-valley_ind1+1)])
        subtractPoint2 = min(sample_temp[(peak_ind-valley_ind1):(valley_ind2-valley_ind1+1)])
        ExpLT = np.trapz([peak_ind-valley_ind1,valley_ind2-valley_ind1],[subtractPoint2,subtractPoint2])
        ExpArea = ExpUT - ExpLT
        if ExpArea < 0:
            cycle_quality[i].sample = Quality.UNACCEPTABLE
            continue
        area_Inspiration[i].sample = InspArea
        area_Expiration[i].sample = ExpArea
        area_Respiration[i].sample = InspArea + ExpArea
        area_ie_ratio[i].sample = InspArea/ExpArea

        velocity_Inspiration[i].sample = InspArea/(peak_ind-valley_ind1)
        velocity_Expiration[i].sample = ExpArea/(valley_ind2-peak_ind)

        shape_kurt[i].sample = stats.kurtosis(sample_temp)
        shape_skew[i].sample = stats.skew(sample_temp)

    return cycle_quality,area_Inspiration,area_Expiration,area_Respiration,area_ie_ratio, \
           velocity_Inspiration,velocity_Expiration,shape_skew, shape_kurt

def spectral_entropy_calculation(sample, ts, cycle_quality,
                                 unacceptable=-9999):
    """
    Calcuclates the entropy of a respiration cycle
    :param sample:
    :param ts:
    :param cycle_quality:
    :return: entropy of respiration cycle
    """
    a = deepcopy(cycle_quality)
    for i,dp in enumerate(a):
        a[i].sample = unacceptable
    entropy_array = deepcopy(a)
    ts_index = {element:i for i,element in enumerate(ts)}
    for i,dp in enumerate(cycle_quality):
        if dp.sample == Quality.UNACCEPTABLE:
            continue
        valley_ind1 = ts_index[dp.start_time.timestamp()]
        valley_ind2 = ts_index[dp.end_time.timestamp()]
        sample_temp = sample[valley_ind1:(valley_ind2+1)]
        fftx=np.fft.fft(sample_temp)
        sum_fftx = np.sum(np.abs(fftx[1:]))
        fftx=fftx/sum_fftx
        entropy_array[i].sample = stats.entropy(np.abs(fftx[1:]))
    return entropy_array

def calculate_power_in_frequency_band(y, Fs):
    """
    calculates power in respiration cycle from predefined frequency bands
    :param y:
    :param Fs:
    :return: an array of 5 represinting the power in each band
    """
    frequencyBand = np.zeros((5,2))
    frequencyBand[0,:] = [0.05,0.2]
    frequencyBand[1,:] = [0.201,.4]
    frequencyBand[2,:] = [.401,.6]
    frequencyBand[3,:] = [.601,.8]
    frequencyBand[4,:] = [0.801,1]
    all_band_power = np.zeros((1,np.shape(frequencyBand)[0]))
    for i in range(np.shape(frequencyBand)[0]):
        all_band_power[0,i] = 10*np.log10(bandpower(y,Fs,frequencyBand[i,0],frequencyBand[i,1]))
    return all_band_power

def spectral_energy_calculation(sample, ts, cycle_quality,
                                fs=25,unacceptable=-9999,f1L=.01,f1H=.05,
                                f2L=1.8,f2H=1.85):
    """
    calculates the spectral energy of respiration cycle

    :param sample:
    :param ts:
    :param cycle_quality:
    :param fs:
    :return: energy of respiration cycle,power between .05-.2 Hz,.201-.4 Hz,
    .401-.6 Hz,.601-.8 Hz,.801-1 Hz
    """
    a = deepcopy(cycle_quality)
    for i,dp in enumerate(a):
        a[i].sample = unacceptable
    energyX=deepcopy(a);FQ_05_2_Hz=deepcopy(a);FQ_201_4_Hz=deepcopy(a)
    FQ_401_6_Hz=deepcopy(a);FQ_601_8_Hz=deepcopy(a);FQ_801_1_Hz=deepcopy(a)
    sample = return_bandPassedSignal(sample,f1L,f1H,f2L,f2H)
    ts_index = {element:i for i,element in enumerate(ts)}
    for i,dp in enumerate(cycle_quality):
        if dp.sample == Quality.UNACCEPTABLE:
            continue
        valley_ind1 = ts_index[dp.start_time.timestamp()]
        valley_ind2 = ts_index[dp.end_time.timestamp()]
        sample_temp = sample[valley_ind1:(valley_ind2+1)]
        sample_temp = sample_temp - min(sample_temp)

        Xdft=np.fft.fft(sample_temp-np.mean(sample_temp))/len(sample_temp)
        Xdft = Xdft[:np.int64(np.floor(len(sample_temp)/2+1)+1)]
        energyX[i].sample=np.sum(np.abs(Xdft)**2)/len(sample_temp)
        all_band_power = calculate_power_in_frequency_band(sample_temp,fs)
        #         print(all_band_power)
        FQ_05_2_Hz[i].sample = all_band_power[0,0]
        FQ_201_4_Hz[i].sample = all_band_power[0,1]
        FQ_401_6_Hz[i].sample = all_band_power[0,2]
        FQ_601_8_Hz[i].sample = all_band_power[0,3]
        FQ_801_1_Hz[i].sample = all_band_power[0,4]
    return energyX,FQ_05_2_Hz,FQ_201_4_Hz,FQ_401_6_Hz,FQ_601_8_Hz,FQ_801_1_Hz

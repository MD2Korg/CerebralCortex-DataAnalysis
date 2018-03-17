from scipy import signal,interpolate
from cerebralcortex.core.datatypes.datapoint import DataPoint
from enum import Enum
from scipy.signal import butter, lfilter
import scipy
import numpy as np


class Quality(Enum):
    ACCEPTABLE = 1
    UNACCEPTABLE = 0
def recover_RIP_rawWithMeasuredREF_newAuto(RIP,ref,Fs):
    m = np.mean(RIP)
    ref_synn_noM = -(ref-np.mean(ref))
    RIP_synn_noM = RIP-m
    R_36 = 10000
    R_37 = 10000
    R_40 = 604000
    G1 = R_40/(R_36+R_37)
    G_ref = -5
    b = signal.firwin(65,0.3*2/Fs,window='bartlett')
    ref_synn_noM_LP = np.convolve(np.squeeze(ref_synn_noM),b,'same')
    RIP_raw_measuredREF = (ref_synn_noM_LP/G_ref + RIP_synn_noM/G1)*G1
    return RIP_raw_measuredREF+m

def get_recovery(rip,baseline,Fs):
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
        recover_RIP_rawWithMeasuredREF_newAuto(
            [i.sample for i in rip],[i.sample for i in baseline],
            Fs)
    recovered_dp_list = \
        [DataPoint.from_tuple(start_time=rip[i].start_time,sample=recovered[i])
         for i in range(len(recovered))]
    return recovered_dp_list
def smooth(y, box_pts):
    box = np.ones(box_pts)/box_pts
    y_smooth = np.convolve(y, box, mode='same')
    return y_smooth
def smooth_detrend(sample,ts,window_size=300):
    sample = smooth(sample,13)
    sample_final = []
    initial = ts[0]
    while initial<ts[-1]:
        sample_temp = np.array(sample)[np.where((ts>=initial) &
                                                (ts<(initial+window_size)))[0]]
        sample_final.extend(list(sample_temp-np.mean(sample_temp)))
        initial+=window_size
    return np.array(sample_final)
def filter_bad_rip(ts,sample,window_size=150,filter_condition=150):
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

def get_cycle_quality(sample,ts,fs=25):
    if len(sample)>=.5*fs*(ts[-1]-ts[0]):
        return Quality.ACCEPTABLE
    return Quality.UNACCEPTABLE

def get_covariance(data1,data2):
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

def butter_bandpass(lowcut, highcut, fs, order=5):
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    b, a = butter(order, [low, high], btype='band')
    return b, a


def butter_bandpass_filter(data, lowcut, highcut, fs, order=5):
    b, a = butter_bandpass(lowcut, highcut, fs, order=order)
    y = lfilter(b, a, data)
    return y

def bandpower(x, fs, fmin, fmax):
    f, Pxx = scipy.signal.welch(x,
                                fs=fs,window=signal.get_window('hamming', len(x)),nfft=1024,return_onesided=False)
    ind_min = scipy.argmax(f > fmin) - 1
    ind_max = scipy.argmax(f > fmax) - 1
    return scipy.trapz(Pxx[ind_min: ind_max+1], f[ind_min: ind_max+1])

def return_bandPassedSignal(sample,f1L,f1H,f2L,f2H,Fs=21.33):
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
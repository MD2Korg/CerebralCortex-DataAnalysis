import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
from joblib import Parallel, delayed
from scipy import signal
from scipy.signal import find_peaks
import pickle
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler,MinMaxScaler,RobustScaler
from sklearn.neighbors import LocalOutlierFactor
from sklearn.covariance import  MinCovDet
from core.feature.stress_from_ppg.ecg import ecg_feature_computation
from sklearn.decomposition import PCA
from scipy.stats.stats import pearsonr
from scipy import signal
import warnings
from scipy.stats import skew,kurtosis
from scipy import interpolate
from datetime import datetime

from core.computefeature import get_resource_contents
warnings.filterwarnings('ignore')

STRESS_MODEL_PATH = 'core/resources/models/stress_from_ppg/stress_clf.p'


def get_pv(ppg_window_val,p=0,Fs=25,peak_percentile=30,peak_distance=.3):
    peak_loc, peak_dict = find_peaks(ppg_window_val[:,p],
                                     distance=Fs*peak_distance,
                                     height=np.percentile(ppg_window_val[:,p],peak_percentile))
    peak_indicator = [1 for o in range(len(peak_loc))]
    valley_loc, valley_dict = find_peaks(-1*ppg_window_val[:,p],
                                         distance=Fs*peak_distance,
                                         height=np.percentile(-1*ppg_window_val[:,p],peak_percentile))
    valley_indicator = [0 for o in range(len(valley_loc))]
    indicator = peak_indicator + valley_indicator
    locs = list(peak_loc) + list(valley_loc)
    heights = list(peak_dict['peak_heights']) + list(valley_dict['peak_heights'])
    channel = [p for o in range(len(locs))]
    peak_valley = np.concatenate((np.array(locs).reshape(-1,1),
                                  np.array(heights).reshape(-1,1),
                                  np.array(indicator).reshape(-1,1),
                                  np.array(channel).reshape(-1,1))
                                 ,axis=1)
    peak_valley = peak_valley[peak_valley[:,0].argsort()]
    lets_see = [np.array([o,o+1,o+2]) for o in range(0,len(locs)-3,1)]
    return peak_valley,lets_see


def get_feature_for_channel(ppg_window_val,
                            ppg_window_time,
                            window_size=10,
                            Fs=25,
                            var_threshold=.001,
                            kurtosis_threshold_high=5,
                            kurtosis_threshold_low=-1,
                            skew_threshold_low=-3,
                            skew_threshold_high=3,
                            iqr_diff=.2):
    try:
        ts_array = np.linspace(ppg_window_time[0],ppg_window_time[-1],window_size*Fs)
        interp = interpolate.interp1d(ppg_window_time,ppg_window_val,axis=0,fill_value='extrapolate')
        final_data_2 = interp(ts_array)
        final_data_2 = MinMaxScaler().fit_transform(RobustScaler().fit_transform(final_data_2))
        X = final_data_2.T
        gg = X.T
        predicted = np.array([0]*gg.shape[1])
        for i in range(len(predicted)):
            if len(np.where(np.diff(ppg_window_val[:,i])==0)[0])/ppg_window_val.shape[0] > .2:
                predicted[i] = 1
            if np.var(ppg_window_val[:,i])<var_threshold:
                predicted[i] = 1
            if kurtosis(gg[:,i])>kurtosis_threshold_high:
                predicted[i] = 1
            if kurtosis(gg[:,i])<kurtosis_threshold_low:
                predicted[i] = 1
            if not skew_threshold_low<skew(gg[:,i])<skew_threshold_high:
                predicted[i] = 1
            if np.percentile(gg[:,i],75)-np.percentile(gg[:,i],25)<iqr_diff:
                predicted[i] = 1
        return predicted,gg,ts_array
    except Exception as e:
        return np.array([1]*ppg_window_val.shape[1]),np.zeros((250,0)),ppg_window_time

def get_feature_peak_valley(ppg_window_val,ppg_window_time,Fs=25,window_size=10):
    feature_for_channel,ppg_window_val,ppg_window_time = get_feature_for_channel(ppg_window_val,
                                                                                 ppg_window_time,
                                                                                 Fs=Fs,
                                                                                 window_size=window_size)
    ppg_window_val = ppg_window_val[:,np.where(feature_for_channel==0)[0]]
    feature_final = np.zeros((0,6))
    if ppg_window_val.shape[1]==0:
        return feature_final
    if ppg_window_val.shape[1]>1:
        height_var = []
        for i in range(ppg_window_val.shape[1]):
            peak_loc, peak_dict = find_peaks(ppg_window_val[:,i], distance=Fs*.3,
                                             height=np.percentile(ppg_window_val[:,i],30))
            height_var.append(np.std(list(peak_dict['peak_heights'])))
        ppg_window_val = ppg_window_val[:,np.argmin(np.array(height_var))].reshape(-1,1)
#     plt.plot(ppg_window_val)
#     plt.show()
#     print(ppg_window_val.shape)
    for p in range(ppg_window_val.shape[1]):
        peak_valley,lets_see = get_pv(ppg_window_val,p,Fs)
        feature = []
        for ind,item in enumerate(lets_see):
            window = peak_valley[item,:]
            if len(np.unique(window[:,2]))==1 or sum(window[:,2]) not in [2,1] or \
                    len(np.unique(np.abs(np.diff(window[:,2]))))>1:
                continue
            start = np.int64(window[0,0])
            end = np.int64(window[-1,0]) + 1
            if window[1,2] == 0:
                cycle = ppg_window_val[start:end,p]*(-1)
            else:
                cycle = ppg_window_val[start:end,p]
            feature.append(np.array([ppg_window_time[np.int64(window[1,0])],
                                     np.trapz(cycle),
                                     np.std(window[:,1]),
                                     np.mean(window[:,1]),
                                     window[2,0]-window[0,0],
                                     p]))
        feature = np.array(feature)
        if len(feature)==0:
            continue
        feature_final = np.concatenate((feature_final,feature))
    return feature_final

def get_features_for_kuality(acl):
    f = []
    f.extend(list(np.var(acl[:,2:5],axis=0)))
    return f


def get_data_out(ppg_data,acl_data,
                 Fs=25,
                 window_size=10,
                 step_size=2000,
                 acl_threshold=0.042924592358051586):
    left_data =ppg_data
    acl_l = acl_data*2/16384
    ts_array = np.arange(left_data[0,0],left_data[-1,0],step_size)
    y = []
    for k in range(0,len(ts_array),1):
        t = ts_array[k]
        index_ppg = np.where((left_data[:,0]>=t-window_size*1000/2)&(left_data[:,0]<=t+window_size*1000/2))[0]
        index_acl = np.where((acl_l[:,0]>=t-window_size*1000/2)&(acl_l[:,0]<=t+window_size*1000/2))[0]
        if len(index_ppg)<.6*window_size*Fs:
            continue
        ppg_window_time = left_data[index_ppg,0]
        ppg_window_val = left_data[index_ppg,1:]
#         print(ppg_window_val.shape)
        ff = get_features_for_kuality(acl_l[index_acl,:])
        if np.max(ff)>acl_threshold:
            continue
        ppg_window_val = signal.detrend(ppg_window_val,axis=0)
        feature_final = get_feature_peak_valley(ppg_window_val,ppg_window_time,
                                                Fs=Fs,window_size=window_size)
#         print(feature_final)
        if feature_final.shape[0]<3:
            continue
        clf = LocalOutlierFactor(n_neighbors=2,contamination=.2)
        ypred = clf.fit_predict(feature_final[:,1:-1])
        y.append(np.array([t,np.median(feature_final[ypred==1,-2])*40]))
#         print(len(y))
    return np.array(y)

def get_ecg_windowss(rr_interval):
    window_col,ts_col = [],[]
    ts_array = np.arange(rr_interval[0,0],rr_interval[-1,0],60000)
    for t in ts_array:
        index = np.where((rr_interval[:,0]>=t)&(rr_interval[:,0]<=t+60000))[0]
        if len(index)<15:
            continue
        rr_temp = rr_interval[index,:]
        window_col.append(rr_temp)
        ts_col.append(t+30000)
    return window_col,ts_col
def combine_data_sobc(window_col,ts_col,clf):
    feature_matrix = []
    ts_col_final = []
    for i,item in enumerate(window_col):
        feature = ecg_feature_computation(item[:,0],item[:,1])
#         if feature[0]>2:
#             continue
        feature_matrix.append(np.array(feature).reshape(-1,11))
        ts_col_final.append(ts_col[i])
    if len(feature_matrix)>0:
        feature_matrix = np.array(feature_matrix).reshape(len(feature_matrix),11)
        stress_probs = clf.predict_proba(feature_matrix)
        stress_probs[:,0] = ts_col_final
        return stress_probs
    else:
        return np.zeros((0,2))

def get_stress_time_series(data):
    # clf = pickle.load(open('stress_clf.p','rb'))
    clf = get_resource_contents(STRESS_MODEL_PATH)
    data[:,0] = data[:,0]*1000
    data = data[:300000,:]
    if np.shape(data)[0]>100:
        ppg_data = data[:,np.array([0,2,3,4])]
        acl_data = data[:,np.array([0,0,5,6,7])]
        heart_rate = get_data_out(ppg_data,acl_data)
        heart_rate[:,1] = (heart_rate[:,1] - np.mean(heart_rate[:,1]))/np.std(heart_rate[:,1])
        window_col,ts_col = get_ecg_windowss(heart_rate)
        stress_timeseries = combine_data_sobc(window_col,ts_col,clf)
        return stress_timeseries
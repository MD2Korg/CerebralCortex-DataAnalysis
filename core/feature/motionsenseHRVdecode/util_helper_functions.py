import numpy as np
from core.feature.motionsenseHRVdecode.util_raw_byte_decode import Preprc


def get_decoded_matrix(data):
    ts = [i.start_time.timestamp() for i in data]
    sample = np.zeros((len(ts),22))
    sample[:,0] = ts;sample[:,1] = ts
    for k in range(len(ts)):
        sample[k,2:] = [np.int8(np.float(dp)) for dp in (data[k].sample.split(','))]
    ts_temp = np.array([0]+list(np.diff(ts)))
    ind = np.where(ts_temp>1)[0]
    initial = 0
    sample_final  = [0]*11
    for k in ind:
        sample_temp = Preprc(raw_data=sample[initial:k,:])
        sample_final = np.vstack((sample_final,sample_temp.values))
        initial = k
    return sample_final
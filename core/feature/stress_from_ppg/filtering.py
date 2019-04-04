import numpy as np
from scipy import signal
from core.feature.stress_from_ppg.decode import Preprc

def isDatapointsWithinRange(red,infrared,green):
    """
    :param red: ppg channel=red
    :param infrared: ppg channel = infrared
    :param green: ppg channel = green
    :return: boolean condition specifying dc values in window are within range
    """
    red = np.asarray(red, dtype=np.float32)
    infrared = np.asarray(infrared, dtype=np.float32)
    green = np.asarray(green, dtype=np.float32)
    a =  len(np.where((red >= 30000)& (red<=170000))[0]) < .5*25*3
    b = len(np.where((infrared >= 120000)& (infrared<=230000))[0]) < .5*25*3
    c = len(np.where((green >= 500)& (green<=20000))[0]) < .5*25*3
    if a and b and c:
        return False
    return True

def compute_quality(window):
    """
    :param window: a window containing the datapoints in the form of a numpy array (n*4)
    first column is timestamp, second third and fourth are the ppg channels
    :return: an integer reptresenting the status of the window 0= attached, 1 = not attached
    """
    if len(window)==0:
        return 1 #not attached
    red = window[:,0]
    infrared = window[:,1]
    green = window[:,2]
    if not isDatapointsWithinRange(red,infrared,green):
        return 1
    if np.mean(red) < 5000 and np.mean(infrared) < 5000 and np.mean(green)<500:
        return 1
    if np.mean(red)<np.mean(green) or np.mean(infrared)<np.mean(red):
        return 1
    diff = 20000
    if np.mean(red)>140000:
        diff = 10000
    if np.mean(red) - np.mean(green) < diff or np.mean(infrared) - np.mean(red)<diff:
        return 1
    if np.var(red) <1 and np.var(infrared) <1 and np.var(green)<1:
        return 1
    return 0
def get_clean_ppg(data):
    """
    :param data: a numpy array of shape n*10 .. the columns are timestamp,ppg red, ppg infrared,
    ppg green, acl x,y,z, gyro x,y,z
    :return:
    """
    start_ts = data[0,0]
    final_data = np.zeros((0,10))
    ind = np.array([1,2,3])
    while start_ts < data[-1,0]:
        index = np.where((data[:,0]>=start_ts)&(data[:,0]<start_ts+3000))[0]
        temp_data = data[index,:]
        temp_data = temp_data[:,ind]
        if compute_quality(temp_data)==0:
            final_data = np.concatenate((final_data,data[index,:]))
        start_ts = start_ts + 3000
    return final_data

def preProcessing(data,Fs=25,fil_type='ppg'):
    '''
    Inputs
    data: a numpy array of shape n*10 .. the columns are timestamp,ppg red, ppg infrared,
    ppg green, acl x,y,z, gyro x,y,z
    Fs: sampling rate
    fil_type: ppg or ecg
    Output X2: preprocessed signal data
    preprocessing the data by filtering
    '''
    if data.shape[0]<165:
        return np.zeros((0,data.shape[1]))

    X0 = data[:,1:4]
    X1 = signal.detrend(X0,axis=0,type='constant')
    # if fil_type in ['ppg']:
    b = signal.firls(65,np.array([0,0.3, 0.4, 2 ,2.5,Fs/2]),np.array([0, 0 ,1 ,1 ,0, 0]),
                     np.array([100*0.02,0.02,0.02]),fs=Fs)
    X2 = np.zeros((np.shape(X1)[0]-len(b)+1,data.shape[1]))
    for i in range(X2.shape[1]):
        if i in [1,2,3]:
            X2[:,i] = signal.convolve(X1[:,i-1],b,mode='valid')
        else:
            X2[:,i] = data[64:,i]

    return X2



def get_filtered_data(data):
    """
    data: a numpy array of shape n*10 .. the columns are timestamp,ppg red, ppg infrared,
    ppg green, acl x,y,z, gyro x,y,z
    :return: bandpass filtered data of the time when motionsense was attached to wrist
    """
    data = get_clean_ppg(data)
    final_data = preProcessing(data)
    return final_data

def get_decoded_matrix(data: np.ndarray, row_length=22):
    """
    given the raw byte array containing lists it returns the decoded values
    :param row_length:
    :param data: input matrix(*,22) containing raw bytes
    :return: a matrix each row of which contains consecutively sequence
    number,acclx,accly,acclz,gyrox,gyroy,gyroz,red,infrared,green leds,
    timestamp
    """
    if len(data)<1:
        return np.zeros((0,10))
    ind_acl = np.array([10,7,8,9,1,2,3,4,5,6])
    ts = data[:,0]
    sample = np.zeros((len(ts), row_length))
    sample = data
    ts_temp = np.array([0] + list(np.diff(ts)))
    ind = np.where(ts_temp > 1000)[0]
    initial = 0
    sample_final = [0] * int(row_length / 2)
    for k in ind:
        sample_temp = Preprc(raw_data=sample[initial:k, :])
        initial = k
        if not list(sample_temp):
            continue
        sample_final = np.vstack((sample_final, sample_temp.values))
        # print(sample_final.shape)
    sample_temp = Preprc(raw_data=sample[initial:, :])
    if np.shape(sample_temp)[0] > 0:
        sample_final = np.vstack((sample_final, sample_temp.values))
    if np.shape(sample_final)[0] == 1:
        return np.zeros((0,10))
    return sample_final[1:,ind_acl]


def decode_and_filter(data):
    """
    data: a numpy array of shape n*22 ..this is the raw byte array from the raw stream
    :return: bandpass filtered data of the time when motionsense was attached to wrist
    """
    data = get_decoded_matrix(data)
    # import matplotlib.pyplot as plt
    # plt.figure(figsize=(16,8))
    # plt.plot(data[:,0],data[:,1:4])
    # plt.show()
    final_data = get_filtered_data(data)
    return final_data

def get_realigned_data(data):
    """
    Here filename is the name of the csv file that contains the datastream
    "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV_PLUS--LEFT_WRIST"
                                    or
    "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV_PLUS--RIGHT_WRIST"
                                    or
    "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
                                    or
    "RAW--CHARACTERISTIC_LED--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
                                    or
    "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
                                    or
    "RAW--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
    """
    #     data = pd.read_csv(filename,sep=',',columns=None).values
    if len(data)>0:
        offset = data[0,1]
        data = decode_and_filter(data)
        if len(data)>0:
            final_data = np.zeros((data.shape[0],data.shape[1]+1))
            final_data[:,0] = data[:,0]
            final_data[:,2:] = data[:,1:]
            final_data[:,1] = [offset]*data.shape[0]
        else:
            final_data = np.zeros((0,11))
    else:
        final_data = np.zeros((0,11))
    return final_data
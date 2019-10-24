import numpy as np
import math
from scipy.stats import skew,kurtosis
import pickle
from sklearn.ensemble import RandomForestClassifier
from core.computefeature import get_resource_contents
MODEL_FILENAME = 'core/resources/models/dataquality/classifier.p'

def get_model() -> RandomForestClassifier:
    """

    :rtype: object
    :param bool is_gravity:
    :return:
    """
    model_file_contents = get_resource_contents(MODEL_FILENAME)
    clf = pickle.loads(model_file_contents)
    return clf




def get_rate_of_change(timestamp: object, value: object) -> object:
    """
    :rtype: object
    :param timestamp:
    :param value:
    :return:
    """
    roc = 0
    cnt = 0
    for i in range(len(value) - 1):
        if (timestamp[i + 1] - timestamp[i])/1000 > 0:
            roc = roc + (((value[i + 1] - value[i]) / (
                    timestamp[i + 1] - timestamp[i])/1000))
            cnt = cnt + 1
    if cnt > 0:
        roc = roc / cnt

    return roc


def get_magnitude(ax: object, ay: object, az: object) -> object:
    """
    :rtype: object
    :param ax:
    :param ay:
    :param az:
    :return:
    """
    return math.sqrt(ax * ax + ay * ay + az * az)


def spectral_entropy(data, sampling_freq, bands=None):
    """
    Compute spectral entropy of a  signal with respect to frequency bands.
    The power spectrum is computed through fft. Then, it is normalised and
    assimilated to a probability density function.
    The entropy of the signal :math:`x` can be expressed by:
    .. math::
        H(x) =  -\sum_{f=0}^{f = f_s/2} PSD(f) log_2[PSD(f)]
    Where:
    :math:`PSD` is the normalised power spectrum (Power Spectrum Density), and
    :math:`f_s` is the sampling frequency
    :param data: a one dimensional floating-point array representing a time series.
    :type data: :class:`~numpy.ndarray` or :class:`~pyrem.time_series.Signal`
    :param sampling_freq: the sampling frequency
    :type sampling_freq:  float
    :param bands: a list of numbers delimiting the bins of the frequency bands.
    If None the entropy is computed over the whole range of the DFT
    (from 0 to :math:`f_s/2`)
    :return: the spectral entropy; a scalar
    """
    psd = np.abs(np.fft.rfft(data)) ** 2
    psd /= np.sum(psd)  # psd as a pdf (normalised to one)

    if bands is None:
        power_per_band = psd[psd > 0]
    else:
        freqs = np.fft.rfftfreq(data.size, 1 / float(sampling_freq))
        bands = np.asarray(bands)

        freq_limits_low = np.concatenate([[0.0], bands])
        freq_limits_up = np.concatenate([bands, [np.Inf]])

        power_per_band = [np.sum(psd[np.bitwise_and(freqs >= low, freqs < up)])
                          for low, up in zip(freq_limits_low, freq_limits_up)]

        power_per_band = power_per_band[power_per_band > 0]

    return - np.sum(power_per_band * np.log2(power_per_band))


def peak_frequency(data: object) -> object:
    """
    :rtype: object
    :param data:
    :return:
    """
    w = np.fft.fft(data)
    freqs = np.fft.fftfreq(len(w))
    return freqs.max()


def compute_basic_features(timestamp: object, data: object) -> object:
    """
    :rtype: object
    :param timestamp:
    :param data:
    :return:
    """
    mean = np.mean(data)
    median = np.median(data)
    std = np.std(data)
    skewness = skew(data)
    kurt = kurtosis(data)
    rate_of_changes = get_rate_of_change(timestamp, data)
    power = np.mean([v * v for v in data])
    sp_entropy = spectral_entropy(data,16.33)
    peak_freq = peak_frequency(data)

    return mean, median, std, skewness, kurt, rate_of_changes, power, sp_entropy, peak_freq


def computeFeatures(time: object, x: object, y: object, z: object) -> object:
    """
    :rtype: object
    :param start_time:
    :param end_time:
    :param time:
    :param x:
    :param y:
    :param z:
    :param pid:
    :return:
    """
    mag = [0] * len(x)  # np.empty([len(x), 1])
    for i, value in enumerate(x):
        mag[i] = math.sqrt(x[i] * x[i] + y[i] * y[i] + z[i] * z[i])

    mag_mean, mag_median, mag_std, mag_skewness, mag_kurt, mag_rateOfChanges, \
    mag_power, mag_sp_entropy, mag_peak_freq = compute_basic_features(time, mag)
    x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power, \
    x_sp_entropy, x_peak_freq = compute_basic_features(time, x)
    y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power, \
    y_sp_entropy, y_peak_freq = compute_basic_features(time, y)
    z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power, \
    z_sp_entropy, z_peak_freq = compute_basic_features(time, z)

    f = [mag_mean, mag_median, mag_std, mag_skewness,
         mag_kurt, mag_rateOfChanges, mag_power,
         mag_sp_entropy, mag_peak_freq]

    f.extend(
        [x_mean, x_median, x_std, x_skewness, x_kurt, x_rateOfChanges, x_power,
         x_sp_entropy, x_peak_freq])
    f.extend(
        [y_mean, y_median, y_std, y_skewness, y_kurt, y_rateOfChanges, y_power,
         y_sp_entropy, y_peak_freq])
    f.extend(
        [z_mean, z_median, z_std, z_skewness, z_kurt, z_rateOfChanges, z_power,
         z_sp_entropy, z_peak_freq])

    return f
def get_features(window):
    tmp = computeFeatures(window[:,0], window[:,1], window[:,2], window[:,3])
    return tmp
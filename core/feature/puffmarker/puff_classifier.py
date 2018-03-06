import pickle

from sklearn.ensemble import RandomForestClassifier

from cerebralcortex.core.datatypes.datastream import DataPoint
from core.feature.puffmarker.PUFFMARKER_CONSTANTS import *


def get_posture_model() -> RandomForestClassifier:
    clf = pickle.load(open(PUFFMARKER_MODEL_FILENAME, 'rb'))
    return clf


def classify_puffs(features):
    clf = get_posture_model()
    labels = []
    for dp in features:
        predicted_label = clf.predict([dp.sample])
        labels.append(
            DataPoint(start_time=dp.start_time, offset=dp.offset,
                      end_time=dp.end_time, sample=predicted_label))

    return labels

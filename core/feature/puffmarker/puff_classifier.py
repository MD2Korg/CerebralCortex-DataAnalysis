import pickle

from sklearn.ensemble import RandomForestClassifier
from core.feature.puffmarker.utils import *


def get_posture_model() -> RandomForestClassifier:
    clf = pickle.load(open(PUFFMARKER_MODEL_FILENAME, 'rb'))
    return clf


def classify_puffs(features):
    clf = get_posture_model()
    X = [dp.sample for dp in features]
    predicted_labels = clf.predict(X)
    labels = [DataPoint(start_time=dp.start_time,
                        offset=dp.offset,
                        end_time=dp.end_time,
                        sample=int(str(predicted_labels[0])))
              for i, dp in features]

    return labels

import pickle

from sklearn.ensemble import RandomForestClassifier
from core.feature.puffmarker.utils import *
from core.computefeature import get_resource_contents


def get_posture_model() -> RandomForestClassifier:
    clf = pickle.loads(get_resource_contents(PUFFMARKER_MODEL_FILENAME))
    return clf


def classify_puffs(features):
    clf = get_posture_model()
    X = [dp.sample for dp in features]
    predicted_labels = clf.predict(X)
    labels = [DataPoint(start_time=dp.start_time,
                        offset=dp.offset,
                        end_time=dp.end_time,
                        sample=int(str(predicted_labels[i])))
              for i, dp in enumerate(features)]

    return labels

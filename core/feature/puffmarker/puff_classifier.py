import pickle

from sklearn.ensemble import RandomForestClassifier

from cerebralcortex.core.datatypes.datastream import DataPoint

puffmarker_model_filename = 'core/feature/puffmarker/model_file/puffmarker_wrist_randomforest.model'

def get_posture_model() -> RandomForestClassifier:
    clf = pickle.load(open(puffmarker_model_filename, 'rb'))
    return clf

def classify_puffs(features):
    clf = get_posture_model()
    labels = []
    for dp in features:
        predicted_label=clf.predict([dp.sample])
        labels.append(DataPoint(start_time=dp.start_time, offset=dp.offset, end_time=dp.end_time, sample=predicted_label))

    return labels



import pickle
from sklearn.ensemble import RandomForestClassifier

posture_model_filename = './models/posture_randomforest.model'
activity_model_filename = './models/activity_level_randomforest.model'

def get_posture_model() -> RandomForestClassifier:
    clf = pickle.load(open(posture_model_filename, 'rb'))
    return clf

def get_activity_model():
    clf = pickle.load(open(posture_model_filename, 'rb'))
    return clf


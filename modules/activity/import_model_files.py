import pickle
from sklearn.ensemble import RandomForestClassifier


posture_model_filename = 'random_forest_posture.model'
activity_model_filename = 'random_forest_activity.model'

def get_posture_model() -> RandomForestClassifier:
    clf = pickle.load(open(posture_model_filename, 'rb'))
    return clf

def get_activity_model():
    clf = pickle.load(open(posture_model_filename, 'rb'))
    return clf


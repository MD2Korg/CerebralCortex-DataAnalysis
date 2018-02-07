from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint
from core.markers.activity.import_model_files import get_posture_model, get_activity_model


def classify_posture(features: DataStream) -> DataStream:

    clf = get_posture_model()
    labels = []
    for dp in features.data:
        preds=clf.predict([dp.sample])
        labels.append(DataPoint(start_time=dp.start_time, end_time=dp.end_time, sample=preds))

    label_stream = DataStream.from_datastream([features])
    label_stream.data = labels

    return label_stream

def classify_activity(features: DataStream) -> DataStream:

    clf = get_activity_model()
    labels = []
    for dp in features.data:
        preds=clf.predict([dp.sample])
        labels.append(DataPoint(start_time=dp.start_time, end_time=dp.end_time, sample=preds))

    label_stream = DataStream.from_datastream([features])
    label_stream.data = labels

    return label_stream






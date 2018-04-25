# Copyright (c) 2018, MD2K Center of Excellence
# - Sayma Akther <sakther@memphis.edu>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from cerebralcortex.core.datatypes.datastream import DataPoint
from core.feature.activity.import_model_files import get_posture_model, get_activity_model
from typing import List


def _classify(clf, features):
    """ Classifier helper method

    Args:
        clf: ML model
        features: A set of features to run posture classification on

    Returns:
        List[DataPoints]: Labeled events
    """
    labels = []
    prediction_values = [dp.sample for dp in features]
    predictions = clf.predict(prediction_values)
    for i, dp in enumerate(features):
        labels.append(DataPoint(start_time=dp.start_time, end_time=dp.end_time,
                                offset=dp.offset, sample=str(predictions[i])))
    return labels


def classify_posture(features: List[DataPoint], is_gravity: bool) -> List[DataPoint]:
    """Classify posture from a set of input features based on a predefined ML model.

    Args:
        features: A set of features to run posture classification on
        is_gravity: Flag to account for gravity or not

    Returns:
        List[DataPoints]: Labeled postures
    """

    clf = get_posture_model(is_gravity)
    return _classify(clf, features)


def classify_activity(features: List[DataPoint], is_gravity: bool) -> List[DataPoint]:
    """Classify activity from a set of input features based on a predefined ML model.

    Args:
        features: A set of features to run activity classification on
        is_gravity: Flag to account for gravity or not

    Returns:
        List[DataPoints]: Labeled activities

    """

    clf = get_activity_model(is_gravity)
    return _classify(clf, features)

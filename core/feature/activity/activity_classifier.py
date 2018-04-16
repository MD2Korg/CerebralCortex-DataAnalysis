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
from core.feature.activity.import_model_files import get_posture_model, \
    get_activity_model
from typing import List


def classify_posture(features: List[DataPoint], is_gravity: bool) -> List[DataPoint]:
    """
    Classify posture from a set of input features based on a predefined ML model.

    :type is_gravity: bool
    :type features: List[DataPoint]
    :rtype: List[DataPoint]
    :param features: A set of features to run posture classification on
    :param is_gravity: Flag to account for gravity or not
    :return: Labeled postures
    """
    clf = get_posture_model(is_gravity)
    labels = []

    prediction_values = [dp.sample for dp in features]
    preds = clf.predict(prediction_values)
    for i, dp in enumerate(features):
        labels.append(DataPoint(start_time=dp.start_time, end_time=dp.end_time,
                                offset=dp.offset, sample=str(preds[i])))

    return labels


def classify_activity(features: List[DataPoint], is_gravity) -> List[DataPoint]:
    """
    Classify activity from a set of input features based on a predefined ML model.

    :type is_gravity: bool
    :type features: List[DataPoint]
    :rtype: List[DataPoint]
    :param features: A set of features to run activity classification on
    :param is_gravity: Flag to account for gravity or not
    :return: Labeled activities
    """
    clf = get_activity_model(is_gravity)
    labels = []

    prediction_values = [dp.sample for dp in features]
    preds = clf.predict(prediction_values)
    for i, dp in enumerate(features):
        labels.append(DataPoint(start_time=dp.start_time, end_time=dp.end_time,
                                offset=dp.offset, sample=str(preds[i])))

    return labels

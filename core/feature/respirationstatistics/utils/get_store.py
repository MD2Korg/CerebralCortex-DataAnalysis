# Copyright (c) 2018, MD2K Center of Excellence
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

import os
import json
import uuid
from datetime import timedelta
from typing import List
from cerebralcortex.cerebralcortex import CerebralCortex

def get_stream_days(stream_id: uuid, CC: CerebralCortex) -> List:
    """
    Returns a list of days (string format: YearMonthDay (e.g., 20171206)
    :param stream_id:
    """
    stream_dicts = CC.get_stream_duration(stream_id)
    stream_days = []
    days = stream_dicts["end_time"]-stream_dicts["start_time"]
    for day in range(days.days+1):
        stream_days.append(
            (stream_dicts["start_time"]+timedelta(days=day)).strftime('%Y%m%d'))
    return stream_days

def store_data(filepath, input_streams, user_id, data, instance):
    output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(filepath +
                                                              user_id+
                                                              "RESPIRATION"+
                                                              " STATISTICS")))
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    newfilepath = os.path.join(cur_dir,filepath)
    with open(newfilepath,"r") as f:
        metadata = f.read()
        metadata = metadata.replace("CC_INPUT_STREAM_ID_CC",
                                    input_streams[0]["identifier"])
        metadata = metadata.replace("CC_INPUT_STREAM_NAME_CC",
                                    input_streams[0]["name"])
        metadata = metadata.replace("CC_OUTPUT_STREAM_IDENTIFIER_CC",
                                    output_stream_id)
        metadata = metadata.replace("CC_OWNER_CC",
                                    user_id)
        metadata = json.loads(metadata)
        if len(data) > 0:
            instance.store(identifier=output_stream_id, owner=user_id,
                           name=metadata["name"],
                           data_descriptor=metadata["data_descriptor"],
                           execution_context=metadata["execution_context"],
                           annotations=metadata["annotations"],
                           stream_type="datastream", data=data)
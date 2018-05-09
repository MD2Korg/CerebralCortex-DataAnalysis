# Copyright (c) 2018, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
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

from datetime import datetime, timedelta


def filter_data(data: list, start_time: datetime, end_time: datetime) -> list:
    """
    Filter data points based on start and end time
    :param data:
    :param start_time:
    :param end_time:
    :return:
    """
    subset_data = []
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                subset_data.append(dp)
    return subset_data


def get_home_work_location(data: list, start_time: datetime, end_time: datetime) -> str:
    """
    Get location (work/home) between start and end time duration
    :param data:
    :param start_time:
    :param end_time:
    :return:
    """
    subset_data = []
    val = "undefined"
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                subset_data.append(dp.sample)
    if len(subset_data) > 0:
        val = max(set(subset_data), key=subset_data.count)
    return val


def get_places(data: list, start_time: datetime, end_time: datetime) -> list:
    """
    Filter data points based on start and end time
    :param data:
    :param start_time:
    :param end_time:
    :return:
    """
    subset_data = []
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                subset_data.append(dp.sample)
    return subset_data


def get_phone_physical_activity_data(data: list, start_time: datetime, end_time: datetime) -> int:
    """
    Get a user's physical activity between start and end time
    :param data:
    :param start_time:
    :param end_time:
    :return:
    """
    sample_val = []
    val = 0
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                sample_val.append(dp.sample[0])
        if len(sample_val) > 0:
            val = round(sum(sample_val) / float(len(sample_val)))

    return val


def is_talking(data: list, start_time: datetime, end_time: datetime) -> bool:
    """
    Get whether a user was talking between start/end time provided
    :param data:
    :param start_time:
    :param end_time:
    :return:
    """
    sample_val = []
    val = 0
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                sample_val.append(dp.sample)
        if len(sample_val) > 0:
            val = round(sum(sample_val) / float(len(sample_val)))
        if val > 0:
            return True
        else:
            return False
    return False


def is_on_sms(data: list, start_time: datetime, end_time: datetime) -> bool:
    """
    Get whether a user was busy with sms durting start/end time
    :param data:
    :param start_time:
    :param end_time:
    :return:
    """
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                return True
    return False


def is_on_phone(data: list, start_time: datetime, end_time: datetime) -> bool:
    """
    Get whether a user was on phone durting start/end time
    :param data:
    :param start_time:
    :param end_time:
    :return:
    """
    try:
        if len(data) > 0:
            for dp in data:
                dp_start_time = dp.start_time
                if dp.sample is not None:
                    if isinstance(dp.sample, list):
                        sample = dp.sample[0]
                    else:
                        sample = dp.sample
                    dp_end_time = dp_start_time + timedelta(minutes=sample)
                if in_time_range(dp_start_time, dp_end_time, start_time, end_time):
                    return True
    except:
        pass
    return False


def is_on_social_app(data: list, start_time: datetime, end_time: datetime) -> bool:
    """
    Get whether a user was busy on social apps durting start/end time
    :param data:
    :param start_time:
    :param end_time:
    :return:
    """
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                if dp.sample == "Social":
                    return True
    return False


def get_physical_activity_wrist_sensor(data: list, start_time: datetime, end_time: datetime) -> str:
    """
    Get a user's activity based on wrist sensor during start/end time
    :param data:
    :param start_time:
    :param end_time:
    :return:
    """
    subset_data = []
    val = 0
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                subset_data.append((str(dp.sample).lower()))
        if len(subset_data) > 0:
            val = max(set(subset_data), key=subset_data.count)
    return val


def in_time_range(dp_start_time: datetime, dp_end_time: datetime, start_time: datetime, end_time: datetime) -> bool:
    """
    Check whether datapoint's start/end time is in range of qualtrics start/end time
    :param dp_start_time:
    :param dp_end_time:
    :param start_time:
    :param end_time:
    :return:
    """
    if dp_start_time is None or dp_end_time is None or start_time is None or end_time is None:
        return False
    elif (dp_start_time <= start_time and start_time <= dp_end_time) or (
            dp_start_time <= end_time and end_time <= dp_end_time):
        return True
    else:
        False


def get_input_streams(stream: dict) -> list:
    """
    Generate input stream list
    :param stream:
    :return:
    """
    input_stream_ids = stream.get("stream_ids", [])
    stream_name = stream.get("stream_name", "")
    input_streams = []
    for id in input_stream_ids:
        input_streams.append({"name": stream_name, "identifier": id["identifier"]})

    return input_streams

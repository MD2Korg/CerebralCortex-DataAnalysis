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


import uuid
import argparse

from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.util.spark_helper import get_or_create_sc
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from core.markers.activity.activity_marker import do_activity_marker
from core.markers.activity.util import *

def all_users_data(study_name: str, CC:CerebralCortex, spark_context):
    """
    Process all participants' streams
    :param study_name:
    """
    # get all participants' name-ids
    all_users = CC.get_all_users(study_name)

    if all_users:
        for user in all_users:
            process_streams(user["identifier"], CC)
    else:
        print(study_name, "- study has 0 users.")

def process_streams(user_id: uuid, CC: CerebralCortex):
    """
    Contains pipeline execution of all the diagnosis algorithms
    :param user_id:
    :param CC:
    :param config:
    """
    motionsense_hrv_accel_right = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
    motionsense_hrv_accel_left = "ACCELEROMETER--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"
    motionsense_hrv_gyro_right = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
    motionsense_hrv_gyro_left = "GYROSCOPE--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"

    # get all the streams belong to a participant
    streams = CC.get_user_streams(user_id)

    stream_days = get_stream_days(streams[motionsense_hrv_gyro_right]["identifier"], CC)
    for day in stream_days:

        accel_stream_left = CC.get_stream(streams[motionsense_hrv_accel_left]["identifier"], day, data_type=DataSet.COMPLETE)
        gyro_stream_left = CC.get_stream(streams[motionsense_hrv_gyro_left]["identifier"], day, data_type=DataSet.COMPLETE)

        accel_stream_right = CC.get_stream(streams[motionsense_hrv_accel_right]["identifier"], day, data_type=DataSet.COMPLETE)
        gyro_stream_right = CC.get_stream(streams[motionsense_hrv_gyro_right]["identifier"], day, data_type=DataSet.COMPLETE)

        # Calling puffmarker algorithm to get smoking episodes
        posture_labels, activity_label = do_activity_marker(accel_stream_right, gyro_stream_right)

if __name__ == '__main__':
    # create and load CerebralCortex object and configs
    parser = argparse.ArgumentParser(description='CerebralCortex Kafka Message Handler.')
    parser.add_argument("-cc", "--cc_config_filepath", help="Configuration file path", required=True)
    args = vars(parser.parse_args())

    CC = CerebralCortex(args["cc_config_filepath"])

    # get/create spark context
    spark_context = get_or_create_sc(type="sparkContext")

    # run for all the participants in a study
    all_users_data("mperf", CC, spark_context)


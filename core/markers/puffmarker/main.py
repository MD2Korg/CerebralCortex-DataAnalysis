# Copyright (c) 2018, MD2K Center of Excellence
# - Nazir Saleheen <nazir.saleheen@gmail.com>
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
from cerebralcortex.core.config_manager.config import Configuration
from core.markers.puffmarker.puffmarker_wrist import process_puffmarker
from core.markers.puffmarker.util import get_stream_days
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet

def all_users_data(study_name: str, md_config, CC, spark_context):
    """
    Process all participants' streams
    :param study_name:
    """
    # get all participants' name-ids
    all_users = CC.get_all_users(study_name)

    if all_users:
        for user in all_users:
            process_streams(user["identifier"], CC, md_config)
    else:
        print(study_name, "- study has 0 users.")

def process_streams(user_id: uuid, CC: CerebralCortex, config: dict):
    """
    Contains pipeline execution of all the diagnosis algorithms
    :param user_id:
    :param CC:
    :param config:
    """
    # get all the streams belong to a participant
    streams = CC.get_user_streams(user_id)

    stream_days = get_stream_days(streams[config["stream_names"]["motionsense_hrv_accel_left"]]["identifier"], CC)
    for day in stream_days:

        accel_stream_left = CC.get_stream(streams[config["stream_names"]["motionsense_hrv_accel_left"]]["identifier"], day, data_type=DataSet.COMPLETE)
        gyro_stream_left = CC.get_stream(streams[config["stream_names"]["motionsense_hrv_gyro_left"]]["identifier"], day, data_type=DataSet.COMPLETE)

        accel_stream_right = CC.get_stream(streams[config["stream_names"]["motionsense_hrv_accel_left"]]["identifier"], day, data_type=DataSet.COMPLETE)
        gyro_stream_right = CC.get_stream(streams[config["stream_names"]["motionsense_hrv_gyro_left"]]["identifier"], day, data_type=DataSet.COMPLETE)

        # Calling puffmarker algorithm to get smoking episodes
        process_puffmarker(user_id, CC, config, accel_stream_left, gyro_stream_left, accel_stream_right, gyro_stream_right)


if __name__ == '__main__':
    # create and load CerebralCortex object and configs
    parser = argparse.ArgumentParser(description='CerebralCortex Kafka Message Handler.')
    parser.add_argument("-cc", "--cc_config_filepath", help="Configuration file path", required=True)
    parser.add_argument("-pmc", "--puffmarker_wrist_config_filepath", help="puffmarker wrist configuration file path", required=True)
    args = vars(parser.parse_args())

    CC = CerebralCortex(args["cc_config_filepath"])

    # load data diagnostic configs
    md_config = Configuration(args["puffmarker_wrist_config_filepath"]).config

    # get/create spark context
    spark_context = get_or_create_sc(type="sparkContext")

    # run for one participant
    # one_user_data(["cd7c2cd6-d0a3-4680-9ba2-0c59d0d0c684"], md_config, CC, spark_context)

    # run for all the participants in a study
    all_users_data("mperf", md_config, CC, spark_context)




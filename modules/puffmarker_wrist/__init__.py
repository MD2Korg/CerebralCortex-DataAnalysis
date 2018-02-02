import uuid
import argparse

from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.util.spark_helper import get_or_create_sc
from cerebralcortex.core.config_manager.config import Configuration
from modules.puffmarker_wrist.wrist_features import compute_wrist_feature
from modules.puffmarker_wrist.puffmarker_wrist import process_puffmarker

def all_users_data(study_name: str, md_config, CC, spark_context):
    """
    Process all participants' streams
    :param study_name:
    """
    # get all participants' name-ids
    all_users = CC.get_all_users(study_name)

    if all_users:
        rdd = spark_context.parallelize(all_users)
        results = rdd.map(
            lambda user: process_streams(user["identifier"], CC, md_config))
        results.count()
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

    accel_stream_left = CC.get_stream(streams[config["stream_names"]["motionsense_hrv_accel_left"]]["identifier"])
    gyro_stream_left = CC.get_stream(streams[config["stream_names"]["motionsense_hrv_gyro_left"]]["identifier"])

    accel_stream_right = CC.get_stream(streams[config["stream_names"]["motionsense_hrv_accel_left"]]["identifier"])
    gyro_stream_right = CC.get_stream(streams[config["stream_names"]["motionsense_hrv_gyro_left"]]["identifier"])

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




#!/usr/bin/env bash

CC_CONFIG_PATH='/home/vagrant/CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml'

DATA_DIR='/home/vagrant/phone_features'

METADATA_DIR='/home/vagrant/CerebralCortex-DataAnalysis/core/feature/phone_features/metadata'

python3.6 hdfs_to_cassandra.py -c $CC_CONFIG_PATH -d $DATA_DIR -m $METADATA_DIR

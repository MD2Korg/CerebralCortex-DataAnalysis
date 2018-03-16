#!/usr/bin/env bash

CC_CONFIG_PATH='/home/vagrant/CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml'

DATA_DIR='/home/vagrant/mperf_data'

UUID_MAPPING='/home/vagrant/mperf_ids.txt'

python3.6 qualtrix_data_importer.py -c $CC_CONFIG_PATH -d $DATA_DIR -u $UUID_MAPPING

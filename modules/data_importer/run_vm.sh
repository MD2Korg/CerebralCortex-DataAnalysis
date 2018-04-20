#!/usr/bin/env bash

CC_CONFIG_PATH='/home/vagrant/CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml'

DATA_DIR='/home/vagrant/20180403/'

UUID_MAPPING='/home/vagrant/mperf_ids.txt'

export PYTHONPATH='':$PYTHONPATH


python3.6 qualtrix_data_importer.py -c $CC_CONFIG_PATH -d $DATA_DIR -u $UUID_MAPPING

#echo 'Processing IGTB'
python3.6 igtb_qualtrix_data_importer.py -c $CC_CONFIG_PATH -d $DATA_DIR -u $UUID_MAPPING

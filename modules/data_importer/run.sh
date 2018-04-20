#!/usr/bin/env bash

CC_CONFIG_PATH='/cerebralcortex/code/config/cc_starwars_configuration.yml'

DATA_DIR='/cerebralcortex/code/anand/qualtrics/20180403/'

UUID_MAPPING='/cerebralcortex/code/anand/qualtrics/mperf_ids.txt'

export PYTHONPATH='/cerebralcortex/code/eggs/MD2K_Cerebral_Cortex-2.0.0-py3.6.egg':$PYTHONPATH


python3.6 qualtrix_data_importer.py -c $CC_CONFIG_PATH -d $DATA_DIR -u $UUID_MAPPING

#echo 'Processing IGTB'
python3.6 igtb_qualtrix_data_importer.py -c $CC_CONFIG_PATH -d $DATA_DIR -u $UUID_MAPPING

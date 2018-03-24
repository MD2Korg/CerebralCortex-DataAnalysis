#!/usr/bin/env bash

DATA_ANALYSIS_PATH='/cerebralcortex/code/anand/CerebralCortex-DataAnalysis'
# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6

#Spark path
export SPARK_HOME=/usr/local/spark/

# path of cc configuration path
CC_CONFIG_FILEPATH="/cerebralcortex/code/config/cc_starwars_configuration.yml"

# list of features to process, leave blank to process all features
FEATURES="beacon"

# study name
STUDY_NAME="mperf"

# start date
START_DATE="20171103"

# end date
END_DATE="20171111"

# Folder containing the metadata templates for the features
FEATURE_METADATA_DIR=$DATA_ANALYSIS_PATH'/core/resources/metadata'

# list of usersids separated by comma. Leave blank to process all users.
USERIDS="a28665d0-c10f-4f3e-946a-2b37b8799621"

SPARK_MASTER="spark://dagobah10dot.memphis.edu:7077"

PY_FILES="/cerebralcortex/code/anand/CerebralCortex/dist/MD2K_Cerebral_Cortex-2.0.0-py3.6.egg,dist/MD2K_Cerebral_Cortex_DataAnalysis_compute_features-2.2.1-py3.6.egg"

SPARK_UI_PORT=4066

MAX_CORES=8

# set to True to make use of spark parallel execution
SPARK_JOB="False"

if [ $SPARK_JOB == 'True' ]
    then
        echo 'Executing Spark job'
        spark-submit --master $SPARK_MASTER \
                     --conf spark.ui.port=$SPARK_UI_PORT \
                     --conf spark.cores.max=$MAX_CORES \
                     --py-files $PY_FILES \
                     core/driver.py -c $CC_CONFIG_FILEPATH \
                     -s $STUDY_NAME -sd $START_DATE \
                     -ed $END_DATE -u $USERIDS -f $FEATURES \
                     -m $FEATURE_METADATA_DIR \
                     -p $MAX_CORES
    else
        echo 'Executing single threaded'
        export PYTHONPATH=.:$PYTHONPATH
        python3.6 core/driver.py -c $CC_CONFIG_FILEPATH \
                       -s $STUDY_NAME -sd $START_DATE \
                       -ed $END_DATE -u $USERIDS -f $FEATURES \

fi


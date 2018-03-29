#!/usr/bin/env bash

#DATA_ANALYSIS_PATH='/cerebralcortex/code/anand/CerebralCortex-DataAnalysis'
# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6

#Spark path
export SPARK_HOME=/usr/local/spark/

#set spark home
#export PATH=$SPARK_HOME/bin:$PATH

# setting of PYTHONPATH
export PYTHONPATH=$PYTHONPATH:'/cerebralcortex/code/CerebralCortex/'
export PYTHONPATH=$PYTHONPATH:$DATA_ANALYSIS_PATH
export PYTHONPATH=$PYTHONPATH:$DATA_ANALYSIS_PATH'/core/feature/activity/'
export PYTHONPATH=$PYTHONPATH:$DATA_ANALYSIS_PATH'/core/feature/phone_features/'
export PYTHONPATH=$PYTHONPATH:$DATA_ANALYSIS_PATH'/core/feature/stress_from_respiration/'
export PYTHONPATH=$PYTHONPATH:$DATA_ANALYSIS_PATH'/core/feature/motionsenseHRVdecode/'
export PYTHONPATH=$PYTHONPATH:$DATA_ANALYSIS_PATH'/core/feature/respiration_cycle_statistics/'
export PYTHONPATH=$PYTHONPATH:$DATA_ANALYSIS_PATH'/core/signalprocessing/'
export PYTHONPATH=$PYTHONPATH:$DATA_ANALYSIS_PATH'/core/signalprocessing/gravity_filter/'

# path of cc configuration path
CC_CONFIG_FILEPATH="/cerebralcortex/code/config/cc_starwars_configuration.yml"

# list of features to process, leave blank to process all features
FEATURES="respiration_cycle_statistics"

# study name
STUDY_NAME="mperf"

# start date
START_DATE="20171001"

# end date
END_DATE="20180201"

# Folder containing the metadata templates for the features
FEATURE_METADATA_DIR=$DATA_ANALYSIS_PATH'/core/resources/metadata'

# list of usersids separated by comma. Leave blank to process all users.
USERIDS=""

SPARK_MASTER="spark://dagobah10dot.memphis.edu:7077"

# set to True to make use of spark parallel execution
SPARK_JOB="True"

PY_FILES="/cerebralcortex/code/CerebralCortex/dist/MD2K_Cerebral_Cortex-2.2.2-py3.6.egg,dist/MD2K_Cerebral_Cortex_DataAnalysis_compute_features-2.2.1-py3.6.egg"

SPARK_UI_PORT=4067

MAX_CORES=32


if [ $SPARK_JOB == 'True' ]
    then
        echo 'Executing Spark job'
        spark-submit --master $SPARK_MASTER \
		     --name 'Respiration Features' \
                     --conf spark.ui.port=$SPARK_UI_PORT \
                     --conf spark.cores.max=$MAX_CORES \
                     --py-files $PY_FILES \
                     core/driver.py -c $CC_CONFIG_FILEPATH \
                     -s $STUDY_NAME -sd $START_DATE \
                     -ed $END_DATE -u $USERIDS -f $FEATURES \
                     -p $MAX_CORES
    else
        echo 'Executing single threaded'
        echo $PYTHONPATH
        python3.6 core/driver.py -c $CC_CONFIG_FILEPATH \
                       -s $STUDY_NAME -sd $START_DATE \
                       -ed $END_DATE -u $USERIDS -f $FEATURES

fi


#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6

#Spark path
export SPARK_HOME=/usr/local/spark/

# path of cc configuration path
CC_CONFIG_FILEPATH="/cerebralcortex/code/config/cc_starwars_configuration.yml"

# list of features to process, leave blank to process all features
FEATURES=""

# study name
STUDY_NAME="mperf"

# start date
START_DATE="20171001" #"20171103"

# end date
END_DATE="20180130" #"20171111"

# Folder containing the metadata templates for the features
FEATURE_METADATA_DIR=$DATA_ANALYSIS_PATH'/core/resources/metadata'

# list of usersids separated by comma. Leave blank to process all users.
USERIDS="e8976d47-539a-425a-9505-b06b0adce8bb,1202a63a-5f17-4f56-8f6c-45858f45b75a,2f0b3625-db5b-4159-87c1-9c005fb8659e,65f92f49-dec0-4da6-8396-e9d7046d934f,d491bbc3-7ad8-4d34-9ace-38efc3c80497,f4608852-fa34-4a2a-8682-9ef7038af460,863ae7e4-3eba-4062-b102-acd6df48abdc,b61bcbe1-38ac-484f-ae35-13df248d8135,e7157387-8c32-401c-a6fc-461d36bd96b6,207a99f5-de40-48c1-a1be-508ab98d935d,a3ce4c46-01c7-41a8-9591-1686a12673ad,ea1c8352-a843-492d-b119-0dc4fdabe630,a1e2f165-d872-4c94-9e3f-0d0d6c45fd93,05cb11e7-2c05-4f16-99da-f408a3cc19e9,"
#USERIDS=""

SPARK_MASTER="spark://dagobah10dot.memphis.edu:7077"

PY_FILES="/home/nndugudi/md2k/CerebralCortex/dist/MD2K_Cerebral_Cortex-2.0.0-py3.6.egg,dist/MD2K_Cerebral_Cortex_DataAnalysis_compute_features-2.2.1-py3.6.egg"

SPARK_UI_PORT=4066

MAX_CORES=48,

# set to True to make use of spark parallel execution
#SPARK_JOB="True"
SPARK_JOB="False"

# build before executing

python3.6 setup.py bdist_egg

if [ $SPARK_JOB == 'True' ]
    then
        echo 'Executing Spark job'
        spark-submit --master $SPARK_MASTER \
                     --conf spark.ui.port=$SPARK_UI_PORT \
                     --conf spark.cores.max=$MAX_CORES \
                     --conf spark.app.name='OFFICE_TIME' \
                     --py-files $PY_FILES \
                     core/driver.py -c $CC_CONFIG_FILEPATH \
                     -s $STUDY_NAME -sd $START_DATE \
                     -ed $END_DATE -u $USERIDS -f $FEATURES \
                     -p $MAX_CORES
    else
        echo 'Executing single threaded'
        export PYTHONPATH=.:"/home/nndugudi/md2k/CerebralCortex/dist/MD2K_Cerebral_Cortex-2.0.0-py3.6.egg":$PYTHONPATH
        echo $PYTHONPATH
        python3.6 core/driver.py -c $CC_CONFIG_FILEPATH \
                       -s $STUDY_NAME -sd $START_DATE \
                       -ed $END_DATE -u $USERIDS -f $FEATURES 

fi


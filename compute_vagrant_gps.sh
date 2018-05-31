#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6

#Spark path
export SPARK_HOME=/usr/local/spark/

# path of cc configuration path
CC_CONFIG_FILEPATH="/home/vagrant/CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml"

# list of features to process, leave blank to process all features
FEATURES="gps"

# study name
STUDY_NAME="demo"

# start date
START_DATE="20180525" #"20171103"

# end date
END_DATE="20180801" #"20171111"

# list of usersids separated by comma. Leave blank to process all users.
USERIDS=""

SPARK_MASTER="local[2]"

CC_EGG=""

PY_FILES=${CC_EGG}",dist/MD2K_Cerebral_Cortex_DataAnalysis_compute_features-2.2.3-py3.6.egg"


SPARK_UI_PORT=4066

MAX_CORES=6

# set to True to make use of spark parallel execution
SPARK_JOB="False"

# build before executing

python3.6 setup.py bdist_egg

if [ $SPARK_JOB == 'True' ]
    then
        echo 'Executing Spark job'
        spark-submit --master $SPARK_MASTER \
                     --conf spark.ui.port=$SPARK_UI_PORT \
                     --conf spark.cores.max=$MAX_CORES \
                     --conf spark.app.name=$FEATURES \
                     --py-files $PY_FILES \
                     core/driver.py -c $CC_CONFIG_FILEPATH \
                     -s $STUDY_NAME -sd $START_DATE \
                     -ed $END_DATE -u $USERIDS -f $FEATURES \
                     -p $MAX_CORES
    else
        echo 'Executing single threaded'
        export PYTHONPATH=.:${CC_EGG}:$PYTHONPATH
        echo $PYTHONPATH
        python3.6 core/driver.py -c $CC_CONFIG_FILEPATH \
                       -s $STUDY_NAME -sd $START_DATE \
                       -ed $END_DATE -u $USERIDS -f $FEATURES

fi


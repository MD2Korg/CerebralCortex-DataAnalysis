#!/usr/bin/env bash

DATA_ANALAYSIS_PATH='/home/vagrant/tmp/CerebralCortex-DataAnalysis/'
# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6
export LD_LIBRARY_PATH=/home/vagrant/hadoop/lib/native/
export PATH=/home/vagrant/hadoop/bin/:$PATH

#Spark path
export SPARK_HOME=/usr/local/spark/

#set spark home
export PATH=$SPARK_HOME/bin:$PATH

# setting of PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$DATA_ANALAYSIS_PATH
export PYTHONPATH=$PYTHONPATH:'$DATA_ANALAYSIS_PATH:core/feature/activity'
export PYTHONPATH=$PYTHONPATH:'$DATA_ANALAYSIS_PATH:core/signalprocessing'
export PYTHONPATH=$PYTHONPATH:'$DATA_ANALAYSIS_PATH:core/signalprocessing/gravity_filter'

# path of cc configuration path
CC_CONFIG_FILEPATH="/home/vagrant/CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml"

# spark master
SPARK_MASTER="local[2]"

# list of features to process, leave blank to process all features
FEATURES="activity"

# study name
STUDY_NAME="mPerf"

# start date
START_DATE="20180101"

# end date
END_DATE="20180110"

# list of usersids separated by comma. Leave blank to process all users.
USERIDS="test1,test2"

spark-submit --master $SPARK_MASTER \
core/driver.py -c $CC_CONFIG_FILEPATH -s $STUDY_NAME -sd $START_DATE \
               -ed $END_DATE -u $USERIDS -f $FEATURES


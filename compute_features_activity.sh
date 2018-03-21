#!/usr/bin/env bash

DATA_ANALYSIS_PATH='/home/nndugudi/md2k/CerebralCortex-DataAnalysis'
# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6
#export LD_LIBRARY_PATH=/home/vagrant/hadoop/lib/native/
#export PATH=/home/vagrant/hadoop/bin/:$PATH

#Spark path
export SPARK_HOME=/usr/local/spark/

#set spark home
#export PATH=$SPARK_HOME/bin:$PATH

# setting of PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$DATA_ANALYSIS_PATH
export PYTHONPATH=$PYTHONPATH:$DATA_ANALYSIS_PATH'/core/feature/activity/'
export PYTHONPATH=$PYTHONPATH:$DATA_ANALYSIS_PATH'/core/feature/gps/'
export PYTHONPATH=$PYTHONPATH:$DATA_ANALYSIS_PATH'/core/signalprocessing/'
export PYTHONPATH=$PYTHONPATH:$DATA_ANALYSIS_PATH'/core/signalprocessing/gravity_filter/'
echo $PYTHONPATH
# path of cc configuration path
CC_CONFIG_FILEPATH="/md2k/code/ali/cc_config/cc_configuration.yml"

# spark master
SPARK_MASTER="local[1]"

# list of features to process, leave blank to process all features
FEATURES="activity"

# study name
STUDY_NAME="mperf"

# start date
START_DATE="20171104"

# end date
END_DATE="20171111"

# Folder containing the metadata templates for the features
FEATURE_METADATA_DIR=$DATA_ANALYSIS_PATH'/core/resources/metadata'

# list of usersids separated by comma. Leave blank to process all users.
USERIDS="247d42cf-f81c-44d2-9db8-fea69f468d58"

spark-submit --master $SPARK_MASTER \
             --conf spark.ui.port=4045 \
core/driver.py -c $CC_CONFIG_FILEPATH -s $STUDY_NAME -sd $START_DATE \
               -ed $END_DATE -u $USERIDS -f $FEATURES \
               -m $FEATURE_METADATA_DIR


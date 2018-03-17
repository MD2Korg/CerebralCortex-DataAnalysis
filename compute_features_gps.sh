#!/usr/bin/env bash

DATA_ANALAYSIS_PATH='/md2k/code/CerebralCort-DataAnalysis/'
# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6
#export LD_LIBRARY_PATH=/home/vagrant/hadoop/lib/native/
#export PATH=/home/vagrant/hadoop/bin/:$PATH

#Spark path
export SPARK_HOME=/usr/local/spark/

#set spark home
#export PATH=$SPARK_HOME/bin:$PATH

# setting of PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$DATA_ANALAYSIS_PATH
export PYTHONPATH=$PYTHONPATH:'$DATA_ANALAYSIS_PATH:core/feature/activity'
export PYTHONPATH=$PYTHONPATH:'$DATA_ANALAYSIS_PATH:core/feature/gps'
export PYTHONPATH=$PYTHONPATH:'$DATA_ANALAYSIS_PATH:core/signalprocessing'
export PYTHONPATH=$PYTHONPATH:'$DATA_ANALAYSIS_PATH:core/signalprocessing/gravity_filter'

# path of cc configuration path
CC_CONFIG_FILEPATH="/md2k/code/ali/cc_config/cc_configuration.yml"

# spark master
SPARK_MASTER="local[1]"

# list of features to process, leave blank to process all features
FEATURES="gps"

# study name
STUDY_NAME="mperf"

# start date
START_DATE="20171001"

# end date
END_DATE="20180130"

# list of usersids separated by comma. Leave blank to process all users.
USERIDS="be4297a8-d763-42e2-a2cb-cab38f64cfe3,397c6457-0954-4cd2-995c-2fbeb6c72097,397c6457-0954-4cd2-995c-2fbeb6c72097"

spark-submit --master $SPARK_MASTER \
             --conf spark.ui.port=4045 \
core/driver.py -c $CC_CONFIG_FILEPATH -s $STUDY_NAME -sd $START_DATE \
               -ed $END_DATE -u $USERIDS -f $FEATURES


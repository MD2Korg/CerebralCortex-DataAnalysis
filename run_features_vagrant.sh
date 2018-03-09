#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6
export LD_LIBRARY_PATH=/home/vagrant/hadoop/lib/native/
export PATH=/home/vagrant/hadoop/bin/:$PATH

#Spark path
export SPARK_HOME=/usr/local/spark/

#use mydb to process messages without publishing them on kafka
DATA_REPLAY_TYPE="mydb" #acceptable params are mydb or kfka
MYDB_BATCH_SIZE="5000" #number of messages

#set spark home
export PATH=$SPARK_HOME/bin:$PATH

# path of cc configuration path
CC_CONFIG_FILEPATH="/home/vagrant/CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml"

# spark master
SPARK_MASTER="local[2]"
#-f activity -c /home/vagrant/CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml -s mPerf -sd 20180101 -ed 20180110 -u test
# list of features to process
FEATURES="activity"

# study name
STUDY_NAME="mPerf"

# start date
START_DATE="20180101"

# end date
END_DATE="20180110"

# list of users
USERIDS="test1,test2"

spark-submit --master $SPARK_MASTER \
core/driver.py -c $CC_CONFIG_FILEPATH -s $STUDY_NAME -sd $START_DATE \
               -ed $END_DATE -u $USERIDS -f $FEATURES


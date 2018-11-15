#!/usr/bin/env bash

# path of cc configuration path
CC_CONFIG_FILEPATH="/cc_conf/"

# list of features to process, leave blank to process all features
FEATURES="phone_features,office_time,phone_screen_touch_features,sleep_time,sleep_duration,sleep_duration_analysis,activity,activity_features,typing,task_features,context,cyberslacking,phone_app_usage,socialjetlag,typing_context,typing_speed,puffmarker,rr_interval,respiration_cycle_statistics,stress_from_respiration,heart_rate,stress_from_wrist"

# study name
STUDY_NAME="demo"

# start date
START_DATE=$1 #"20171001" #"20171103"

# end date
END_DATE=$2 #"20180130" #"20171111"

# list of usersids separated by comma. Leave blank to process all users.
USERIDS=""

SPARK_MASTER="local[*]"

PY_FILES=${CC_EGG}",dist/MD2K_Cerebral_Cortex_DataAnalysis_compute_features-2.2.1-py3.6.egg"

SPARK_UI_PORT=4066

MAX_CORES=4

# set to True to make use of spark parallel execution
SPARK_JOB="True"

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

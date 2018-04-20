#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6

#Spark path
export SPARK_HOME=/usr/local/spark/

# path of cc configuration path
CC_CONFIG_FILEPATH="/cerebralcortex/code/config/cc_starwars_configuration.yml"

# list of features to process, leave blank to process all features
FEATURES="beacon,gps_location_daywise,gps_daily,gpsfeature,phone_features,office_time,phone_screen_touch_features,sleep_time,sleep_duration,sleep_duration_analysis,activity,activity_features,typing,task_features,rr_interval,respiration_cycle_statistics,stress_from_respiration,heart_rate,stress_from_wrist"

# study name
STUDY_NAME="mperf"

# start date
START_DATE="20171001" #"20171103"

# end date
END_DATE="20180130" #"20171111"

# list of usersids separated by comma. Leave blank to process all users.
USERIDS="622bf725-2471-4392-8f82-fcc9115a3745,d3d33d63-101d-44fd-b6b9-4616a803225d,c1f31960-dee7-45ea-ac13-a4fea1c9235c,7b8358f3-c96a-4a17-87ab-9414866e18db,8a3533aa-d6d4-450c-8232-79e4851b6e11,e118d556-2088-4cc2-b49a-82aad5974167,260f551d-e3c1-475e-b242-f17aad20ba2c,dd13f25f-77a0-4a2c-83af-bb187b79a389,17b07883-4959-4037-9b80-dde9a06b80ae,5af23884-b630-496c-b04e-b9db94250307,61519ad0-2aea-4250-9a82-4dcdb93a569c,326a6c55-c963-42c2-bb8a-2591993aaaa2,a54d9ef4-a46a-418b-b6cc-f10b49a946ac,2fb5e890-afaf-428a-8e28-a7c70bf8bdf1,c93a811e-1f47-43b6-aef9-c09338e43947,9e4aeae9-8729-4b0f-9e84-5c1f4eeacc74,479eea59-8ad8-46aa-9456-29ab1b8f2cb2,b4ff7130-3055-4ed1-a878-8dfaca7191ac,fbd7bc95-9f42-4c2c-94f4-27fd78a7273c,bbc41a1e-4bbe-4417-a40c-64635cc552e6,82a921b9-361a-4fd5-8db7-98961fdbf25a,66a5cdf8-3b0d-4d85-bdcc-68ae69205206,d4691f19-57be-44c4-afc2-5b5f82ec27b5,136f8891-af6f-49c1-a69a-b4acd7116a3c"

SPARK_MASTER="spark://dagobah10dot.memphis.edu:7077"

CC_EGG=""

PY_FILES=${CC_EGG}",dist/MD2K_Cerebral_Cortex_DataAnalysis_compute_features-2.2.1-py3.6.egg"


SPARK_UI_PORT=4066

MAX_CORES=48

# set to True to make use of spark parallel execution
SPARK_JOB="True"

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


#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3.6

#Spark path
export SPARK_HOME=/usr/local/spark/

# path of cc configuration path
CC_CONFIG_FILEPATH="/cerebralcortex/code/config/cc_starwars_configuration.yml"

# list of features to process, leave blank to process all features
FEATURES="rr_interval"

# study name
STUDY_NAME="mperf-alabsi"

# start date
START_DATE="20171001" #"20171103"

# end date
END_DATE="20180130" #"20171111"

# Folder containing the metadata templates for the features
FEATURE_METADATA_DIR=$DATA_ANALYSIS_PATH'/core/resources/metadata'

# list of usersids separated by comma. Leave blank to process all users.
USERIDS="e8976d47-539a-425a-9505-b06b0adce8bb,
1202a63a-5f17-4f56-8f6c-45858f45b75a,
2f0b3625-db5b-4159-87c1-9c005fb8659e,
65f92f49-dec0-4da6-8396-e9d7046d934f,
d491bbc3-7ad8-4d34-9ace-38efc3c80497,
f4608852-fa34-4a2a-8682-9ef7038af460,
863ae7e4-3eba-4062-b102-acd6df48abdc,
b61bcbe1-38ac-484f-ae35-13df248d8135,
e7157387-8c32-401c-a6fc-461d36bd96b6,
207a99f5-de40-48c1-a1be-508ab98d935d,
a3ce4c46-01c7-41a8-9591-1686a12673ad,
ea1c8352-a843-492d-b119-0dc4fdabe630,
a1e2f165-d872-4c94-9e3f-0d0d6c45fd93,
05cb11e7-2c05-4f16-99da-f408a3cc19e9,
a0d77195-f911-48b8-a1a2-fbf3ed7e668f,
206e63d6-061f-425d-bd28-b845c6dcc8cf,
4c4271e7-464a-43c8-b976-3e6a705e2b51,
a236102f-0eb3-49c2-9f80-b28f590e1862,
286078ea-79e3-4a76-a08b-79d9f8f657f2,
59a5080b-4230-4b35-bce6-3d9932777903,
6f0b8a9b-6fe2-4cda-9825-d0b130f438a4,
a26c79be-154c-4e8c-b533-0347e39ea761,
3ed9da7f-1df0-4214-8d6a-de9332f8bbcb,
629887c7-614f-439d-8485-23455b9b7528,
0db4b9ac-8bba-45ae-824d-8892695bed08,
c8c42ae1-4fda-48c8-8d19-58c7b76d1c3e,
28777465-663f-447a-b6e6-015b60d1543a,
12560bbf-8e86-42fa-8b7c-a06a9c987b90,
7cf3871c-a7ae-438d-83f8-3249264ff6c1,
ac787ead-563f-4d0a-92d1-1c595150dc5f,
a6260465-03bc-418f-af1f-b266cb7f1de1,
b3d3a191-b828-42f9-9f19-958c76a8e639,
714b9f6d-8a4b-47fd-ba3a-d277cbec666b,
8cfa8490-aec2-4032-9452-de9c4dccb94a,
d77ee136-0ded-4cca-87f0-a779bb6cd012,
39e57530-8c8a-41f6-8659-da7091c1a8eb,
64ce1bb3-97c8-419b-b1d9-e159cb50a8fa,
9b22305d-5327-40a7-8baf-4f54af4cc373,
09e87864-9585-42d7-ac1f-ff88cce577b0,
b105c482-2d09-48b8-9202-d8e4d97b515f,
e5fee47d-79ca-4cc4-95c2-f432f0f62379"
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


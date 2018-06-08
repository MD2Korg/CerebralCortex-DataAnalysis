#!/usr/bin/env bash
export PYSPARK_PYTHON=/usr/bin/python3.6

#Spark path
export SPARK_HOME=/usr/local/spark/


CC_CONFIG_PATH='/cerebralcortex/code/config/cc_starwars_configuration.yml'

STUDY_NAME='mperf'

CC_EGG="/cerebralcortex/code/eggs/MD2K_Cerebral_Cortex-2.2.2-py3.6.egg"
#export PYTHONPATH=${CC_EGG}:$PYTHONPATH
START_DATE="20171001"
END_DATE="20180130"
MAX_CORES=128

SPARK_MASTER="spark://dagobah10dot.memphis.edu:7077"
PY_FILES=${CC_EGG}


spark-submit --master $SPARK_MASTER \
                --conf spark.ui.port=4066\
              --conf spark.cores.max=$MAX_CORES \
              --conf spark.app.name=DATA_AUDITOR \
               --py-files $PY_FILES \
             auditor.py -c $CC_CONFIG_PATH -s $STUDY_NAME -sd $START_DATE -ed $END_DATE



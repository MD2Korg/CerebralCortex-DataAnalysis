#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3

#Spark path
export SPARK_HOME=/home/ali/spark/spark-2.2.0-bin-hadoop2.7/

export PYTHONPATH=/home/ali/spark/spark-2.2.0-bin-hadoop2.7/python:/home/ali/spark/spark-2.2.0-bin-hadoop2.7/python/lib/py4j-0.10.3-src.zip:/home/ali/IdeaProjects/CerebralCortex-DataAnalysis/
#PySpark args (do not change unless you know what you are doing)
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 pyspark-shell"

#set spark home
export PATH=$SPARK_HOME/bin:$PATH


# path of cc configuration file path
CC_CONFIG_FILEPATH="/home/ali/IdeaProjects/CerebralCortex-2.0/cerebralcortex/core/resources/cc_configuration.yml"
# mdebuger configuration file path
MDEBUGER_CONFIG_FILEPATH="/home/ali/IdeaProjects/CerebralCortex-DataAnalysis/modules/mdebugger/data_diagnostic_config.yml"

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 main.py -cc $CC_CONFIG_FILEPATH -mdc $MDEBUGER_CONFIG_FILEPATH
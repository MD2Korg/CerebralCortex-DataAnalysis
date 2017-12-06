import random
from datetime import datetime, timedelta
import numpy as np
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import cume_dist
import pyspark.sql.functions as F
import itertools
from pyspark.sql.types import *

ss = SparkSession.builder
sparkSession = ss.getOrCreate()
sc = sparkSession.sparkContext
sqlContext = SQLContext(sc)

def merge_lists(lst):
    return list(itertools.chain(*lst))

def get_sample_data():
    sample_data = []
    initial_time = datetime(2017, 9, 1, 11, 34, 40)

    for row in range(0,5):
        sample = [row*7]
        tmp = ('1', '02', initial_time + timedelta(0, row), sample)
        sample_data.append (tmp)
    return sample_data

print(get_sample_data())

dummy_function_udf = F.udf(merge_lists, StringType())

df = sqlContext.createDataFrame(get_sample_data(), schema=['id', 'day', 'start_time', 'sample'])
df.show()

initial_time = datetime(2017, 9, 1, 10, 34, 40)

df3 = df.groupBy(F.window("start_time", windowDuration="5 second",startTime="1 second")) \
    .agg(F.collect_set('sample'))
df3.show()
sd = df3.collect()
df4 = df3.withColumn("sample", dummy_function_udf(df3['sample']))

print("done")



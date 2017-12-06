import numpy as np
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import cume_dist
import pyspark.sql.functions as F
import random
ss = SparkSession.builder
sparkSession = ss.getOrCreate()
sc = sparkSession.sparkContext
sqlContext = SQLContext(sc)

np.random.seed(1)


keys = ["foo"] * 10 + ["bar"] * 10
values = np.hstack([np.random.normal(0, 1, 10), np.random.normal(10, 1, 100)])

df = sqlContext.createDataFrame([
    {"k": k, "v": round(float(v), 3)} for k, v in zip(keys, values)])

w =  Window.partitionBy(df.k).orderBy(df.v)

df2 = df.select(
    "k", "v",
    cume_dist().over(w).alias("cume_dists")
)
df2.show()

df3 = df.groupBy('k') \
    .agg(F.collect_list('v')).over(w)



df = sc.sql.createDataFrame([
    ('1', '02', '3', '[6]'),
    ('1', '02', '3.1', '[6]'),
    ('1', '02', '3.2', '[6]'),
    ('1', '02', '3', '[6]'),
    ('1', '02', '3', '[6]'),
    ('1', '02', '3', '[6]'),
    ('1', '02', '3', '[6]'),
    ('1', '02', '3', '[6]'),
    ('1', '02', '3', '[6]'),
    ('1', '02', '3', '[6]'),
    ('1', '02', '3', '[6]'),
], schema=['id', 'day', 'start_time', 'sample'])
print("done")
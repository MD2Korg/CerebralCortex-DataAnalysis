import bz2
import uuid
from os import listdir
from os.path import isfile, join

from cassandra.cluster import Cluster
from cassandra.query import *
from dateutil.parser import parse
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

data_path = "/home/ali/IdeaProjects/MD2K_DATA/test_data/"


def process_one_file(only_files, data_path):
    cluster = Cluster(
        ['127.0.0.1'],
        port=9042)

    session = cluster.connect('cerebralcortex')

    for f in only_files:
        with bz2.BZ2File(data_path + f, 'r') as file:
            print(f)
            batch_size = 0;
            insert_user = session.prepare("INSERT INTO data (identifier, day, start_time, sample) VALUES (?, ?, ?, ?)")
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            try:
                for line in file:
                    line = line.decode("utf-8")
                    if "|" in line and "day" not in line:
                        line = line.split("|")
                        identifier = uuid.UUID(line[0].strip())
                        day = line[1].strip()
                        start_time = parse(line[2].strip())
                        sample = line[4].strip()
                        if batch_size > 64990:
                            session.execute(batch)
                            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                            batch.clear()
                            batch_size = 0
                        else:
                            batch.add(insert_user, (identifier, day, start_time, sample))
                            batch_size += 1

            except Exception as e:
                print(e)


def process_data2(f, data_path):
    cluster = Cluster(
        ['127.0.0.1'],
        port=9042)

    session = cluster.connect('cerebralcortex')

    with bz2.BZ2File(data_path + f, 'r') as file:
        print(f)
        batch_size = 0;
        insert_user = session.prepare("INSERT INTO data (identifier, day, start_time, sample) VALUES (?, ?, ?, ?)")
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        try:
            for line in file:
                line = line.decode("utf-8")
                if "|" in line and "day" not in line:
                    line = line.split("|")
                    identifier = uuid.UUID(line[0].strip())
                    day = line[1].strip()
                    start_time = parse(line[2].strip())
                    sample = line[4].strip()
                    if batch_size > 100:
                        session.execute(batch)
                        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                        batch.clear()
                        batch_size = 0
                    else:
                        batch.add(insert_user, (identifier, day, start_time, sample))
                        batch_size += 1

        except Exception as e:
            print(e)
        return f


#process_data2("588a1703-1af1-3aed-a260-f45e2e685f84.csv.bz2", data_path)

ss = SparkSession.builder
sparkSession = ss.getOrCreate()
sc = sparkSession.sparkContext
sqlContext = SQLContext(sc)

only_files = [f for f in listdir(data_path) if isfile(join(data_path, f))]
rdd = sc.parallelize(only_files)
#process_data(only_files, data_path)
result = rdd.map(lambda x: process_data2(x, data_path))
print(result.collect())

from os import listdir
from os.path import isfile, join
import bz2
from dateutil.parser import parse
import uuid
from cassandra.cluster import Cluster
from cassandra.query import *
import csv
import time
import uuid
import datetime
cluster = Cluster(
    ['127.0.0.1'],
    port=9042)

session = cluster.connect('cerebralcortex')
st = datetime.datetime.now()
query = "SELECT * FROM data where identifier=98c2509a-3891-3177-865f-5836b701e6f6 and day='20171101'"  # users contains 100 rows
statement = SimpleStatement(query, fetch_size=600000)
i = 0
for row in session.execute(statement):
    i +=1



print(i, " done ", datetime.datetime.now()-st)
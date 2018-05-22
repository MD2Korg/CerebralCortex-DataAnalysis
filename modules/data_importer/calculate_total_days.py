import os
import csv
import uuid
from datetime import datetime
from datetime import timedelta

CC_CONFIG_PATH='/cerebralcortex/code/config/cc_starwars_configuration.yml'                                                                                            
UUID_MAPPING='/cerebralcortex/code/anand/qualtrics/mperf_ids.txt' 
        
user_id_mappings = {}
    

def parse_userid_mappings():
    f = open(UUID_MAPPING)
    if f is None:return
    for line in f:
        splits = line.split()
        uuid = splits[1]
        username = splits[0]
        username_splts = username.split('_')
        username = username_splts[1]
        user_id_mappings[username] = uuid
    f.close()


parse_userid_mappings()
user_dates = {}


def process(file_path):
    f = open(file_path)
    
    if f is None:
        print('File not found',file_path)
        return
    
    reader = csv.reader(f)
    count = 0
    feature_data = {}
    start_column_number = 3    
    
    for row in reader:
        if count == 0:
            header_row = row
            count +=1
            continue
            
        # handling corrupt data, some user id's are NA
        if row[0] not in user_id_mappings:continue
        
        user_id = user_id_mappings[row[0]]
        
        qualtrics_start_time = datetime.strptime(row[3], '%m/%d/%Y %H:%M')
        qualtrics_end_time = datetime.strptime(row[4], '%m/%d/%Y %H:%M')

        key = (row[0], user_id)
        if key not in user_dates:
            user_dates[key] = []

        user_dates[key].append(qualtrics_start_time)
    f.close()

process('Daily.stress.d.csv')

f = open('report.txt','w')
buf = 'User\tUser ID\tTotal days\tNumber of data points\n'
for usr in user_dates:
    dates = user_dates[usr]
    line = str(usr[0]) + '\t' + str(usr[1]) + '\t' +\
    str((dates[-1] - dates[0]).days) +\
            '\t'+ str(len(dates)) +'\n'
    buf += line

f.write(buf)
f.close()

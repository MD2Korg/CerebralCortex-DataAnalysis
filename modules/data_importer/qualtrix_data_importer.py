import csv
import os
from datetime import datetime


with open('mPerf_test_data/Daily.alc.d.csv', newline='') as f:
    reader = csv.reader(f)
    count = 0
    for row in reader:
        if count == 0:
            count +=1
            continue
        user_id = row[0]
        start_time = row[1]
        start_time = datetime.strptime(row[1], '%m/%d/%Y %H:%M')
        end_time = datetime.strptime(row[2], '%m/%d/%Y %H:%M')
        alc_status = row[5]
        alc_quantity = row[6] 
        print('alc stat %s quantity %s'%(alc_status,alc_quantity))



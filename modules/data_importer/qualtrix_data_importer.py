import csv
import os
from datetime import datetime

DATA_DIR='/home/vagrant/mperf_test_data'

ALC_D = 'Daily.alc.d.csv'
ALC_D_METADATA='metadata/daily.alc.d.json'


def open_data_file(filename):
    fp = os.path.join(DATA_DIR,filename)
    if os.path.exists(fp):
        return open(fp, newline='')    

def import_file(filename):
    f = open_data_file(filename)
    csv_reader = csv.reader(f)
         
    
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

def main():
    



if __name__ == '__main__':
    main()

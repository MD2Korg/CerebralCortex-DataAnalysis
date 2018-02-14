import csv
import os
import json
import uuid
from datetime import datetime
from cerebralcortex.core.datatypes.datastream import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream


DATA_DIR='/home/vagrant/mPerf_test_data'

UUID_MAPPING = 'UUID_mapping.txt'
ALC_D = 'Daily.alc.d.csv'
ALC_D_METADATA='metadata/daily.alc.d.json'


user_id_mappings={}

def parse_userid_mappings():
    f = open_data_file(UUID_MAPPING)
    if f is None:return
    for line in f:
        splits = line.split()
        uuid = splits[1]
        username = splits[0]
        username_splts = username.split('_')
        username = username_splts[1]
        user_id_mappings[username] = uuid
    

def open_data_file(filename):
    fp = os.path.join(DATA_DIR,filename)
    if os.path.exists(fp):
        return open(fp, newline='') 
    else:
        print('File not found %s' % fp)   

def import_file(filename):
    f = open_data_file(filename)
    csv_reader = csv.reader(f)
         
    
def process_alc():
    f = open_data_file(ALC_D)
    
    if f is None:return
    
    reader = csv.reader(f)
    count = 0
    feature_data = {}
    
    for row in reader:
        if count == 0:
            header_row = row
            count +=1
            continue
        user_id = user_id_mappings[row[0]]
        start_time = datetime.strptime(row[1], '%m/%d/%Y %H:%M')
        start_time = start_time.timestamp() * 1000
        end_time = datetime.strptime(row[2], '%m/%d/%Y %H:%M')
        end_time = end_time.timestamp() * 1000
        alc_status = row[5]
        alc_quantity = row[6] 
        sample = {}
        sample[header_row[4]] = row[4]
        sample[header_row[5]] = row[5]
        sample[header_row[6]] = row[6]
        sample_str = json.dumps(sample)
        dp = DataPoint(start_time=start_time, end_time=end_time, offset=0, sample=sample_str) 
        
        if user_id not in feature_data:
            feature_data[user_id] = []
        
        feature_data[user_id].append(dp)

    f = open(ALC_D_METADATA)
    metadata = f.read()
    metadata = json.loads(metadata)

    for user in feature_data:
        output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(ALC_D_METADATA + user + ALC_D)))
        print('output_stream_id %s user %s' % (output_stream_id, user))
        ds = DataStream(identifier=output_stream_id, owner=user, name=metadata['name'], data_descriptor=\
                metadata['data_descriptor'], execution_context=metadata['execution_context'], annotations=\
                metadata['annotations'], stream_type='datastream', data=feature_data[user]) 
    
        # TODO store ds


def main():
    parse_userid_mappings()
    process_alc()



if __name__ == '__main__':
    main()

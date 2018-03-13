import csv
import os
import json
import uuid
import argparse
from datetime import datetime
from cerebralcortex.core.datatypes.datastream import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.cerebralcortex import CerebralCortex



CC_CONFIG_PATH = '/home/vagrant/CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml'

DATA_DIR='/home/vagrant/mperf_data'

UUID_MAPPING = '/home/vagrant/mperf_ids.txt'

parser = argparse.ArgumentParser(description='CerebralCortex '
                                     'Qualtrics data importer')
parser.add_argument("-c", "--cc-config", help="Path to file containing the "
                        "CerebralCortex configuration", required=True)
parser.add_argument("-d", "--data-dir", help="Path to dir containing the "
                                      "qualtrics data" , required=True)
parser.add_argument("-u", "--user-mappings", help="Path to the file containing "
                    " the user id to uuid mappings",  required=True)

args = vars(parser.parse_args())

if args['cc_config']:
    CC_CONFIG_PATH = args['cc_config']
if args['data_dir']:
    DATA_DIR = args['data_dir']
if args['user_mappings']:
    UUID_MAPPING = args['user_mappings']

files_to_process=[]

# Below are the list of filenames 
ALC_D = 'Daily.alc.d.csv'
ALC_D_METADATA='metadata/daily.alc.d.json'
files_to_process.append((ALC_D,ALC_D_METADATA))


ANXIETY_D = 'Daily.anxiety.d.csv'
ANXIETY_D_METADATA='metadata/daily.anxiety.d.json'
files_to_process.append((ANXIETY_D,ANXIETY_D_METADATA))

BFI_D = 'Daily.bfi.d.csv'
BFI_D_METADATA='metadata/daily.bfi.d.json'
files_to_process.append((BFI_D,BFI_D_METADATA))

DALAL_D = 'Daily.dalal.csv'
DALAL_D_METADATA='metadata/daily.dalal.json'
files_to_process.append((DALAL_D,DALAL_D_METADATA))

EXERCISE_D = 'Daily.exercise.d.csv'
EXERCISE_D_METADATA='metadata/daily.exercise.d.json'
files_to_process.append((EXERCISE_D,EXERCISE_D_METADATA))

IRB_D = 'Daily.irb.d.csv'
IRB_D_METADATA='metadata/daily.irb.d.json'
files_to_process.append((IRB_D,IRB_D_METADATA))

ITP_D = 'Daily.itp.d.csv'
ITP_D_METADATA='metadata/daily.itp.d.json'
files_to_process.append((ITP_D,ITP_D_METADATA))

PAN_D = 'Daily.pan.d.csv'
PAN_D_METADATA='metadata/daily.pan.d.json'
files_to_process.append((PAN_D,PAN_D_METADATA))

SLEEP_D = 'Daily.sleep.d.csv'
SLEEP_D_METADATA='metadata/daily.sleep.d.json'
files_to_process.append((SLEEP_D,SLEEP_D_METADATA))

STRESS_D = 'Daily.stress.d.csv'
STRESS_D_METADATA='metadata/daily.stress.d.json'
files_to_process.append((STRESS_D,STRESS_D_METADATA))

TOB_D = 'Daily.tob.d.csv'
TOB_D_METADATA='metadata/daily.tob.d.json'
files_to_process.append((TOB_D,TOB_D_METADATA))

WORKTODAY_D = 'Daily.worktoday.d.csv'
WORKTODAY_D_METADATA='metadata/daily.worktoday.d.json'
# TODO confirm what this file corresponds to
#files_to_process.append((WORKTODAY_D,WORKTODAY_D_METADATA))

IGTB = 'igtb_composites.csv'
IGTB_METADATA='metadata/igtb_composites.json'
files_to_process.append((IGTB,IGTB_METADATA))
# End list of file names

# Map that contains the user 
user_id_mappings={}


# CC intialization
CC = CerebralCortex(CC_CONFIG_PATH)

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
         
    
def process_feature(file_path, metadata_path):
    f = open_data_file(file_path)
    mf = open(metadata_path)
    
    if f is None:return
    
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
        start_time = datetime.strptime(row[1], '%m/%d/%Y %H:%M')
        
        # handling the different format of the IGTB file
        if IGTB not in file_path:
            end_time = datetime.strptime(row[2], '%m/%d/%Y %H:%M')
        else:
            end_time = start_time
            start_column_number = 2    
        
        sample = {}
        
        for idx in range(start_column_number, len(row)):
            sample[header_row[idx]] = row[idx]
        
        sample_str = json.dumps(sample)
        dp = DataPoint(start_time=start_time, end_time=end_time, offset=0, sample=sample_str) 
        
        if user_id not in feature_data:
            feature_data[user_id] = []
        
        feature_data[user_id].append(dp)

    metadata = mf.read()
    metadata = json.loads(metadata)
    
    for user in feature_data:
        output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str( metadata_path + user + file_path)))
        ds = DataStream(identifier=output_stream_id, owner=user, name=metadata['name'], data_descriptor=\
                metadata['data_descriptor'], execution_context=metadata['execution_context'], annotations=\
                metadata['annotations'], stream_type='datastream', data=feature_data[user]) 
        #print(str(user),str(output_stream_id),len(feature_data[user]))   	 
        try:
            CC.save_stream(ds)
        except Exception as e:
            print(e)
    f.close()
    mf.close()

def main():
    parse_userid_mappings()
    
    # processing ALC_D
    # ID","StartDate","EndDate","RecordedDate","SurveyType","alc_status","alc.quantity.d"
    for feature in files_to_process:
        print("PROCESSING %s %s"%(feature[0], feature[1]))
        process_feature(feature[0], feature[1])



if __name__ == '__main__':
    main()

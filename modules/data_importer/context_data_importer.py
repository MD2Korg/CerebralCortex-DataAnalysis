# Copyright (c) 2017, MD2K Center of Excellence
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import sys
import csv
import os
import json
import uuid
import argparse
import pytz
import pickle
from datetime import datetime
from cerebralcortex.core.datatypes.datastream import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.cerebralcortex import CerebralCortex
import cerebralcortex.cerebralcortex



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
FILE_NAME = 'context1.csv'
FILE_METADATA='metadata/context.json'
files_to_process.append((FILE_NAME,FILE_METADATA))
FILE_NAME = None
FILE_METADATA = None

FILE_NAME = 'context2.csv'
FILE_METADATA='metadata/context.json'
files_to_process.append((FILE_NAME,FILE_METADATA))
FILE_NAME = None
FILE_METADATA = None

FILE_NAME = 'context3.csv'
FILE_METADATA='metadata/context.json'
files_to_process.append((FILE_NAME,FILE_METADATA))
FILE_NAME = None
FILE_METADATA = None

FILE_NAME = 'context4.csv'
FILE_METADATA='metadata/context.json'
files_to_process.append((FILE_NAME,FILE_METADATA))
FILE_NAME = None
FILE_METADATA = None
# End list of file names

# Map that contains the user 
user_id_mappings={}

# Timezone in which all times are recorded
centraltz=pytz.timezone('US/Central')
easterntz=pytz.timezone('US/Eastern')
pacifictz=pytz.timezone('US/Pacific')


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
        
        qualtrics_start_time = datetime.strptime(row[3], '%m/%d/%Y %H:%M')
        qualtrics_end_time = datetime.strptime(row[4], '%m/%d/%Y %H:%M')

        if len(user_id) == 4 and int(user_id[0]) == 5: # all 5xxx users are incentral
            qualtrics_start_time = centraltz.localize(qualtrics_start_time)
            qualtrics_end_time = centraltz.localize(qualtrics_end_time)
        elif len(user_id) == 4 and int(user_id[0]) == 1: # all 1xxx users are east
            qualtrics_start_time = easterntz.localize(qualtrics_start_time)
            qualtrics_end_time = easterntz.localize(qualtrics_end_time)
        elif len(user_id) == 4 and int(user_id[0]) == 9: # all 9xxx users are west
            qualtrics_start_time = pacifictz.localize(qualtrics_start_time)
            qualtrics_end_time = pacifictz.localize(qualtrics_end_time)
        else:
            qualtrics_start_time = centraltz.localize(qualtrics_start_time)
            qualtrics_end_time = centraltz.localize(qualtrics_end_time)
        

        utc_offset = qualtrics_start_time.utcoffset().total_seconds() * 1000
        # -1000 - DataPoint expects offset to be in milliseconds and negative is
        # to account for being west of UTC
        

        sample = row[6:]
        values = None
        val = sample[0]
        #print('X'*20,val, len(val.strip()))
        if 'yes' in val or 'no' in val:# Check for Daily.tob.d.mitre.csv
            value = float('Nan')
        elif 'NA' in val:
            value = float('Nan')
        elif not len(val.strip()):
            value = float('Nan')
        else:
            value = float(val)
        
        q_dp = DataPoint(start_time=qualtrics_start_time, end_time=qualtrics_end_time,
                       offset=utc_offset, sample=values) 

        if user_id not in feature_data:
            feature_data[user_id] = []
        
        feature_data[user_id].append(q_dp)

    metadata = mf.read()
    metadata = json.loads(metadata)
    metadata_name = metadata['name']
    
    for user in feature_data:
        output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(
            metadata_name + user + file_path)))
        q_dps = feature_data[user]
        
        q_ds = DataStream(identifier=output_stream_id, owner=user, 
                        name=metadata_name, 
                        data_descriptor= metadata['data_descriptor'], 
                        execution_context=metadata['execution_context'], 
                        annotations= metadata['annotations'], 
                        stream_type=1,
                        data=q_dps) 
        
        try:
            CC.save_stream(q_ds, localtime=True)
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

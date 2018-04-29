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

print('-'*10,os.path.abspath(cerebralcortex.__file__))


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
FILE_NAME = 'igtb_composites.csv'
FILE_METADATA='metadata/igtb_composites.json'
files_to_process.append((FILE_NAME,FILE_METADATA))
FILE_NAME = None
FILE_METADATA = None
# End list of file names

# Map that contains the user 
user_id_mappings={}

# Timezone in which all times are recorded
centraltz=pytz.timezone('US/Central')


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
         
    
def parse_str(num):
    try:
        return float(num)
    except Exception as e:
        return float('NaN')

def save_point(user, value, start_time, end_time, offset, metadata, stream_name_suffix):
    dp = DataPoint(start_time=start_time, end_time=end_time,
                       offset=offset, sample=[value]) 


    metadata_name = metadata['name'] 
    metadata_name = metadata_name + stream_name_suffix
    
    output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(
            metadata_name + user + str(metadata))))
    ds = DataStream(identifier=output_stream_id, owner=user, 
                        name=metadata_name, 
                        data_descriptor= metadata['data_descriptor'], 
                        execution_context=metadata['execution_context'], 
                        annotations= metadata['annotations'], 
                        stream_type=1,
                        data=[dp]) 
        #print(str(user),str(output_stream_id),len(feature_data[user]))   	 
    try:
        CC.save_stream(ds, localtime=True)
    except Exception as e:
        print(e)


def process_feature(file_path, metadata_path):
    f = open_data_file(file_path)
    mf = open(metadata_path)
    metadata_str = mf.read()
    metadata = json.loads(metadata_str)
    mf.close()
    
    if f is None:return
    
    reader = csv.reader(f)
    count = 0
    feature_data = {}

    for row in reader:
        if count == 0:
            header_row = row
            count +=1
            continue
            
        # handling corrupt data, some user id's are NA
        if row[0] not in user_id_mappings:continue
        
        user_id = user_id_mappings[row[0]]
        start_time = datetime.strptime(row[1], '%m/%d/%Y %H:%M')
        if len(user_id) == 4 and int(user_id[0]) == 5: # all 5xxx users are incentral
            start_time = centraltz.localize(start_time)
        elif len(user_id) == 4 and int(user_id[0]) == 1: # all 1xxx users are east
            start_time = easterntz.localize(start_time)
        elif len(user_id) == 4 and int(user_id[0]) == 9: # all 9xxx users are west
            start_time = pacifictz.localize(start_time)
        else:
            start_time = centraltz.localize(start_time)
        
        utc_offset = start_time.utcoffset().total_seconds() * 1000
        # -1000 - DataPoint expects offset to be in milliseconds and negative is
        # to account for being west of UTC
        
        '''
        "shipley.vocab","shipley.abs","irb","itp","ocb","inter.deviance","org.deviance","extraversion","agreeableness","conscientiousness"
        "neuroticism","openness","pos.affect","neg.affect","stai.trait","audit","gats.status","gats.quantity","gats.quantity.sub","ipaq","psqi"
        '''
        shipley_vocab = parse_str(row[2])
        save_point(user = user_id,
                   value = shipley_vocab, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='shipley.vocab')

        shipley_abs = parse_str(row[3])
        save_point( user = user_id,value = shipley_abs, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='shipley.abs')

        irb = parse_str(row[4])
        save_point( user = user_id,value = irb, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='irb')

        itp = parse_str(row[5])
        save_point(user = user_id,value = itp, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='itp')

        ocb = parse_str(row[6])
        save_point(user = user_id,value = ocb, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='ocb')

        inter_deviance = parse_str(row[7])
        save_point(user = user_id,value = inter_deviance, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='inter.deviance')

        org_deviance = parse_str(row[8])
        save_point(user = user_id,value = org_deviance, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='org_deviance')

        extraversion = parse_str(row[9])
        save_point(user = user_id,value = extraversion, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='extraversion')

        agreeableness = parse_str(row[10])
        save_point(user = user_id,value = agreeableness, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='agreeableness')

        conscientiousness = parse_str(row[11])
        save_point(user = user_id,value = conscientiousness, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='conscientiousness')

        neuroticism = parse_str(row[12])
        save_point(user = user_id,value = neuroticism, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='neuorticism')

        openness = parse_str(row[13])
        save_point(user = user_id,value = openness, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='openness')

        pos_effect = parse_str(row[14])
        save_point(user = user_id,value = pos_effect, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='pos.affect')

        neg_effect = parse_str(row[15])
        save_point(user = user_id,value = neg_effect, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='neg_effect')

        stai_trait = parse_str(row[16])
        save_point(user = user_id,value = stai_trait, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='stai.trait')

        audit = parse_str(row[17])
        save_point(user = user_id,value = audit, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='audit')

        gats_status = str(row[18].strip())
        save_point(user = user_id,value = gats_status, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='gats.status')

        gats_quantity = parse_str(row[19])
        save_point(user = user_id,value = gats_quantity, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='gats.quantity')

        #gats_quantity_sub = parse_str(row[20])
        ipaq = parse_str(row[21])
        save_point(user = user_id,value = ipaq, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='ipaq')

        psqi = parse_str(row[22])
        save_point(user = user_id,value = psqi, 
                   start_time = start_time, 
                   end_time=start_time,
                   offset=utc_offset, 
                   metadata=metadata,
                   stream_name_suffix='psqi')

        
        

def main():
    parse_userid_mappings()
    
    # processing ALC_D
    # ID","StartDate","EndDate","RecordedDate","SurveyType","alc_status","alc.quantity.d"
    for feature in files_to_process[:1]:
        print("PROCESSING %s %s"%(feature[0], feature[1]))
        process_feature(feature[0], feature[1])



if __name__ == '__main__':
    main()

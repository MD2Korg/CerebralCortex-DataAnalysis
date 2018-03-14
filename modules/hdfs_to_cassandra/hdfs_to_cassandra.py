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
import os
import argparse
import pickle
import json
import traceback
from cerebralcortex.core.datatypes.datastream import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.stream_types import StreamTypes
from cerebralcortex.cerebralcortex import CerebralCortex

CC_CONFIG_PATH = '/home/vagrant/CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml'

DATA_DIR = '/home/vagrant/features'

METADATA = '/home/vagrant/mperf_ids.txt'

parser = argparse.ArgumentParser(description='CerebralCortex '
                                     'Imports data from HDFS')
parser.add_argument("-c", "--cc-config", help="Path to file containing the "
                        "CerebralCortex configuration", required=True)
parser.add_argument("-d", "--data-dir", help="Path to dir containing the "
                                      "feature data" , required=True)
parser.add_argument("-m", "--metadata_file", help="Path to the file containing "
                    " the metadata information",  required=True)

args = vars(parser.parse_args())
metadata_map = {}
stream_names = {}

if args['cc_config']:
    CC_CONFIG_PATH = args['cc_config']
if args['data_dir']:
    DATA_DIR = args['data_dir']
if args['metadata_file']:
    METADATA = args['metadata_file']

CC = CerebralCortex(CC_CONFIG_PATH)

def load_metadata(metadata_dir):
    '''
    This method reads all the metadata files in the given directory and loads
    them with key as the stream name in the metadata_map dict. 
    '''
    metadata_files = [os.path.join(metadata_dir,f) for f in os.listdir(metadata_dir)
                      if os.path.isfile(os.path.join(metadata_dir,f))]
    for mf in metadata_files:
        mfp = open(mf,'r')
        metadata_json = json.loads(mfp.read())        
        metadata_map[metadata_json['name']] = metadata_json


def load_streamnames():
    f = open('stream_names.txt','r')
    for line in f:
        id_name = line.split('\t')
        streamid = id_name[0]
        streamname = id_name[1].strip()
        stream_names[streamid] = streamname
    f.close()

def parse_and_save_pickles(root_dir):
    '''
    This method walks the root_dir, loads the pickle files in the sub
    directories. And then imports the loaded pickle files to Cassandra
    The assumption of the directory structure
    root_dir
    |_user_id1
      |_stream_id1
        |_day1.pickle
        |_day2.pickle
          .
          .
        |_dayn.pickle
      |_stream_id2
    |_user_id2
    .
    .
    |_user_idn
    '''
    f = open('streamids.txt','a')
    user_ids = [os.path.join(root_dir,d) for d in os.listdir(root_dir) 
                if os.path.isdir(os.path.join(root_dir,d))]

    for user in user_ids:
        stream_ids = [os.path.join(user,d) for d in os.listdir(user) 
                      if os.path.isdir(os.path.join(user,d))]
        for stream in stream_ids:
            userid_streamid = str(os.path.basename(user)) + '\t' + \
                              str(os.path.basename(stream)) + '\n'
            f.write(userid_streamid)
            pickle_files = [os.path.join(stream,f) for f in os.listdir(stream) 
                          if os.path.isfile(os.path.join(stream, f))]
            for pick in pickle_files:
                pick_fp = open(pick,'rb')
                dps = pickle.load(pick_fp)
                user_id = os.path.basename(user)
                stream_id = os.path.basename(stream)
                stream_name = stream_names[stream_id]
                metadata = metadata_map[stream_name]
                save(stream_id, user_id, stream_name, 
                      metadata['data_descriptor'],
                      metadata['execution_context'],
                      metadata['annotations'],
                      StreamTypes.DATASTREAM,
                      dps) 
                pick_fp.close()
            #print(stream,pickle_files)
    f.close()        
            

def save(identifier, owner, name, data_descriptor, execution_context,
         annotations, stream_type, data):
    ds = DataStream(identifier=identifier, owner=owner, name=name, 
                    data_descriptor=data_descriptor,
                    execution_context=execution_context, 
                    annotations=annotations,
                    stream_type=stream_type, data=data)


    try:
        CC.save_stream(ds)
        print("Saved %d data points"%(len(data)))
    except Exception as e:
       print(traceback.format_exc()) 

#main 
load_metadata(METADATA)
load_streamnames()
parse_and_save_pickles(DATA_DIR)

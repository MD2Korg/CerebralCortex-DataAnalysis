
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
from datetime import datetime
from datetime import timedelta
import traceback
from cerebralcortex.core.datatypes.datastream import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.stream_types import StreamTypes
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.util.spark_helper import get_or_create_sc

CC_CONFIG_PATH = '/home/vagrant/CerebralCortex-DockerCompose/cc_config_file/cc_vagrant_configuration.yml'

STUDY_NAME = 'mperf'

date_format = '%Y%m%d'

parser = argparse.ArgumentParser(description='CerebralCortex '
                                     'Imports data from HDFS')
parser.add_argument("-c", "--cc-config", help="Path to file containing the "
                        "CerebralCortex configuration", required=True)
parser.add_argument("-s", "--study-name", help="Path to dir containing the "
                                      "feature data" , required=True)
parser.add_argument("-sd", "--start-date", help="Start date in " 
                     "YYYYMMDD Format", required=True)
parser.add_argument("-ed", "--end-date", help="End date in " 
                     "YYYYMMDD Format", required=True)

args = vars(parser.parse_args())

if args['cc_config']:
    CC_CONFIG_PATH = args['cc_config']
    print('A'*30,CC_CONFIG_PATH)
if args['study_name']:
    STUDY_NAME = args['study_name']
if args['start_date']:
    start_date = datetime.strptime(args['start_date'], date_format)
if args['end_date']:
    end_date = datetime.strptime(args['end_date'], date_format)

print('B'*30,CC_CONFIG_PATH)
CC = CerebralCortex(CC_CONFIG_PATH)


all_days = []
while True:
    all_days.append(start_date.strftime(date_format))
    start_date += timedelta(days = 1)
    if start_date > end_date : break

all_users = CC.get_all_users(STUDY_NAME) 

all_user_ids = [usr['identifier'] for usr in all_users]

def audit_user_streams(user_id, all_days, cc_config):
    print('X'*100,cc_config)
    CC = CerebralCortex(cc_config)
    all_user_streams = CC.get_user_streams(user_id)
    userbuf=''
    for user_stream_key in all_user_streams:
        user_stream = all_user_streams[user_stream_key]
        
        if 'analysis' not in user_stream['name']:
            continue

        for day in all_days:
            data_points = 0
            for stream_id in user_stream['stream_ids']:
                ds = CC.get_stream(stream_id,user_id,day)
                data_points += len(ds.data)

            buf = '%s\t%s\t%s\t%d\n' % (user_id, user_stream['name'], str(day),
                                       data_points)
            userbuf += buf

    out_dir = '/tmp/data_audit'
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    file_path = os.path.join(out_dir,user_id)
    f = open(file_path,'w')
    f.write(userbuf)
    f.close()


num_cores = 128
spark_context = get_or_create_sc(type="sparkContext")
rdd = spark_context.parallelize(all_user_ids,num_cores)
print('X'*100,CC_CONFIG_PATH)
results = rdd.map(
    lambda user: audit_user_streams(user, all_days, 
                                         CC_CONFIG_PATH))
results.count()


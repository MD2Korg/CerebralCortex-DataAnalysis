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
import utils.config
import argparse
import traceback
from datetime import datetime
from datetime import timedelta

import syslog
from syslog import LOG_ERR
from cerebralcortex.cerebralcortex import CerebralCortex

# Initialize logging
syslog.openlog(ident="CerebralCortex-Driver")

def process_features(feature_list, CC, users, all_days):
    '''
    This method runs the processing pipeline for each of
    the features in the list.
    '''
    # TODO FIXME - should we parallize these as spark jobs ?
    for module in feature_list:
        feature_class_name = getattr(module,'feature_class_name')
        feature_class = getattr(module,feature_class_name)
        feature_class_instance = feature_class(CC)
        try:
            feature_class_instance.process(users[0],all_days)
        except Exception as e:
            #syslog.syslog(LOG_ERR,str(e))
            syslog.syslog(LOG_ERR, str(e) + "\n" + str(traceback.format_exc()))


def discover_features(feature_list):
    '''
    This method discovers all the features that are present.
    '''
    feature_dir = os.path.join(utils.config.FEATURES_DIR_NAME)
    print(feature_dir)
    found_features = []
    if feature_list:
        feature_subdirs = feature_list
    else:
        feature_subdirs = os.listdir(feature_dir)
    
    for subdir in feature_subdirs:
        feature = os.path.join(feature_dir,subdir)
        if not os.path.exists(feature):
            syslog.syslog(LOG_ERR,'Feature not found %s.' % subdir)
            continue
        if os.path.isdir(feature):
            mod_file_path = os.path.join(feature,subdir) + '.py'

            if not os.path.exists(mod_file_path):
                continue
            sys.path.append(feature)
            try:
                module = __import__(subdir)
                found_features.append(module)
                syslog.syslog('Loaded feature %s' % feature)
            except Exception as exp:
                #syslog.syslog(LOG_ERR,str(exp))
                print(str(exp)+"\n"+str(traceback.format_exc()))
                syslog.syslog(LOG_ERR, str(exp) + "\n" + str(traceback.format_exc()))
    
    return found_features
    

def generate_feature_processing_order(feature_list):
    '''
    This method returns the execution order of processing the features 
    after resolving the inter dependencies.
    '''
    return feature_list


def main():
    # Get the list of the features to process
    parser = argparse.ArgumentParser(description='CerebralCortex '
                                     'Feature Processing Driver')
    parser.add_argument("-f", "--feature-list", help="List of feature names "
                         "seperated by commas", required=False)
    parser.add_argument("-c", "--cc-config", help="Path to file containing the "
                         "CerebralCortex configuration", required=True)
    parser.add_argument("-s", "--study-name", help="Study name.", required=True)
    parser.add_argument("-u", "--users", help="Comma separated user uuids", 
                         required=False)
    parser.add_argument("-sd", "--start-date", help="Start date in " 
                         "YYYYMMDD Format", required=True)
    parser.add_argument("-ed", "--end-date", help="End date in " 
                         "YYYYMMDD Format", required=True)
    
    args = vars(parser.parse_args())
    print(args)
    feature_list = None
    cc_config_path = None
    study_name = None 
    users = None
    start_date = None
    end_date = None
    date_format = '%Y%m%d'
    
    if args['feature_list']:
        feature_list = args['feature_list'].split(',')
    if args['cc_config']:
        cc_config_path = args['cc_config']
    if args['study_name']:
        study_name = args['study_name']
    if args['users']:
        users = args['users'].split(',')
    if args['start_date']:
        start_date = datetime.strptime(args['start_date'], date_format)
    if args['end_date']:
        end_date = datetime.strptime(args['end_date'], date_format)
    
    all_days = []
    while True:
        all_days.append(start_date)
        start_date += timedelta(days = 1)
        if start_date > end_date : break

    CC = None
    try:
        CC = CerebralCortex(cc_config_path)
        if not users:
            users = CC.get_all_users(study_name)
            if not users:
                print('USERS',users)
                return
            if not len(users):
                return # no point continuing
        
    except Exception as e:
        print(str(e)
    )

    found_features = discover_features(feature_list)
    feature_to_process = generate_feature_processing_order(found_features)
    process_features(feature_to_process, CC, users, all_days)
    
if __name__ == '__main__':
    main()


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

import syslog
from syslog import LOG_ERR
from core.utils.config import CC_CONFIG_PATH
from cerebralcortex.cerebralcortex import CerebralCortex


# TODO ADD ADMISSION CoNTROL LOGIC

# Initialize logging
syslog.openlog(ident="CerebralCortex-Driver")

'''
This method runs the processing pipeline for each of
the features in the list.
'''
def process_features(feature_list):
# TODO FIXME - should we parallize these as spark jobs ?
    for module in feature_list:
        feature_class_name = getattr(module,'feature_class_name')
        feature_class = getattr(module,feature_class_name)
        feature_class_instance = feature_class()
        try:
            feature_class_instance.process()
        except Exception as e:
            #syslog.syslog(LOG_ERR,str(e))
            syslog.syslog(LOG_ERR, str(e) + "\n" + str(traceback.format_exc()))
'''
This method discovers all the features that are present.
'''
def discover_features(feature_list):
    feature_dir = os.path.join(utils.config.FEATURES_DIR_NAME)
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
                syslog.syslog(LOG_ERR,self.__class__.__name__ + str(exp) + "\n" + str(traceback.format_exc()))
    
    return found_features
    

'''
This method returns the execution order of processing the features 
after resolving the inter dependencies.
'''
def generate_feature_processing_order(feature_list):
    return feature_list


def main():
    # Get the list of the features to process
    parser = argparse.ArgumentParser(description='CerebralCortex Feature Processing Driver')
    parser.add_argument("-f", "--feature-list", help="List of feature names seperated by commas", required=False)
    args = vars(parser.parse_args())
    feature_list = None
    if args['feature_list']:
        feature_list = args['feature_list'].split(',')
    
    found_features = discover_features(feature_list)
    feature_to_process = generate_feature_processing_order(found_features)
    
    CC = CerebralCortex(CC_CONFIG_PATH)

    process_features(feature_to_process)
    
if __name__ == '__main__':
    main()


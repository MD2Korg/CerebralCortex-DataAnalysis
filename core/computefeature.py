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

import traceback
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.log_manager.log_handler import LogTypes
from cerebralcortex.core.datatypes.stream_types import StreamTypes
import uuid
import os
import json

class ComputeFeatureBase(object):
    '''
    This module describes the ComputeFeatureBase class.
    Feature modules should inherit from ComputeFeatureBase.
    '''
    def process(self):
        '''
        Use this method as an entry point for all your computations.
        '''
        pass
    
    def store_stream(self,filepath, input_streams, user_id, data):
        '''
        This method saves the computed DataStreams from different features
        '''
        stream_str = str(filepath) 
        stream_str += str(user_id) 
        stream_str += str(self.__class__.__name__)
        output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, stream_str))
        
        # creating new input_streams list with only the needed information
        input_streams_metadata = []
        for input_strm in input_streams:
            if type(input_strm) != dict:
                self.CC.logging.log('Inconsistent type found in '
                                    'input_streams',str(input_streams))
            stream_info = {}
            stream_info['identifier'] = input_strm['identifier']
            stream_info['name'] = input_strm['name']
            input_streams_metadata.append(stream_info)

        newfilepath = os.path.join(self.CC.feature_metadata_dir, filepath)
        self.CC.logging.log('METADATA file path %s' % (newfilepath))

        with open(newfilepath, "r") as f:
            metadata = f.read()
            metadata = json.loads(metadata)
            metadata["execution_context"]["processing_module"]["input_streams"]\
            = input_streams_metadata
            metadata["identifier"] = str(output_stream_id)
            metadata["owner"] = str(user_id)

            self.store(identifier=output_stream_id, owner=user_id, name=metadata["name"],
                       data_descriptor=metadata["data_descriptor"],
                       execution_context=metadata["execution_context"], annotations=metadata["annotations"],
                       stream_type=StreamTypes.DATASTREAM, data=data)



    def store(self, identifier, owner, name, data_descriptor, execution_context,
              annotations, stream_type=StreamTypes.DATASTREAM, data=None):
        '''
        All store operations MUST be through this method.
        '''
        if not data:
            self.CC.logging.log(error_type=LogTypes.MISSING_DATA, error_message
                                = 'Null data received for '
                                  'saving stream from  ' + self.__class__.__name__)
            return
        
        ds = DataStream(identifier=identifier, owner=owner, name=name, 
                        data_descriptor=data_descriptor,
                        execution_context=execution_context, 
                        annotations=annotations,
                        stream_type=stream_type, data=data)
        try:
            self.CC.save_stream(ds)
            self.CC.logging.log('Saved %d data points from %s' % 
                                 (len(data), self.__class__.__name__))
        except Exception as exp:
            self.CC.logging.log(self.__class__.__name__ + str(exp) + "\n" + 
                          str(traceback.format_exc()))

    def __init__(self, CC = None):
        self.CC = CC


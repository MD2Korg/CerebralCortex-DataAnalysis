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

'''
This module describes the ComputeFeatureBase class.
Feature modules should inherit from ComputeFeatureBase.
'''
import syslog
import traceback
from syslog import LOG_ERR
from core.utils.config import CC_CONFIG_PATH
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream

# Initialize logging
syslog.openlog(ident="CerebralCortex-ComputeFeatureBase")

class ComputeFeatureBase(object):
    '''
    Use this method for all your computations.
    '''
    def process(self):
        pass
    
    '''
    All store operations MUST be through this method.
    '''
    def store(self,identifier,owner,name,data_descriptor,execution_context,annotations,stream_type,data):
        ds = DataStream(identifier=identifier, owner=owner, name=name, data_descriptor=data_descriptor,
                        execution_context=execution_context, annotations=annotations,
                        stream_type=stream_type, data=data)
        try:
            print("Saving Stream",ds)
            self.CC.save_stream(ds)
        except Exception as exp:
            syslog.syslog(LOG_ERR,self.__class__.__name__ + str(exp) + "\n" + str(traceback.format_exc()))

    def __init__(self):
        self.CC = CerebralCortex(CC_CONFIG_PATH)




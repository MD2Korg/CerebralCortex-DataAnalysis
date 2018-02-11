from computefeature import ComputeFeatureBase
import syslog
from syslog import LOG_ERR

# Initialize logging
syslog.openlog(ident="CerebralCortex-ExampleFeature")

feature_class_name='ExampleFeature'

'''
This is an example that demonstrates how to write a feature.
'''
class ExampleFeature(ComputeFeatureBase):
    def helper_function(self):
        pass

    def process(self):
        syslog.syslog("Processing ExampleFeature")
        # Get data streams
        # Apply admission control on your data streams
        # process your data streams, optionally you may define other helper_functions ()
        #     to make your code more readable

        self.helper_function()
        # store your results by calling the store() method in ComputeFeatureBase
        
        


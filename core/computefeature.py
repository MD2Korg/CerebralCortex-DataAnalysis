from utils.config import CC_CONFIG_PATH
from cerebralcortex.cerebralcortex import CerebralCortex

class ComputeFeatureBase(object):
  
  def process(self):
    pass

  def store(self):
    pass

  def __init__(self):
    self.CC = CerebralCortex(CC_CONFIG_PATH)




from cerebralcortex.cerebralcortex import CerebralCortex
from pprint import pprint
from datetime import timedelta
import numpy as np
import pandas as pd
import os
import pickle
from util_get_store import get_stream_days
CC = CerebralCortex()
users = CC.get_all_users("mperf-alabsi")
for user in users:

    print(user['identifier'])
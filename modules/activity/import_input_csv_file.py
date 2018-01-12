import pandas as pd
import uuid
from _datetime import datetime
import pytz

from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream

def getData(cur_dir, filename):
    col_name = ['timestamp', 'x', 'y', 'z']
    D = pd.read_csv(cur_dir + filename, names = col_name)

    return D['timestamp'], D['x'], D['y'], D['z']

def convert_sample(sample):
    return list([float(x.strip()) for x in sample.split(',')])


def line_parser(input):
    ts, offset, sample = input.split(',', 2)
    start_time = int(ts) / 1000.0
    offset = int(offset)
    return DataPoint(datetime.fromtimestamp(start_time), convert_sample(sample))


def getInputData(cur_dir):

    t, Ax, Ay, Az = getData(cur_dir, 'right-wrist-accelxyz.csv')

    return t, Ax, Ay, Az

def getInputDataStream(cur_dir):

    t, Ax, Ay, Az = getInputData(cur_dir)
    accel_data = []
    for i in range(len(t)):
        accel_data.append(DataPoint.from_tuple(start_time=datetime.fromtimestamp(t.iloc[i]/1000, pytz.timezone('US/Central')), sample = [Ax.iloc[i], Ay.iloc[i], Az.iloc[i]]))

    start_time = accel_data[0].start_time
    end_time = accel_data[-1].start_time

    identifier = 'posture_detection'
    owner = uuid.UUID('{00010203-0405-0607-0809-0a0b0c0d0e0f}')
    execution_context = {}
    annotations = {}
    data_descriptor = []

    accel_stream = DataStream(identifier, owner, 'accel', data_descriptor,
                                  execution_context,
                                  annotations,
                                  "1",
                                  start_time,
                                  end_time,
                                  accel_data)

    return accel_stream

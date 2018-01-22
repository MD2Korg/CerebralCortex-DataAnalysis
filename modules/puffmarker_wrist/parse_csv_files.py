import pandas as pd
import uuid
from _datetime import datetime

from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream

def getData(cur_dir, filename):
    col_name = ['timestamp', 'value']
    D = pd.read_csv(cur_dir + filename, names = col_name)

    return D['timestamp'], D['value']

def convert_sample(sample):
    return list([float(x.strip()) for x in sample.split(',')])

def line_parser(input):
    ts, offset, sample = input.split(',', 2)
    start_time = int(ts) / 1000.0
    offset = int(offset)
    return DataPoint(datetime.fromtimestamp(start_time), convert_sample(sample))

def getGroundTruthInputData(cur_dir, wrist):
    epi_st, epi_et = getData(cur_dir, 'episode_start_end.csv')

    epi_st = [datetime.fromtimestamp(t/1000) for t in epi_st]
    epi_et = [datetime.fromtimestamp(t/1000) for t in epi_et]

    if wrist == 0:
        puff_times = pd.read_csv(cur_dir + 'puff_timestamp_leftwrist.csv', names=['timings'])
    else:
        puff_times = pd.read_csv(cur_dir + 'puff_timestamp_rightwrist.csv', names=['timings'])
    puff_times = puff_times['timings']
    puff_times = [datetime.fromtimestamp(t/1000) for t in puff_times.values]

    return epi_st, epi_et, puff_times

def getInputData(cur_dir, wrist):

    epi_st, epi_et = getData(cur_dir, 'episode_start_end.csv')

    if wrist == 0:
        puff_times = pd.read_csv(cur_dir + 'puff_timestamp_leftwrist.csv', names=['timings'])
        puff_times = puff_times['timings']
        puff_times = puff_times.values

        t, Ax = getData(cur_dir, 'left-wrist-accelx.csv')
        t, Ay = getData(cur_dir, 'left-wrist-accely.csv')
        t, Az = getData(cur_dir, 'left-wrist-accelz.csv')

        t, Gx = getData(cur_dir, 'left-wrist-gyrox.csv')
        t, Gy = getData(cur_dir, 'left-wrist-gyroy.csv')
        t, Gz = getData(cur_dir, 'left-wrist-gyroz.csv')
    else:
        puff_times = pd.read_csv(cur_dir + 'puff_timestamp_rightwrist.csv', names=['timings'])
        puff_times = puff_times['timings']
        puff_times = puff_times.values

        t, Ax = getData(cur_dir, 'right-wrist-accelx.csv')
        t, Ay = getData(cur_dir, 'right-wrist-accely.csv')
        t, Az = getData(cur_dir, 'right-wrist-accelz.csv')

        t, Gx = getData(cur_dir, 'right-wrist-gyrox.csv')
        t, Gy = getData(cur_dir, 'right-wrist-gyroy.csv')
        t, Gz = getData(cur_dir, 'right-wrist-gyroz.csv')

    return t, Ax, Ay, Az, Gx, Gy, Gz, epi_st, epi_et, puff_times

def getInputDataStream(cur_dir, wrist):

    t, Ax, Ay, Az, Gx, Gy, Gz, epi_st, epi_et, puff_times = getInputData(cur_dir, wrist)
    accel_data = []
    gyro_data = []
    for i in range(len(t)):
        accel_data.append(DataPoint.from_tuple(start_time=datetime.fromtimestamp(t.iloc[i]/1000), sample = [Ax.iloc[i], Ay.iloc[i], Az.iloc[i]]))
        gyro_data.append(DataPoint.from_tuple(start_time=datetime.fromtimestamp(t.iloc[i]/1000), sample = [Gx.iloc[i], Gy.iloc[i], Gz.iloc[i]]))

    start_time = accel_data[0].start_time
    end_time = accel_data[-1].start_time

    identifier = 'smoking_detection'
    owner = uuid.UUID('{00010203-0405-0607-0809-0a0b0c0d0e0f}')
    execution_context = {}
    annotations = {}
    data_descriptor = []

    accel_datastream = DataStream(identifier, owner, 'accel', data_descriptor,
                                  execution_context,
                                  annotations,
                                  "1",
                                  start_time,
                                  end_time,
                                  accel_data)
    gyro_datastream = DataStream(identifier, owner, 'gyro', data_descriptor,
                                 execution_context,
                                 annotations,
                                 "1",
                                 start_time,
                                 end_time,
                                 accel_data)

    return accel_datastream, gyro_datastream

import pandas as pd
import numpy as np
import os
from modules.puffmarker_wrist.parse_csv_files import getGroundTruthInputData, getInputDataStream
from modules.puffmarker_wrist.wrist_features import compute_wrist_feature


def getLabel(st, et, epi_st, epi_et, puff_times):
    label = 0  # not puff
    #     print(range(len(puff_times)))
    for i in range(len(puff_times)):
        if (puff_times[i] >= st) & (puff_times[i] <= et):
            label = 1
            return label

    for i in range(len(epi_et)):
        if ((epi_st[i] <= st) & (st <= epi_et[i])) | ((epi_st[i] <= et) & (et <= epi_et[i])):
            label = -1  # included episode but not puff
            return label
    return label


if __name__ == '__main__':

    fastSize = 13
    slowSize = 131

    data_dir = '/home/nsaleheen/data/csvdataSI_new/'
    pids = ['p01', 'p02', 'p03', 'p04', 'p05', 'p06']
    # pids = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]

    Xs = []
    Ys = []

    nPuff = 0
    nPuffCand = 0
    nNonPuffCand = 0


    for i in range(len(pids)):
        basedir = data_dir + pids[i] + '/'
        sids = [d for d in os.listdir(basedir) if os.path.isdir(os.path.join(basedir, d))]

        for j in range(len(sids)):
            cur_dir = data_dir + pids[i] + '/' + sids[j] + '/'
            print(cur_dir)

            for wrist in range(2):  # 0 for left wrist, 1 for right wrist

                epi_st, epi_et, puff_times = getGroundTruthInputData(cur_dir, wrist)
                nPuff = nPuff + len(puff_times)

                accel_stream, gyro_stream = getInputDataStream(cur_dir, wrist)
                print('success -- input')

                all_features_stream = compute_wrist_feature(accel_stream, gyro_stream)

                st = [f.start_time for f in all_features_stream.data]
                et = [f.end_time for f in all_features_stream.data]
                labels = [0] * len(st)

                for k in range(len(all_features_stream.data)):
                    labels[k] = getLabel(st[k], et[k], epi_st, epi_et, puff_times)
                    if labels[k] != -1:
                        Xs.append(all_features_stream.data[k])
                        if labels[k] == 0:
                            Ys.append('non_puff')
                            nNonPuffCand = nNonPuffCand + 1
                        else:
                            Ys.append('puff')
                            nPuffCand = nPuffCand + 1

    print("# of puffs = ", nPuff)
    print("# of puff candidates= ", nPuffCand)
    print("# of non-puff candidates= ", nNonPuffCand)

    #     print(Xs)
    #     print(Ys)

    # Xs = np.array([x.sample for x in Xs])
    # Ys = np.array([Ys])
    # M = np.concatenate((Xs, Ys.T), axis=1)
    #
    # df = pd.DataFrame(M)
    # feature_name = ['duration', 'roll_mean', 'roll_median', 'roll_sd', 'roll_quartile', 'pitch_mean', 'pitch_median', 'pitch_sd', 'pitch_quartile', 'yaw_mean', 'yaw_median', 'yaw_sd', 'yaw_quartile', 'gyro_mean', 'gyro_median', 'gyro_sd', 'gyro_quartile', 'label']
    # # feature_name = ['duration', 'roll_mean', 'roll_median', 'roll_sd', 'roll_quartile', 'pitch_mean', 'pitch_median', 'pitch_sd', 'pitch_quartile', 'yaw_mean', 'yaw_median', 'yaw_sd', 'yaw_quartile', 'gyro_mean', 'gyro_median', 'gyro_sd', 'gyro_quartile', 'label(1:puff; 0:non-puff)']
    # df.to_csv(data_dir + "/puffmarker_wrist_features_py.csv", header=feature_name)

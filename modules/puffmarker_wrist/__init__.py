from modules.puffmarker_wrist.wrist_features import compute_wrist_feature


def get_input_streams():

    accel_stream=[]
    gyro_stream = []

    return accel_stream, gyro_stream

if __name__ == '__main__':

    accel_stream, gyro_stream = get_input_streams()

    all_features = compute_wrist_feature(accel_stream, gyro_stream)

    puff_labels = predict_puff(all_features)






import numpy as np
from cerebralcortex.core.datatypes.datastream import DataStream

def filterDuration(gyr_intersections: DataStream):
    gyr_intersections_filtered = []

    for I in gyr_intersections.data:
        dur = (I.end_time - I.start_time).total_seconds()
        if (dur >= 1.0) & (dur <= 5.0):
            gyr_intersections_filtered.append(I)

    gyr_intersections_filtered_datastream = DataStream.from_datastream([gyr_intersections])
    gyr_intersections_filtered_datastream.data = gyr_intersections_filtered
    return gyr_intersections_filtered_datastream

def filterRollPitch(gyr_intersections_stream: DataStream, roll_stream: DataStream, pitch_stream: DataStream):
    gyr_intersections_filtered = []

    for I in gyr_intersections_stream.data:
        sTime = I.start_time
        eTime = I.end_time
        sIndex = I.sample[0]
        eIndex = I.sample[1]

        roll_sub = [roll_stream.data[i].sample for i in range(sIndex, eIndex)]
        pitch_sub = [pitch_stream.data[i].sample for i in range(sIndex, eIndex)]

        mean_roll = np.mean(roll_sub)
        mean_pitch = np.mean(pitch_sub)

        #         r > -20 && r <= 65 && p >= -125 && p <= -40
        if (mean_roll > -20) & (mean_roll <= 65) & (mean_pitch >= - 125) & (mean_pitch <= - 40):
            gyr_intersections_filtered.append(I)

    gyr_intersections_filtered_stream = DataStream.from_datastream([gyr_intersections_stream])
    gyr_intersections_filtered_stream.data = gyr_intersections_filtered
    return gyr_intersections_filtered_stream

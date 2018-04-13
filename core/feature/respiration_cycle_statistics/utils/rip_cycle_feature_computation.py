# -*- coding: utf-8 -*-
"""
Created on Sat Mar 10 23:19:15 2018

@author: Nazir Saleheen, Md Azim Ullah
"""

from typing import List
from cerebralcortex.core.datatypes.datapoint import DataPoint

def rip_cycle_feature_computation(peaks_datastream: List[DataPoint],
                                  valleys_datastream: List[DataPoint]):
    """
    Respiration Feature Implementation. The respiration feature values are
    derived from the following paper:
    'puffMarker: a multi-sensor approach for pinpointing the timing of first lapse in smoking cessation'
    Removed due to lack of current use in the implementation
    roc_max = []  # 8. ROC_MAX = max(sample[j]-sample[j-1])
    roc_min = []  # 9. ROC_MIN = min(sample[j]-sample[j-1])
    :param peaks_datastream: list of peak datapoints
    :param valleys_datastream: list of valley datapoints
    :return: lists of DataPoints each representing a specific feature calculated from the respiration cycle
    found from the peak valley inputs 
    """


    inspiration_duration = []  # 1 Inhalation duration
    expiration_duration = []  # 2 Exhalation duration
    respiration_duration = []  # 3 Respiration duration
    inspiration_expiration_ratio = []  # 4 Inhalation and Exhalation ratio
    stretch = []  # 5 Stretch
    upper_stretch = []  # 6. Upper portion of the stretch calculation
    lower_stretch = []  # 7. Lower portion of the stretch calculation
    delta_previous_inspiration_duration = []  # 10. BD_INSP = INSP(i)-INSP(i-1)
    delta_previous_expiration_duration = []  # 11. BD_EXPR = EXPR(i)-EXPR(i-1)
    delta_previous_respiration_duration = []  # 12. BD_RESP = RESP(i)-RESP(i-1)
    delta_previous_stretch_duration = []  # 14. BD_Stretch= Stretch(i)-Stretch(i-1)
    delta_next_inspiration_duration = []  # 19. FD_INSP = INSP(i)-INSP(i+1)
    delta_next_expiration_duration = []  # 20. FD_EXPR = EXPR(i)-EXPR(i+1)
    delta_next_respiration_duration = []  # 21. FD_RESP = RESP(i)-RESP(i+1)
    delta_next_stretch_duration = []  # 23. FD_Stretch= Stretch(i)-Stretch(i+1)
    neighbor_ratio_expiration_duration = []  # 29. D5_EXPR(i) = EXPR(i) / avg(EXPR(i-2)...EXPR(i+2))
    neighbor_ratio_stretch_duration = []  # 32. D5_Stretch = Stretch(i) / avg(Stretch(i-2)...Stretch(i+2))

    valleys = valleys_datastream
    peaks = peaks_datastream[:-1]

    for i, peak in enumerate(peaks):
        valley_start_time = valleys[i].start_time
        valley_end_time = valleys[i+1].start_time
        
        delta = peak.start_time - valleys[i].start_time
        inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time,
                                                         sample=delta.total_seconds(),
                                                         end_time=valley_end_time))
        delta = valleys[i + 1].start_time - peak.start_time
        expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time,
                                                        sample=delta.total_seconds(),
                                                        end_time=valley_end_time))

        delta = valleys[i + 1].start_time - valley_start_time
        respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time,
                                                         sample=delta.total_seconds(),
                                                         end_time=valley_end_time))

        ratio = (peak.start_time - valley_start_time) / (valleys[i + 1].start_time - peak.start_time)
        inspiration_expiration_ratio.append(DataPoint.from_tuple(start_time=valley_start_time,
                                                                 sample=ratio,
                                                                 end_time=valley_end_time))

        value = peak.sample - valleys[i + 1].sample
        stretch.append(DataPoint.from_tuple(start_time=valley_start_time,
                                            sample=value,
                                            end_time=valley_end_time))

    for i,point in enumerate(inspiration_duration):
        valley_start_time = valleys[i].start_time
        valley_end_time = valleys[i+1].start_time
        if i == 0:  # Edge case
            delta_previous_inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0,
                                                                           end_time=valley_end_time))
            delta_previous_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0,
                                                                          end_time=valley_end_time))
            delta_previous_respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0,
                                                                           end_time=valley_end_time))
            delta_previous_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0,
                                                                       end_time=valley_end_time))
        else:
            delta = inspiration_duration[i].sample - inspiration_duration[i - 1].sample
            delta_previous_inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta,
                                                                           end_time=valley_end_time))

            delta = expiration_duration[i].sample - expiration_duration[i - 1].sample
            delta_previous_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta,
                                                                          end_time=valley_end_time))

            delta = respiration_duration[i].sample - respiration_duration[i - 1].sample
            delta_previous_respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta,
                                                                           end_time=valley_end_time))

            delta = stretch[i].sample - stretch[i - 1].sample
            delta_previous_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta,
                                                                       end_time=valley_end_time))

        if i == len(inspiration_duration) - 1:
            delta_next_inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0,
                                                                       end_time=valley_end_time))
            delta_next_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0,
                                                                      end_time=valley_end_time))
            delta_next_respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0,
                                                                       end_time=valley_end_time))
            delta_next_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0,
                                                                   end_time=valley_end_time))
        else:
            delta = inspiration_duration[i].sample - inspiration_duration[i + 1].sample
            delta_next_inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta,
                                                                       end_time=valley_end_time))

            delta = expiration_duration[i].sample - expiration_duration[i + 1].sample
            delta_next_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta,
                                                                      end_time=valley_end_time))

            delta = respiration_duration[i].sample - respiration_duration[i + 1].sample
            delta_next_respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta,
                                                                       end_time=valley_end_time))

            delta = stretch[i].sample - stretch[i + 1].sample
            delta_next_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta,
                                                                   end_time=valley_end_time))

        stretch_average = 0
        expiration_average = 0
        count = 0.0
        for j in [-2, -1, 1, 2]:
            if i + j < 0 or i + j >= len(inspiration_duration):
                continue
            stretch_average += stretch[i + j].sample
            expiration_average += expiration_duration[i + j].sample
            count += 1

        stretch_average /= count
        expiration_average /= count

        ratio = stretch[i].sample / stretch_average
        neighbor_ratio_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=ratio,
                                                                   end_time=valley_end_time))

        ratio = expiration_duration[i].sample / expiration_average
        neighbor_ratio_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=ratio,
                                                                      end_time=valley_end_time))

    # Begin assembling datastream for output
    inspiration_duration_datastream = inspiration_duration

    expiration_duration_datastream = expiration_duration

    respiration_duration_datastream = respiration_duration

    inspiration_expiration_ratio_datastream = inspiration_expiration_ratio

    stretch_datastream = stretch

    upper_stretch_datastream = upper_stretch

    lower_stretch_datastream = lower_stretch

    delta_previous_inspiration_duration_datastream = delta_previous_inspiration_duration

    delta_previous_expiration_duration_datastream = delta_previous_expiration_duration

    delta_previous_respiration_duration_datastream = delta_previous_respiration_duration

    delta_previous_stretch_duration_datastream = delta_previous_stretch_duration

    delta_next_inspiration_duration_datastream = delta_next_inspiration_duration

    delta_next_expiration_duration_datastream = delta_next_expiration_duration

    delta_next_respiration_duration_datastream = delta_next_respiration_duration

    delta_next_stretch_duration_datastream = delta_next_stretch_duration

    neighbor_ratio_expiration_datastream = neighbor_ratio_expiration_duration

    neighbor_ratio_stretch_datastream = neighbor_ratio_stretch_duration

    return peaks_datastream, \
           valleys_datastream,\
           inspiration_duration_datastream, \
           expiration_duration_datastream, \
           respiration_duration_datastream, \
           inspiration_expiration_ratio_datastream, \
           stretch_datastream, \
           delta_previous_inspiration_duration_datastream, \
           delta_previous_expiration_duration_datastream, \
           delta_previous_respiration_duration_datastream, \
           delta_previous_stretch_duration_datastream, \
           delta_next_inspiration_duration_datastream, \
           delta_next_expiration_duration_datastream, \
           delta_next_respiration_duration_datastream, \
           delta_next_stretch_duration_datastream, \
           neighbor_ratio_expiration_datastream, \
           neighbor_ratio_stretch_datastream
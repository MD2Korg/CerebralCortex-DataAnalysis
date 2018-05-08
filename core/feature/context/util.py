def filter_data(data, start_time, end_time):
    subset_data = []
    if len(data)>0:
        for dp in data:
            if (dp.start_time and dp.end_time) and dp.start_time>=start_time and dp.end_time<=end_time:
                subset_data.append(dp)
    return subset_data


def get_home_work_location(data, start_time):
    subset_data = []
    val = "undefined"
    if len(data)>0:
        for dp in data:
            if (dp.start_time and dp.end_time) and (dp.start_time<=start_time or dp.end_time>=start_time):
                subset_data.append(dp.sample)
    if len(subset_data)>0:
        val = max(set(subset_data), key=subset_data.count)
    return val

def get_places(data, start_time):
    subset_data = []
    if len(data)>0:
        for dp in data:
            if (dp.start_time and dp.end_time) and (dp.start_time<=start_time or dp.end_time>=start_time):
                subset_data.append(dp.sample)
    return subset_data

def get_phone_physical_activity_data(data, start_time, end_time):
    sample_val = []
    val = 0
    if len(data)>0:
        for dp in data:
            if dp.start_time and dp.start_time>=start_time and dp.start_time<=end_time:
                sample_val.append(dp.sample[0])
        if len(sample_val)>0:
            val = round(sum(sample_val) / float(len(sample_val)))

    return val
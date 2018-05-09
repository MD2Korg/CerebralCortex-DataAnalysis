from datetime import timedelta


def filter_data(data, start_time, end_time):
    subset_data = []
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                subset_data.append(dp)
    return subset_data


def get_home_work_location(data, start_time, end_time):
    subset_data = []
    val = "undefined"
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                subset_data.append(dp.sample)
    if len(subset_data) > 0:
        val = max(set(subset_data), key=subset_data.count)
    return val


def get_places(data, start_time, end_time):
    subset_data = []
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                subset_data.append(dp.sample)
    return subset_data


def get_phone_physical_activity_data(data, start_time, end_time):
    sample_val = []
    val = 0
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                sample_val.append(dp.sample[0])
        if len(sample_val) > 0:
            val = round(sum(sample_val) / float(len(sample_val)))

    return val


def is_talking(data, start_time, end_time):
    sample_val = []
    val = 0
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                sample_val.append(dp.sample)
        if len(sample_val) > 0:
            val = round(sum(sample_val) / float(len(sample_val)))
        if val > 0:
            return True
        else:
            return False
    return False


def is_on_sms(data, start_time, end_time):
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                return True
    return False


def is_on_phone(data, start_time, end_time):
    if len(data) > 0:
        for dp in data:
            dp_start_time = dp.start_time
            if dp.sample is not None:
                dp_end_time = dp_start_time + timedelta(minutes=dp.sample)
            if in_time_range(dp_start_time, dp_end_time, start_time, end_time):
                return True
    return False


def is_on_social_app(data, start_time, end_time):
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                if dp.sample == "Social":
                    return True
    return False


def get_physical_activity_wrist_sensor(data, start_time, end_time):
    subset_data = []
    val = 0
    if len(data) > 0:
        for dp in data:
            if in_time_range(dp.start_time, dp.end_time, start_time, end_time):
                subset_data.append((str(dp.sample).lower()))
        if len(subset_data) > 0:
            val = max(set(subset_data), key=subset_data.count)
    return val


def in_time_range(dp_start_time, dp_end_time, start_time, end_time):
    if dp_start_time is None or dp_end_time is None or start_time is None or end_time is None:
        return False
    elif (dp_start_time <= start_time and start_time <= dp_end_time) or (
            dp_start_time <= end_time and end_time <= dp_end_time):
        return True
    else:
        False

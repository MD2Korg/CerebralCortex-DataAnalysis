import uuid
from datetime import timedelta
from typing import List
from cerebralcortex.cerebralcortex import CerebralCortex

def get_stream_days(stream_id: uuid, CC: CerebralCortex) -> List:
    """
    Returns a list of days (string format: YearMonthDay (e.g., 20171206)
    :param stream_id:
    """
    stream_dicts = CC.get_stream_duration(stream_id)
    stream_days = []
    days = stream_dicts["end_time"]-stream_dicts["start_time"]
    for day in range(days.days+1):
        stream_days.append((stream_dicts["start_time"]+timedelta(days=day)).strftime('%Y%m%d'))
    return stream_days


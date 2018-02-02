from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream

minimum_number_of_puffs = 4
minimum_puff_distance = 2  # seconds
min_smoking_epi_duration = 5  # minutes


def generate_smoking_episodes(puffs_stream: DataStream):
    puffs_data = [v for v in puffs_stream.data if v.sample > 0]
    smoking_epis = []
    i = 0
    while i < len(puffs_data):

        if puffs_data[i + 1].start_time - puffs_data[i].start_time < minimum_puff_distance:
            j = i + 1
            while puffs_data[j].start_time - puffs_data[j - 1].start_time < minimum_puff_distance:
                j = j + 1
            if j-i > minimum_number_of_puffs:
                smoking_epis.append(DataPoint(puffs_data[i].start_time))

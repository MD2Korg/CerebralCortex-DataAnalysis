"""
Takes as input raw datastreams from motionsenseHRV and decodes them 
to get the Accelerometer, Gyroscope, PPG, Sequence number timeseries. 
Last of all it does timestamp correction on all the timeseries and saves them. 

..algorithm:
    Input:
        Raw datastream of motionsenseHRV and motionsenseHRV+
        Each datapoint contains a 20 byte array that was transmitted to the mobile phone by the sensors itself
    Steps:
        1. Decode accelerometer,gyroscope,ppg and sequence number timeseries from the raw datastreams
        2. Timestamp correction based on the sequence number timeseries
        3. Store the timestamp corrected timeseries
    Output:
        motionsenseHRV decoded datastream. Each datapoint sample contains a list of 9 values with the first 3 
        corresponding to accelerometer, next three corresponding to gyroscope and the last three are Red, Infrered,
        Green channels of PPG
"""
"""
Extracts the pre computed rr interval timeseries and computes a continuous heart rate timeseries 
with a resolution of two seconds

..algorithm:
    Input:
        RR interval datastream, each datapoint representing one minute of data and contains the followings things
            1. A list of RR-interval array. Each entry in the list corresponds to a realization of the position of R peaks 
            in that minute
            2. Standard Deviation of Heart Rate within the minute
            3. A list corresponding to the heart rate values calculated from variable realizations of the RR interval on a 
            sliding window of window size = 8 second and window offset = 2 second.
    Steps:
        1. Extract the RR interval data
        2. For every minute of RR interval data unpack the list in the 3rd index to the whole minute to compute a heart 
        rate datastream
        3. store the heart rate datastream
    Output:
        Heart rate datastream

"""
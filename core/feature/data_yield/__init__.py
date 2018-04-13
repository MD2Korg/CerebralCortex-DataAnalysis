"""
Calculates the the motionsenseHRV data yield for each minute of the day when data is present
rendering a list of datapoints. each datapoint contains a boolean decision indicating if the 
sensor was worn or not

..algorithm:
    Input:
        MotionsenseHRV or MotionsenseHRV+ raw datastream
    Steps:
        1. Decode the raw datastream to get the ppg signal
        2. Windowing of PPG signals on a window size of 60 secs
        3. For every 10 seconds of the 60 seconds window determine if the sensor was worn or 
        not rendering at most 6 decisions for a single minute length window 
        4. Take the majority of the decisions in the minute length window as the final decision 
        of the window
        5. store the datastream
    Output:
        Datastream containing a list of datapoints where each datapoint represents a minute of 
        the day where sensor data is present and the sample is a boolean value representing if 
        the sensor was worn or not  
"""
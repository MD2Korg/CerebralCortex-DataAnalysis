"""
Takes the raw datastream of motionsenseHRV and motionsenseHRV+ which contains a byte array 
in each DataPoint and decodes them to get the PPG signal in RED,INFRARED,GREEN channel. This is done for
both left and right/only left/only right sensors whichever is applicable for the person wearing the sensor suite.
Depending on the presence of PPG signal it then tries to combine information of both the wrists in a one minute 
window basis. Then a subspace based method is applied to generate the initial likelihood of the presence of R-peaks in 
the ppg signals. This likelihood array is then used to compute the R-peaks through an Bayesian IP algorithm. 

Algorithm::

    Input:
        Raw datastream of motionsenseHRV and motionsenseHRV+
        Each DataPoint contains a 20 byte array that was transmitted to the mobile phone by the sensors itself

    Steps:
        1. Decodes the raw datastream to get the PPG signals of Red, Infrared, Green channels. Done for both the wrists.
        2. windows ppg from both the wrists on a single windowing scheme of window size=60 secs and window offset=60s.
        3. For every minute of ppg data from both the wrist combine them through interpolation on time axis. It omits 
        those minutes in which the ppg signal does not conform to the standards set beforehand basically rendering a 
        decision of sensor not being worn in any of the wrists.  
        4. For every minute of data found after combining the information from both the wrists computes the initial 
        likelihood of having a R-peak at every timestamp.
        5. Pass the initial likelihood found to the Bayesian IP algorithm to generate the necessary RR interval 
        statistics calculated for each minute

    Output:
        RR interval datastream, each DataPoint representing one minute of data and contains the followings things
            1. A list of RR-interval array. Each entry in the list corresponds to a realization of the position of R peaks 
            in that minute
            2. Standard Deviation of Heart Rate within the minute
            3. A list corresponding to the heart rate values calculated from variable realizations of the RR interval on a 
            sliding window of window size = 8 second and window offset = 2 second.
"""
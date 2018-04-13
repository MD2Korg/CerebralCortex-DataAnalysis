"""
Applies a pretrained Support Vector Machine model with radial basis kernel to one minute of 
respiration cycle features and produces a binary output of Stress/Not Stressed.

..algorithm:
    Input:
        1. Respiration cycle feature datastream: Contains a list of datapoints each representing a respiration cycle 
        and a list of 21 features calculated from each cycle
    Steps:
        1. Implement a non-overlapping windowing with window-size=60 seconds on the input datastream.
        2. Construct a matrix of size (m,14) from each one minute window where m is the number of respiration 
        cycles found in this window
        4. Take median of each column on the matrix for each window generating a (1,14) shaped feature row for every 
        one minute
        5. Transform the feature rows with a standard transformation.
        6. Apply the support vector machine model to predict the output.
    Output:
        1. A 0 or 1 binary value for each minute of data with 0 representing that the person was not stressed and 
        1 representing stress for the minute.
     
    Model Description:
        Support vector machine model was trained with python scikit-learn library with 21 participants data.
        The hyperparameters of the model are:
            1. C = 10.0
            2. Gamma = 0.01
    Input Feature Description:
        The model takes 14 input features as listed:
            1.  inspiration_duration
            2.  expiration_duration
            3.  respiration_duration
            4.  inspiration_expiration_duration_ratio
            5.  stretch
            6.  inspiration_velocity
            7.  expiration_velocity
            8.  skewness
            9.  kurtosis
            10.  entropy
            11.  inspiration_expiration_velocity_ratio
            12.  inspiration_expiration_area_ratio
            13.  expiration_respiration_duration_ratio
            14.  resspiration_area_inspiration_duration_ratio
    References:
        The model trained from respiration here to compute stress has its theoretical underpinnings 
        described in the following paper:
        K. Hovsepian, M. al’Absi, E. Ertin, T. Kamarck, M. Nakajima, and S. Kumar, 
        "cStress: Towards a Gold Standard for Continuous Stress Assessment in the Mobile Environment," 
        ACM UbiComp, pp. 493-504, 2015.


        
"""
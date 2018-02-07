from madgwickahrs import *
from quaternion import *
import math

def degree_to_radian(deg):
    return math.pi * deg / 180.0
    
def Quatern_Conjugate(q):
    quaternion_conj = [-q[0], -q[1], -q[2], q[3]]
    return quaternion_conj

def Quaternion_multiplication(Q2,Q1):
# be aware quaternion multiplication does not commute
# here the code is calculating Q2 * Q1
    Q3 = [0]*4
    Q3[0] = Q1[3]*Q2[0] + Q1[0]*Q2[3] - Q1[1]*Q2[2] + Q1[2]*Q2[1]
    Q3[1] = Q1[3]*Q2[1] + Q1[0]*Q2[2] + Q1[1]*Q2[3] - Q1[2]*Q2[0]
    Q3[2] = Q1[3]*Q2[2] - Q1[0]*Q2[1] + Q1[1]*Q2[0] + Q1[2]*Q2[3]
    Q3[3] = Q1[3]*Q2[3] - Q1[0]*Q2[0] - Q1[1]*Q2[1] - Q1[2]*Q2[2]
    return Q3

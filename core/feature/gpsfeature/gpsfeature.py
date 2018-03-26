
# Copyright (c) 2018, MD2K Center of Excellence
# - Anik Khan <aniknagato@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.



from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint

from datetime import datetime, timedelta
import pprint as pp
import numpy as np

from math import radians, cos, sin, asin, sqrt


import pdb
import uuid

import json

CC = CerebralCortex('/home/md2k/cc_configuration.yml')


feature_class_name = 'GPSFeatures'


class GPSFeatures(ComputeFeatureBase):
    def haversine(self,lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        km = 6373 * c
        return km




    def total_distance_covered(self,data):
        """
        :param data: datapoint array of centroid stream
        :return: total distance covered by participant in kilometers 
        :stream name: org.md2k.data_analysis.feature.total_distance_covered
        """

        name = 'TOT DIST COV'
        execution_context = {}
        annotations = {}
        data_descriptor = [{"NAME":"total distance covered", "DATA_TYPE":"float", "DESCRIPTION": "total distance covered in km"}]
        total_distance = 0
        data_without_transit = []

        for dp in data:
            if ( float(dp.sample[0]) != -1.0):
                data_without_transit.append(dp)

        i = 0
        while i <= len(data_without_transit)-2:



            lattitude_pre = float(data_without_transit[i].sample[0])
            longitude_pre = float(data_without_transit[i].sample[1])

            lattitude_post = float(data_without_transit[i+1].sample[0])
            longitude_post = float(data_without_transit[i+1].sample[1])

            distance = self.haversine(longitude_pre,lattitude_pre,longitude_post,lattitude_post)
            #print (distance)
            total_distance = total_distance + distance
            i += 1


        start_time = data[0].start_time
        end_time = data[-1].end_time
        offset = data[0].offset

        total_distance_datapoint = DataPoint(start_time,end_time,offset,total_distance)

        return [total_distance_datapoint]



    def maximum_distance_between_two_locations(self,data):
        """
        :param data: datapoint array of centroid stream
        :return: maximum distance between two locations covered by participant in kilometers 
        :stream name: org.md2k.data_analysis.feature.maximum_distance_between_two_locations
        """

        name = 'MAX DIST BET TWO LOCS'
        data_descriptor = [{"NAME":"maximum distance between two locations", "DATA_TYPE":"float", "DESCRIPTION": "maximum distance between two locations in km"}]    
        data_without_transit = []

        for dp in data:
            if (float(dp.sample[0]) != -1.0):
                data_without_transit.append(dp)


        max_dist_bet_two_locations = 0
        i=0
        j=0
        while i < len(data_without_transit):

            while j < len(data_without_transit):


                dist_bet_i_j = self.haversine(float(data_without_transit[i].sample[1]),float(data_without_transit[i].sample[0]),float(data_without_transit[j].sample[1]),float(data_without_transit[j].sample[0]))
                #print (dist_bet_i_j)
                if( dist_bet_i_j > max_dist_bet_two_locations):
                    max_dist_bet_two_locations = dist_bet_i_j
                j=j+1
            i=i+1

        start_time = data[0].start_time
        end_time = data[-1].end_time
        offset = data[0].offset
        max_distance_datapoint = DataPoint(start_time,end_time,offset,max_dist_bet_two_locations)

        return [max_distance_datapoint]



    def number_of_different_places(self,data):
        """
        :param data: datapoint array of centroid stream
        :return: number of different places the participant visited in a day
        :stream name: org.md2k.data_analysis.feature.number_of_different_places
        """

        name = 'Num of Diff Pls'
        data_descriptor = [{"NAME":"number of different places", "DATA_TYPE":"int", "DESCRIPTION": "number of different places"}]

        num_diff_places = 0

        loc_array=[]

        ii = 0
        while ii < len(data):
            if(float(data[ii].sample[0]) == -1.0 ):
                ii = ii +1
                continue
            concat_string = str(data[ii].sample[0]) + str(data[ii].sample[1])
            loc_array.append(concat_string)
            ii = ii + 1


        loc_dict = {}
        i = 0
        same = 0
        while i < len(loc_array):
            if(loc_array[i] in loc_dict.keys()):
                same = same +1

            else:
                num_diff_places = num_diff_places + 1
                loc_dict[loc_array[i]] = 1

            i = i+1



        start_time = data[0].start_time
        end_time = data[-1].end_time
        offset = data[0].offset
        num_of_diff_pls_datapoint = DataPoint(start_time,end_time,offset,num_diff_places)

        return [num_of_diff_pls_datapoint]


    def standard_deviation_of_displacements(self,datawithtransit):
        """
        :param data: datapoint array of centroid stream
        :return: standard deviation of displacements in a day
        :stream name: org.md2k.data_analysis.feature.standard_deviation_of_displacements
        """

        name = 'STAND DEV OF DISPLCMNTS'
        data_descriptor = [{"NAME":"standard deviation from displacement", "DATA_TYPE":"float", "DESCRIPTION": "standard deviation from displacement"}]

        data = []
        ii=0
        while ii< len(datawithtransit):
            if (float(datawithtransit[ii].sample[0]) != -1.0):
                data.append(datawithtransit[ii])
            ii = ii+1



        mean_distance = 0
        i=0
        while i < len(data)-1:
            mean_distance = mean_distance + self.haversine(data[i].sample[1],data[i].sample[0],data[i+1].sample[1],data[i+1].sample[0])
            i = i + 1

        mean_distance = mean_distance / (len(data)-1)
        var_distance = 0
        j = 0
        while j < len(data)-1:
            var_distance = var_distance + (self.haversine(data[j].sample[1],data[j].sample[0],data[j+1].sample[1],data[j+1].sample[0]) - mean_distance)**2
            j = j+1


        standard_deviation = sqrt(var_distance/(len(data)-1))


        start_time = data[0].start_time
        end_time = data[-1].end_time
        offset = data[0].offset

        stan_dev_datapoint = DataPoint(start_time,end_time,offset,standard_deviation)


        return [stan_dev_datapoint]

    
    def cumulative_staying_time(self,semanticdata):
        """
        :param data: datapoint array of semantic stream
        :return: cumulative staying time at different types of locations
        :stream name: org.md2k.data_analysis.feature.cumulative_staying_time
        """

        name = 'CUMUL STAY TIME'
        data_descriptor = [{"NAME":"cumulative staying time", "DATA_TYPE":"float", "DESCRIPTION": "cumulative staying time in seconds"}]
        data = semanticdata

        time_dictionary = {}


        i = 0
        while i < len(data):
            get_time_datetime = data[i].end_time - data[i].start_time
            get_time = get_time_datetime.total_seconds()
            #print ("this is time" + str(get_time))
            get_pre_time = 0
            if data[i].sample[0] in time_dictionary.keys():
                get_pre_time = time_dictionary[data[i].sample[0]]
            new_time = get_pre_time + get_time
            time_dictionary[data[i].sample[0]] = new_time
            i = i + 1


        output_data =[]
        start_time = data[0].start_time
        end_time = data[-1].end_time
        offset = data[0].offset

        #for key in time_dictionary:
        #    output_data.append(time_dictionary[key])

        
        output_datapoint = DataPoint(start_time,end_time,offset,time_dictionary)
        return [output_datapoint]



    def transition_counter(self,semanticdata):

        """
        :param data: datapoint array of semantic stream
        :return: number of transitions from one type of location to another 
        :stream name: org.md2k.data_analysis.feature.transition_counter
        """
        name = 'TRAN FRE'
        execution_context = {}
        annotations = {}
        data_descriptor = [{"NAME":"transition frequency", "DATA_TYPE":"int", "DESCRIPTION": "transition frequency"}]


        semanticwithouttransit = []
        jj = 0
        while jj < len(semanticdata):
            if(str(semanticdata[jj].sample[0]) != "dummy"):
                semanticwithouttransit.append(semanticdata[jj])
            jj = jj+1



        number_of_trans_dict = {}
        i = 0

        while i < len(semanticwithouttransit) - 1:


            pre_loc = semanticwithouttransit[i]
            post_loc = semanticwithouttransit[i+1]
            if ( pre_loc.sample != post_loc.sample ):
                key_string = pre_loc.sample[0] + " " + post_loc.sample[0]
                get_pre_num = 0
                if ( key_string in number_of_trans_dict.keys()):
                    get_pre_num = number_of_trans_dict[key_string]
                new_num = get_pre_num + 1
                #print (new_num)
                number_of_trans_dict[key_string] = new_num
            i = i + 1

        output_data = []

        #print (number_of_trans_dict)

        start_time = semanticdata[0].start_time
        end_time = semanticdata[-1].end_time
        offset = semanticdata[0].offset

        #for key in number_of_trans_dict:
        #    output_data.append(DataPoint(start_time,end_time,offset,[key,number_of_trans_dict[key]]))
        #print (len(output_data))


        ouput_datapoint = DataPoint(start_time,end_time,offset,number_of_trans_dict)
        return [ouput_datapoint]


    def maximum_distance_from_home(self,semanticdata, centroiddata):
        """
        :param home: semantic and centroid datastream
        :param locationstream: location datastreaam of the participant
        :return: maximum distance from home in kilometers
        """


        name = 'MAX DIST FROM HOME'
        data_descriptor = [{"NAME":"maximum distance from home", "DATA_TYPE":"float", "DESCRIPTION": "maximum distance from home"}]
        max = 0

        ii = 0

        while ii<len(centroiddata):
            #print (semanticdata[ii].sample)
            if (str(semanticdata[ii].sample[0]) == "Home"):
                home_lattitude = centroiddata[ii].sample[0]
                home_longitude = centroiddata[ii].sample[1]
                break
            ii = ii + 1

        jj = 0
        centroidwithouttransit = []
        while jj < len(centroiddata):
            if(float(centroiddata[jj].sample[0]) != -1.0):
                centroidwithouttransit.append(centroiddata[jj])
            jj = jj+1

        i=0
        #print (home_longitude)
        #print (centroidwithouttransit)

        while i < len(centroidwithouttransit):
            lattitude = centroidwithouttransit[i].sample[0]
            longitude = centroidwithouttransit[i].sample[1]
            distance = self.haversine(float(home_longitude),float(home_lattitude),float(longitude),float(lattitude))
           # print (distance)
            if (max < distance):
                max = distance
            i = i + 1


        start_time = semanticdata[0].start_time
        end_time = semanticdata[-1].end_time
        offset = semanticdata[0].offset

        max_datapoint = DataPoint(start_time,end_time,offset,max)

        return [max_datapoint]




    def process(self,user_id,all_days):
        #user_id = '397c6457-0954-4cd2-995c-2fbeb6c72097'
        #all_days = ["20171027", "20171028"]
        stream_name_gps_cluster = "org.md2k.data_analysis.gps_clustering_episode_generation"
        stream_name_semantic_location = "org.md2k.data_analysis.gps_episodes_and_semantic_location"
        streams = self.CC.get_user_streams(user_id)

        for day in all_days:

            if stream_name_gps_cluster in streams:
                cluster_stream_ids = self.CC.get_stream_id(user_id, stream_name_gps_cluster)
                cluster_data = []
                for cluster_stream_id in cluster_stream_ids:
                    cluster_data += self.CC.get_stream(cluster_stream_id['identifier'], user_id,day).data



            if stream_name_semantic_location in streams:
                semantic_stream_ids = self.CC.get_stream_id(user_id, stream_name_semantic_location)
                semantic_data = []
                for semantic_stream_id in semantic_stream_ids:
                    semantic_data += self.CC.get_stream(semantic_stream_id['identifier'], user_id,day).data        


            tot_dist = self.total_distance_covered(cluster_data)
            
                              

            try:
                if len(tot_dist):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_gps_cluster:
                            #print("---->",stream_metadata)

                            self.store_stream(filepath="total_distance_covered.json",
                                      input_streams=[stream_metadata], 
                                      user_id=user_id,
                                      data=tot_dist)
                            break
            except Exception as e:
                print("Exception:", str(e))
                print(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(tot_dist)))                
            
            
            
            max_dist = self.maximum_distance_between_two_locations(cluster_data)
            
            
            

            try:
                if len(max_dist):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_gps_cluster:
                            #print("---->",stream_metadata)

                            self.store_stream(filepath="maximum_distance_covered.json",
                                      input_streams=[stream_metadata], 
                                      user_id=user_id,
                                      data=max_dist)
                            break
            except Exception as e:
                print("Exception:", str(e))
                print(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(max_dist)))            
            
            
            
            
            num_of_diff_pls = self.number_of_different_places(cluster_data)
            
            try:
                if len(num_of_diff_pls):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_gps_cluster:
                            #print("---->",stream_metadata)

                            self.store_stream(filepath="number_of_different_places.json",
                                      input_streams=[stream_metadata], 
                                      user_id=user_id,
                                      data=num_of_diff_pls)
                            break
            except Exception as e:
                print("Exception:", str(e))
                print(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(num_of_diff_pls)))            
            
                        
            
            
            standard_dev = self.standard_deviation_of_displacements(cluster_data)
            
            
            try:
                if len(standard_dev):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_gps_cluster:
                            #print("---->",stream_metadata)

                            self.store_stream(filepath="standard_deviation_of_displacements.json",
                                      input_streams=[stream_metadata], 
                                      user_id=user_id,
                                      data=standard_dev)
                            break
            except Exception as e:
                print("Exception:", str(e))
                print(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(standard_dev)))            
            



            cumul_stay_data = self.cumulative_staying_time(semantic_data)
            print (">>>>>>>>>>>>>>>>>>>>>>>")
            for dp in cumul_stay_data:
                print (dp)
            
            
            try:
                if len(standard_dev):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_semantic_location:
                            #print("---->",stream_metadata)

                            self.store_stream(filepath="cumulative_staying_time.json",
                                      input_streams=[stream_metadata], 
                                      user_id=user_id,
                                      data=cumul_stay_data)
                            break
            except Exception as e:
                print("Exception:", str(e))
                print(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(cumul_stay_data)))   
            


            trans_fre = self.transition_counter(semantic_data)
            try:
                if len(trans_fre):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name == stream_name_semantic_location:
                            #print("---->",stream_metadata)

                            self.store_stream(filepath="transition_counter.json",
                                      input_streams=[stream_metadata], 
                                      user_id=user_id,
                                      data=trans_fre)
                            break
            except Exception as e:
                print("Exception:", str(e))
                print(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(trans_fre)))  
            

            max_dis_home = self.maximum_distance_from_home(semantic_data,cluster_data)
            #print ("............")
            #print (max_dis_home[0].sample)

            max_dis_home = self.maximum_distance_from_home(semantic_data,cluster_data)
                        
            
            try:
                input_stream_names = [stream_name_semantic_location, stream_name_gps_cluster]
                input_streams = []
                if len(max_dis_home):
                    streams = self.CC.get_user_streams(user_id)
                    for stream_name, stream_metadata in streams.items():
                        if stream_name in input_stream_names:
                            input_streams.append(stream_metadata)

                    self.store_stream(filepath="maximum_distance_from_home.json",
                                      input_streams=input_streams, 
                                      user_id=user_id,
                                      data=max_dis_home)
            except Exception as e:
                print("Exception:", str(e))
                print(traceback.format_exc())
            self.CC.logging.log('%s finished processing for user_id %s saved %d '
                                'data points' %
                                (self.__class__.__name__, str(user_id),
                                 len(max_dis_home)))
    



import unittest


from core.feature.phone_features.phone_features import PhoneFeatures
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint
import datetime
import uuid

from pprint import pprint

class TestPhoneFeatures(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.pp = PhoneFeatures()
        self.data = []
        for t in range(10,1,-1):
            currentTime = datetime.datetime.now()
            self.data.append(DataPoint(currentTime-datetime.timedelta(hours = t-.1),currentTime-datetime.timedelta(hours = t-.9),t))

        ownerUUID = uuid.uuid4()

        phonedata = [

        ]
        self.phoneDataStream = DataStream(identifier=uuid.uuid4(),owner=ownerUUID)
        self.phoneDataStream.data = phonedata

        smsdata = [

        ]
        self.smsDataStream = DataStream(identifier=uuid.uuid4(),owner=ownerUUID)
        self.smsDataStream.data = smsdata


        

    def test_inter_event_time_list_empty(self):
        self.assertIsNone(self.pp.inter_event_time_list([]))

    def test_inter_event_time_list(self):
        results = list(map(int, self.pp.inter_event_time_list(self.data)))
        self.assertIsNotNone(results)
        self.assertEqual([12]*8,results)
        
    
    def test_average_inter_phone_call_sms_time_hourly_empty(self):
        temp = self.phoneDataStream
        temp.data = []
        self.assertIsNone(self.pp.average_inter_phone_call_sms_time_hourly(temp, self.smsDataStream))
        self.assertIsNone(self.pp.average_inter_phone_call_sms_time_hourly(self.phoneDataStream, temp))


    

if __name__=='__main__':
    unittest.main()

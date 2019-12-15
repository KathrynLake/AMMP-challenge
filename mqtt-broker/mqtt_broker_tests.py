import unittest
from mqtt_broker import *
import paho.mqtt.client as mqtt
from data_processing import *

class Test_mqtt_broker_tests(unittest.TestCase):

    def __get_vrm_broker_url(self, system_id):
        ''' Get the url for the MQQT broker from the system ID. '''
        sum = 0
        for character in system_id.lower().strip():
            sum += ord(character)
        broker_index = sum % 128
        return 'mqtt{}.victronenergy.com'.format(broker_index)


    def test_broker_connection(self):
        system_id, username, password, sqs_url = get_broker_credentials()
        host = self.__get_vrm_broker_url(system_id)

        client = mqtt.Client()

        notifications = []
        client.on_connect = lambda c,d,f,r: c.subscribe('N/{}/system'.format(system_id), 0)

        #set username and password
        client.username_pw_set(username, password)

        client.connect(host)
        client.disconnect()



    def test_broker_messages(self):

        system_id, username, password, sqs_url = get_broker_credentials()
        host = self.__get_vrm_broker_url(system_id)

        client = mqtt.Client()

        notifications = []
        client.on_connect = lambda c,d,f,r: c.subscribe('N/{}/system'.format(system_id), 0)

        #set username and password and connect
        client.username_pw_set(username, password)
        client.connect(host)

        #let it run for a while and receive some messages
        client.loop_start()
        time.sleep(2) # wait for retained messages
        client.on_message = lambda c,d,msg: notifications.append(msg)
        time.sleep(2)
        client.loop_stop(True)
        self.assertTrue(len(notifications) > 0)


    def test_deserialize(self):
        message = ['A/B/C/D/E/F {"value": 1}']

        ds_message = deserialize_message_data(message)
        self.assertEqual(ds_message['A']['B']['C']['D']['E']['F'], 1)

if __name__ == '__main__':
    unittest.main()

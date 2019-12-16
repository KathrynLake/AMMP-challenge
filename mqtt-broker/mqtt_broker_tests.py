import unittest
from mqtt_broker import *
import paho.mqtt.client as mqtt
from data_processing import *
from load_config import *

class Test_mqtt_broker_tests(unittest.TestCase):


    def __get_vrm_broker_url(self, system_id):
        ''' Get the url for the MQQT broker from the system ID. '''
        sum = 0
        for character in system_id.lower().strip():
            sum += ord(character)
        broker_index = sum % 128
        return 'mqtt{}.victronenergy.com'.format(broker_index)


    def test_broker_connection(self):
        # load config:
        config = load_config()

        connected = False
        #On connect and on message callbacks
        def on_connect(client,d,f,r):
            connected = True
            print('Connected')
            # subscribe for all devices of user
            client.subscribe('N/{}/#'.format(config['system_id']), 0)

        def on_message(client,userdata,msg):
            try:
                handle_message(userdata, str(msg.topic) + ' ' + str(msg.payload))
            except:
                handle_message(userdata, str(msg))


        client = mqtt.Client("", True, None, protocol=mqtt.MQTTv311)

        client.on_connect = on_connect 
        client.on_message = on_message


        #set username and password
        client.username_pw_set(config['username'], config['password'])

        print('Trying to connect...')
        #connect to broker
        client.connect('mqtt.victronenergy.com', 8883)

        self.assertTrue(connected)



    def test_broker_messages(self):

        # load config:
        config = load_config()

        notifications = []

        #On connect and on message callbacks
        def on_connect(client,d,f,r):
            
            # subscribe for all devices of user
            client.subscribe('N/{}/#'.format(config['system_id']), 0)

        def on_message(client,userdata,msg):
            
            notifications.append(msg)


        client = mqtt.Client("", True, None, protocol=mqtt.MQTTv311)

        client.on_connect = on_connect 
        client.on_message = on_message


        #set username and password
        client.username_pw_set(config['username'], config['password'])

        print('Trying to connect...')
        #connect to broker
        client.connect('mqtt.victronenergy.com', 8883)


        #let it run for a while and receive some messages
        client.loop_start()
        time.sleep(5) # wait for retained messages
        client.loop_stop(True)
        self.assertTrue(len(notifications) > 0)


    def test_deserialize(self):
        message = 'A/B/C/D/E/F {"value": 1}'

        ds_message = deserialize_message_data(message)
        self.assertEqual(ds_message['A']['B']['C']['D']['E']['F'], 1)

if __name__ == '__main__':
    unittest.main()

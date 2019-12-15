import paho.mqtt.client as mqtt
import time
# sqs_pipeline import DataQueue
from data_processing import *
from load_config import *

def get_vrm_broker_url(system_id):
    ''' Get the url for the MQQT broker from the system ID. '''
    sum = 0
    for character in system_id.lower().strip():
        sum += ord(character)
    broker_index = sum % 128
    return 'mqtt{}.victronenergy.com'.format(broker_index)


def collect_data(sys_id, username, password):
    '''Connect to the MQTT broker, and listen for messages. Will run until program is exited. '''

    #On connect and on message callbacks
    def on_connect(client,d,f,r):
        print('Connected')
        # subscribe for all devices of user
        client.subscribe('N/{}/system'.format(sys_id), 0)

    def on_message(client,userdata,msg):
        try:
            handle_message(userdata, str(msg.topic) + ' ' + str(msg.payload))
        except:
            handle_message(userdata, str(msg))


    client = mqtt.Client("", True, None, mqtt.MQTTv311)

    client.on_connect = on_connect 
    client.on_message = on_message


    #set username and password
    client.username_pw_set(username, password)

    print('Trying to connect...')
    #connect to broker
    client.connect('mqtt.victronenergy.com', 8883)
    client.subscribe("N/*", qos=2)
    client.loop_forever()



def handle_message(userdata, message):
    '''Accepts a data point sent by the MQTT broker. Formats the data and pushes it to the SQS queue. '''

    #do what needs to be done to make the data into a dict
    deserialized_message = deserialize_message_data(message)

    #the message will be None if it was founf to be in a bad format, or irrelevant. 
    if message is not None:
        #add metadata
        final_datapoint = add_metadata(deserialized_message)

        #push data to pipeline
        push_data_to_pipeline(final_datapoint)



def push_data_to_pipeline(message):
    '''Connect to an AWS SQS pipeline and send it the provided data. '''

    dq = DataQueue('raw')
    try:
        dq.put(datapoints, company_id)
    except:
        print('datapoint: \n {} \n\n'.format(message))

def get_broker_credentials():
    #set credentials
    system_id = '78a504c59655'
    username = 'monitoring-test@ammp.io'
    password = '3Ybu3tAdfF7kWSBwEwzB5Rhb'

    return system_id, username, password


if __name__ == '__main__':
    config = load_config()

    #collect data
    collect_data(config['system_id'], config['username'], config['password'])
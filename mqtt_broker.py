import paho.mqtt.client as mqtt
import time
#from sqs_pipeline import DataQueue

def get_vrm_broker_url(system_id):
    ''' Get the url for the MQQT broker from the system ID. '''
    sum = 0
    for character in system_id.lower().strip():
        sum += ord(character)
    broker_index = sum % 128
    return 'mqtt{}.victronenergy.com'.format(broker_index)


def collect_data(sys_id, username, password, sqs_queue_url):
    '''Connect to the MQTT broker, and listen for messages. Will run until program is exited. '''

    #On connect and on message callbacks
    def on_connect(client,d,f,r):
        print('Connected with result code:'+str(r))
        # subscribe for all devices of user
        client.subscribe('N/{}/system'.format(sys_id), 0)

    # gives message from device
    def on_message(client,userdata,msg):
        print('Topic',msg.topic + '\nMessage:' + str(msg.payload))
        #handle_message(userdata, msg, sqs_queue_url)

    client = mqtt.Client()

    client.on_connect = on_connect #lambda c,d,f,r: c.subscribe('N/#', 0)
    client.on_message = on_message


    #set username and password
    client.username_pw_set(username, password)

    #connect to broker
    client.connect(get_vrm_broker_url(sys_id), port=8883)#client.connect('mqtt://test.mosquitto.org')

    run = True
    #continue to run until program is exited
    while run:
        client.loop()


def handle_message(message, sqs_queue_url):
    '''Accepts a data point sent by the MQTT broker. Formats the data and pushes it to the SQS queue. '''

    #do what needs to be done to make the data into a dict
    message = format_message_data(message)

    #push data to pipeline
    msg = push_data_to_pipeline(message, sqs_queue_url)


def format_message_data(message):
    """Take raw data from broker, and transform it into a useable data point to be pushed to the pipeline"""
    return message

def push_data_to_pipeline(userdata, message, sqs_queue_url):
    '''Connect to an AWS SQS pipeline and send it the provided data. '''
    dq = DataQueue('raw')
    try:
        dq.put(datapoints, company_id)
    except:
        print('Cannot push data to queue service. The message is: /n/n {} /n/n/n'.format(message))

def get_broker_credentials():
    #set credentials
    system_id = '78a504c59655'
    username = 'monitoring-test@ammp.io'
    password = '3Ybu3tAdfF7kWSBwEwzB5Rhb'
    sqs_url = 'no_sqs_url'

    return system_id, username, password, sqs_url


if __name__ == '__main__':

    system_id, username, password, sqs_url = get_broker_credentials()

    #collect data
    collect_data(system_id, username, password, sqs_url)
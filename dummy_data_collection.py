from mqtt_broker import *

from load_config import *
config = load_config()

userdata = None
for message in config['dummy_data']:

    handle_message(userdata, message)


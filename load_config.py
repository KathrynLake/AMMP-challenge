import json

def load_config():
    with open('config.json') as filehandle:
        config = json.load(filehandle)

    return config

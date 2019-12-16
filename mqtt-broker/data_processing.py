import json
from datetime import datetime


def deserialize_message_data(dp):
    """take in a list of data points, and insert them into a python nested dict"""
    message_dict = {}

    #for dp in message:
    #get "keys" into a list: 
    message_dict_loc = dp.split(' ')[0]
    message_dict_loc = message_dict_loc.split('/')

    #can_deserialize: helps to filter out bad messages
    can_deserialize = True

    #Drop the messages which are just the system ID (eg. N/sys_id/system/0/Serial {"value": "sys_id"})
    if message_dict_loc[-1] == 'Serial': 
        can_deserialize = False

    #Get the value out by converting the value part into json: 
    valuedict = dp[dp.index(' ') + 1:]
    valuedict = valuedict.replace('\'', '\"')

    try:
        valuedict = json.loads(valuedict)
        value = valuedict['value']
    except:
        print('The following data point could not be serialized due to incorrect format: \n{}'.format(dp))
        can_deserialize = False

    if can_deserialize:
        #fill dict with values with recursive function
        message_dict = insert_key_or_value(message_dict, message_dict_loc, 0, value)

        return message_dict

    else:
        return None


def insert_key_or_value(dict, loc, ind, value):
    """
    Recursive function to insert values into a nested dict. Takes:
        dict: dict, Python dict which requires values to be filled in
        loc: list, a list of the dictionary keys/path to the value to be inserted
        ind: int, how far into the dictionary we have moved, according to the keys in the 'loc' list
        value: var, final value to insert
    """
    #If we're at the last key, enter the value
    if ind == len(loc)-1:
        nested_set(dict, loc[:ind+1], value)
        #This will return the final dict
        return dict

    #if the current key is not already in the dict, insert it
    if loc[ind] not in list(nested_get(dict, loc[:ind]).keys()):
        nested_set(dict, loc[:ind+1], {})

    #Add one to the key index, and enter the function again
    ind += 1
    dict = insert_key_or_value(dict, loc, ind, value)

    return dict


def nested_get(dict, keys):
    """ get value from nested dict using list of keys"""
    for key in keys:
            dict = dict[key]
    return dict


def nested_set(dict, keys, value):
    """ set value in nested dict using list of keys"""
    for key in keys[:-1]:
        dict = dict.setdefault(key, {})
    dict[keys[-1]] = value


def add_metadata(deserialized_message):
    #Add metadata:
    final_datapoint = {'meta': {}, 'datapoint': {}}

    timestamp = datetime.now()
    timestamp = str(timestamp.isoformat('T'))
    final_datapoint['meta']['time'] = timestamp

    final_datapoint['meta']['asset_id'] = None
    final_datapoint['meta']['datamap_id'] = None

    final_datapoint['datapoint'] = deserialized_message

    return final_datapoint


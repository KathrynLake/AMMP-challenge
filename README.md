# AMMP-challenge

Please find the following: 
  - Answers to conceptual questions 1(a) and 2 in **Conceptual Questions.pdf**
  - Python scripts in the **mqtt-broker** folder
  
Regarding the Python scripts:
If **mqtt_broker.py** is run, it should connect to the broker, load data from the messages, format them into a Python dict, and print the dict out in the console. I have made an assumption of what the received datapoints will look like in Python, so there is a chance it will not work. 

*In case it does not work*: to see how the data processing part of the script works, **dummy_data_processing.py** should be run. This takes my assumption of what the broker data will look like (see the dummy data in **config.json**), and runs it through the second part of the script to create a python dictionary from the datapoints. 

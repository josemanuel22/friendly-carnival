
import datetime
import pickle

import numpy as np

import time
import configparser
config = configparser.ConfigParser()

config.read("config.ini")



#Defaults
TM_FILE = config["DEFAULT"]["TM_FILE"]
SLEEP_TIME = int(config["DEFAULT"]["SLEEP_TIME"])

#Alog Connection

#Time series database connection

def test_apply(x):
        try:
            return float(x)
        except (ValueError, TypeError):
            return None

def is_string(s):
    return isinstance(s, str)
 
def clean_for_kairos(s):
    removelist = r'[^A-Za-z0-9./_-]'
    return re.sub(removelist,"", s)

now = datetime.datetime.now()
pickle.dump(now, open(TM_FILE,"wb"))
time.sleep(SLEEP_TIME)



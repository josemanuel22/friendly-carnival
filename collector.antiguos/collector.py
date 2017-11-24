#!/opt/anaconda3/bin/python  -u

import psycopg2
import datetime
import pickle

import numpy as np

import telnetlib
import time
import re

import configparser
from elasticsearch import Elasticsearch

config = configparser.ConfigParser()

config.read("config.ini")

#Defaults
TM_FILE = config["DEFAULT"]["TM_FILE"]
SLEEP_TIME = int(config["DEFAULT"]["SLEEP_TIME"])

#Alog Connection
DB_SESSION = config["ALOG"]["DB_SESSION"]

#Time series database connection
HOST = config["TSDB"]["HOST"]
PORT = int(config["TSDB"]["PORT"])

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

while True: 
    db_connection = psycopg2.connect(DB_SESSION)
    db_connection.set_client_encoding("latin1")
    cursor = db_connection.cursor()

    #before = datetime.datetime.now()
    #time.sleep(10);
 
    now = datetime.datetime.now()
    try: 
        before = pickle.load(open(TM_FILE,"rb"))
    except:
        before = now - datetime.timedelta(seconds=10)
   
    query = "SELECT * FROM alog WHERE timestamp BETWEEN %s and %s"
    cursor.execute(query, (before,now))

    # ### Send data to Kairosdb
    tn = telnetlib.Telnet(HOST,PORT)
    es = Elasticsearch(HOST)
    row = cursor.fetchone()
    while row:
        print(timestamp); print(type(timestamp))
        timestamp = int(time.mktime(row[1].timetuple()))
        loghost = clean_for_kairos(row[2])
        env = clean_for_kairos(row[3])
        proc = clean_for_kairos(row[4])
        procid = row[5]
        metric = clean_for_kairos(row[8])
        val = str(row[9])
        value = val.replace("e",".0e") if "e" in val and "." not in val else val  
        logtext = clean_for_kairos(str(row[10]).replace(" ", "_"))        
       
        row = cursor.fetchone()
        if metric and test_apply(value): #FPARS con KEYVALUE NUMERICO
            msg = "put {} {} {} proc={} env={} loghost={} logtext={}\n".format(
                metric,
                timestamp,
                value,
                proc,
                env,
                loghost,
                logtext
            ).encode('ascii')
            tn.write(msg)
        data_dict = {}
        columns =['skip','@timestamp','loghost','envname','procname','procid','module','skip','keywname','keywvalue','logtext']
        for i, col in enumerate(columns):
            if row[i] != "" and row[i] is not None and col!='skip': 
                data_dict[col] = row[i]
        res = es.index(index="vltlog", doc_type="opslog", body=data_dict)
    #print(data_dict) 
    print(msg)
    tn.close()

    pickle.dump(now, open(TM_FILE,"wb"))
    time.sleep(SLEEP_TIME)


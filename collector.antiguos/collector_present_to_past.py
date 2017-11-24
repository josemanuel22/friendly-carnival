#!/opt/anaconda3/bin/python  -u

import psycopg2
import datetime
import pickle

import time
import re

import configparser
import requests
import json
import gzip

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, parallel_bulk

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
    val = str(x[9])
    try:
         float(val)
         return True
    except (ValueError, TypeError):
        return False

def is_string(s):
    return isinstance(s, str)
 
def clean_for_kairos(s):
    removelist = r'[^A-Za-z0-9./_-]'
    return re.sub(removelist,"", s)


def get_metric_names():
    url = 'http://192.168.1.13:8080/api/v1/metricnames'
    r = requests.get(url)
    print("Metric names:\n", r.json())

#get_metric_names()

def list_tag_names():
    url = 'http://192.168.1.13:8080/api/v1/tagnames'
    r = requests.get(url)
    print("tag names:\n", r.json())

#list_tag_names()

def list_tag_values():
    url = 'http://192.168.1.13:8080/api/v1/tagvalues'
    r = requests.get(url)
    print("Tag values:\n", r.json())
#list_tag_values()

def db_connection_setUp(db_session):
    db_connection = psycopg2.connect(db_session)
    db_connection.set_client_encoding("latin1")
    return db_connection

def db_execute_query(db_connection, query, query_args):
    cursor = db_connection.cursor()
    print("reading database[Query. May Take Time]...")
    cursor.execute(query, query_args)
    print("finish to query database")
    return cursor


def kairosdb_insert(data, kairosdb_server, data_filter, data_parser):
    kairosdb_data=[]
    kairosdb_data = list(filter(data_filter, data)); n_data_inserted = n_data_inserted + len(kairosdb_data)

def kairosdb_parser(data_point):
    data_point_parsed = {
        "name": clean_for_kairos(data_point[8]),
        "timestamp":int(time.mktime(data_point[1].timetuple()))*1000, #kairosdb time in miliseconds
        "value"    : float(data_point[9]),
        "tags": {
             "proc": clean_for_kairos(data_point[4]),
             "env": clean_for_kairos(data_point[3]),
             "loghost": clean_for_kairos(data_point[2]),
             "logtext": clean_for_kairos(str(data_point[10]).replace(" ", "_")),
        }
    }
    return data_point_parsed

def kairosdb_insert_period(db_connection, period, kairosdb_server, data_filter, fetchsize=10000):
    cursor = db_execute_query(db_connection, "SELECT * FROM alog WHERE timestamp BETWEEN %s and %s", period)
    data = cursor.fetchmany(fetchsize)
    print(data)
    n_data_inserted = 0; response = None
    while data:
        kairosdb_data=[]
        kairosdb_data = list(filter(data_filter, data)); n_data_inserted = n_data_inserted + len(kairosdb_data)
        gzipped = gzip.compress(bytes(json.dumps(list(map(kairosdb_parser, kairosdb_data))), 'latin1'));
        headers = {'content-type': 'application/gzip'}                                                # To sent without compression comment this line
        response = requests.post(kairosdb_server + "/api/v1/datapoints", gzipped, headers=headers)    # To sent without compression comment this line
#        response = requests.post(kairosdb_server + "/api/v1/datapoints", json.dumps(kairosdb_data))  # To sent without compresion uncomment this line
#        print("Inserted %d data response [with compression]: \t%d (status code)" % (len(data), response.status_code))
        n_data_inserted = n_data_inserted + len(data)
        data = cursor.fetchmany(fetchsize) 
    return (response, n_data_inserted)

    
def kairosdb_insert_backards(db_connection, kairosdb_server, data_filter, fetchsize=10000, datatime_batch=100):
    try:
        present = pickle.load(open(TM_FILE,"rb"))
    except:
        present = datetime.datetime.now()
    past = present - datetime.timedelta(seconds=datatime_batch)
    start_time = present
    while True: #(start_time - past) < datetime.timedelta(0,3600,0):
        (response, n_data_inserted)  = kairosdb_insert_period(db_connection, (past, present), kairosdb_server, test_apply)
        print(response.text)
        #print("Inserted %d data, time %s - %s response [with compression]: \t%d (status code)" % (n_data_inserted, past, present, response.status_code))
        present = past; past = present - datetime.timedelta(seconds=datatime_batch)
        pickle.dump(present, open(TM_FILE,"wb"))

def kairosdb_insert_present(db_connection, kairosdb_server, data_filter, fetchsize=10000, datatime_batch=1000):
    present = datetime.datetime.now()+datetime.timedelta(hours=-3); #computer local time 3h+  
    past = present - datetime.timedelta(minutes=1);
    while True:
        (response, n_data_inserted)  = kairosdb_insert_period(db_connection, (past, present), kairosdb_server, test_apply)
        if n_data_inserted == 0:
            print("No data to insert, time %s - %s" % (past, present))
            time.sleep(100)
            present = present+datetime.timedelta(seconds=100)
            continue
        print("Inserted: %d data, time %s - %s response [with compression]: \t%d (status code)" % (n_data_inserted, past, present, response.status_code))
        print(response.content)
        tmp=present;
        past = present; 
        present=tmp+datetime.timedelta(seconds=datatime_batch) if datetime.datetime.now()-present > datetime.timedelta(seconds=datatime_batch) else datetime.datetime.now()

def es_connection_setUp(es_server):
    return  Elasticsearch(es_server)

def es_generator_data(data,index, doc_type):
    for data_point in data:
        yield {
            '_index': index,
            '_type': doc_type,
            '_source':  {
                          '@timestamp': data_point[1],
                          'loghost': data_point[2] ,
                          'envname': data_point[3],
                          'procname':data_point[4],
                          'procid':data_point[5],
                          'module':data_point[2],
                          'keywname':data_point[8],
                          'keywvalue':data_point[9],
                          'logtext':data_point[10], 
                        },
            }

def es_insert(es_object, data, index, doc_type, bulk_fn=parallel_bulk):
    for ok, result in bulk_fn(es_object, es_generator_data(data, index, doc_type)): pass;
    return (ok, result)
    

def collector_datalab_period(db_connection, kairosdb_server, es_server, period,index, doc_type, data_filter=test_apply, bulk_fn=parallel_bulk, fetchsize=1000):
    es = es_connection_setUp([es_server]) 
    cursor = db_execute_query(db_connection, "SELECT * FROM alog WHERE timestamp BETWEEN %s and %s", period)
    data = cursor.fetchmany(fetchsize)
    n_data_inserted = 0; response = None
    while data:
        kairosdb_data = list(filter(data_filter, data)); n_data_inserted = n_data_inserted + len(kairosdb_data)
        gzipped = gzip.compress(bytes(json.dumps(list(map(kairosdb_parser, kairosdb_data))), 'latin1'));
        headers = {'content-type': 'application/gzip'}                                               
        kairos_response = requests.post(kairosdb_server + "/api/v1/datapoints", gzipped, headers=headers)
        (es_ok, es_result) = es_insert(es, data, index, doc_type, bulk_fn=parallel_bulk)
        data = cursor.fetchmany(fetchsize)
    return (kairos_response, es_ok, es_result, n_data_inserted)

    
db_connection = db_connection_setUp(DB_SESSION)
#kairosdb_insert_present(db_connection, "http://192.168.1.13:8080", test_apply)
#kairosdb_insert_backards(db_connection, "http://192.168.1.13:8080", test_apply)
period = (datetime.datetime.now()-datetime.timedelta(seconds=100), datetime.datetime.now())
#(kairos_response, es_ok, es_result, n_data_inserted) = insert_datalab_period(db_connection, "http://192.168.1.13:8080", "192.168.1.11", period, "prueba", "my_type")
(response, n_data_inserted)  = kairosdb_insert_period(db_connection, period, "http://192.168.1.13:8080", test_apply)
print("Inserted %d data, time %s - %s response [with compression]: \t%d (status code)" % (n_data_inserted, period[0], period[1], kairos_response.status_code)) if n_data_inserted != 0 else print("No data to insert")

if __name__ == "__main__":
    print("collector module for kairos and elasticsearch in python")




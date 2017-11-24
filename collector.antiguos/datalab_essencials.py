#!/opt/anaconda3/bin/python  -u

import psycopg2
import datetime
import pickle

import time
import re
import math

import configparser
import requests
import json
import gzip

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, parallel_bulk
import logging
import sys


config = configparser.ConfigParser()
config.read("config.ini")

#Defaults
#TM_FILE = config["DEFAULT"]["TM_FILE"]
SLEEP_TIME=5    #int(config["DEFAULT"]["SLEEP_TIME"])

#Alog Connection
DB_SESSION = "dbname=alog host=nico.pl.eso.org user=alogreader"  #config["ALOG"]["DB_SESSION"]

#Time series database connection
HOST = "192.168.1.10"  #config["TSDB"]["HOST"]
PORT = "4242"  #int(config["TSDB"]["PORT"])

DEBUG = False

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger(__name__)

def set_log(your_logger):
    global logger
    logger = your_logger

def test_apply(x):
    val = str(x[9])
    try:
         y = float(val)
         if math.isnan(y): return False; 
         return True
    except (ValueError, TypeError):
        return False

def is_string(s):
    return isinstance(s, str)
 
def db_connection_setUp(db_session):
    db_connection = psycopg2.connect(db_session)
    db_connection.set_client_encoding("latin1")
    return db_connection

def db_execute_query(db_connection, query, query_args):
    cursor = db_connection.cursor()
    logger.info("reading database[Query. May Take Time]...")
    cursor.execute(query, query_args)
    logger.info("finish to query database")
    return cursor

def clean_for_kairos(s):
    removelist = r'[^A-Za-z0-9./_-]'
    return re.sub(removelist,"", s)

def kairos_get_metric_names(kairos_server):
    r = requests.get(kairos_server+'/api/v1/metricnames')
    print("Metric names:\n", r.json())
#kairos_get_metric_names('http://192.168.1.10:8080')

def kairos_list_tag_names(kairos_server):
    r = requests.get(kairos_server+'/api/v1/tagnames')
    print("tag names:\n", r.json())
#kairos_list_tag_names(kairos_server)

def kairos_list_tag_values(kairos_server):
    r = requests.get(kairos_server+'/api/v1/tagvalues')
    print("Tag values:\n", r.json())
#kairos_list_tag_values('http://192.168.1.10:8080')

def kairos_insert(data, kairosdb_server, data_filter, data_parser):
    kairosdb_data=[]; n_data_inserted = 0;
    kairosdb_data = list(filter(data_filter, data))
    gzipped = gzip.compress(bytes(json.dumps(list(map(data_parser, kairosdb_data))), 'latin1'));
    headers = {'content-type': 'application/gzip'} 
    response = requests.post(kairosdb_server + "/api/v1/datapoints", gzipped, headers=headers)
    logger.debug("Inserted %d data, response [with compression]: \t%d (status code)" % (len(kairosdb_data) if response.status_code == 204 else 0, response.status_code))
    n_data_inserted = len(kairosdb_data) if response.status_code == 204 else 0
    return (response, n_data_inserted)

def kairos_parser(data_point):
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


def kairos_collector(db_connection, sql_sentence, sql_args, kairosdb_server, data_filter, data_parser,fetchsize=10000):
    cursor = db_execute_query(db_connection, sql_sentence, sql_args)
    data = cursor.fetchmany(fetchsize)
    n_data_inserted = 0; response = None
    while data:
        (response, n) = kairos_insert(data, kairosdb_server, data_filter, data_parser)
        logger.info("Inserted %d data, response [with compression]: \t%d (status code)" % (n_data_inserted+n if response.status_code == 204 else n_data_inserted+n,response.status_code))
        if response.status_code != 204: return (response, n_data_insert);
        n_data_inserted += n 
        data = cursor.fetchmany(fetchsize)
    return (response, n_data_inserted)

def kairos_collector_period(db_connection, period, kairosdb_server, data_filter, data_parser,fetchsize=10000):
    return kairos_collector(db_connection, "SELECT * FROM alog WHERE timestamp BETWEEN %s and %s", period, kairosdb_server, data_filter, data_parser,fetchsize)

TM_FILE="last_timestamp.pkl"    
def kairos_insert_backards(db_connection, kairosdb_server, data_filter, data_parser, fetchsize=10000, datatime_batch=100):
    try:
        present = pickle.load(open(TM_FILE,"rb"))
    except:
        present = datetime.datetime.now()
    past = present - datetime.timedelta(seconds=datatime_batch)
    start_time = present
    while True: #(start_time - past) < datetime.timedelta(0,3600,0):
        (response, n_data_inserted)  = kairos_collector_period(db_connection, (past, present), kairosdb_server, data_filter, data_parser)
        logger.info("Inserted %d data, time %s - %s response [with compression]: \t%d (status code)" % (n_data_inserted, past, present, response.status_code))
        logger.debug(response.text)
        present = past; past = present - datetime.timedelta(seconds=datatime_batch)
        if DEBUG == True and (start_time - past) > datetime.timedelta(0,300,0): return (response, n_data_inserted)
        pickle.dump(present, open(TM_FILE,"wb"))

def kairos_insert_present(db_connection, kairosdb_server, data_filter, data_parser,fetchsize=10000, datatime_batch=1000, time_refresh=100):
    present = datetime.datetime.now()+datetime.timedelta(hours=-3); #computer local time 3h+  
    past = present - datetime.timedelta(minutes=1);
    while True:
        (response, n_data_inserted)  = kairos_collector_period(db_connection, (past, present), kairosdb_server, data_filter, data_parser)
        if n_data_inserted == 0:
            logger.info("No data to insert, time %s - %s" % (past, present))
            time.sleep(time_refresh)
            present = present+datetime.timedelta(seconds=100)
            continue
        logger.info("Inserted: %d data, time %s - %s response [with compression]: \t%d (status code)" % (n_data_inserted, past, present, response.status_code))
        logger.debug(response.text)
        tmp=present;
        past = present; 
        present=tmp+datetime.timedelta(seconds=datatime_batch) if datetime.datetime.now()-present > datetime.timedelta(seconds=datatime_batch) else datetime.datetime.now()
        if DEBUG :  return (response, n_data_inserted)

def es_connection_setUp(es_server):
    return  Elasticsearch(es_server, timeout=100, ignore=[400, 404])


def is_keywvalue_num(keywvalue):
    try:
        float(keywvalue)
        return True
    except:
        return False

def es_get_type(data_point):
    if is_keywvalue_num(data_point[9]) and data_point[7].lower()=='fpar':
        return "opslog"
    else:
        return data_point[7].lower()    

def parsing_keywvalue(keywvalue, mi_type):
    if mi_type == "fpar":
        return keywvalue
    elif mi_type=="opslog":
        return float(keywvalue)
    else:
        return keywvalue

def es_basic_generator_data(data,index, doc_type='default'):
    for data_point in data:
        mi_type=es_get_type(data_point)
        if mi_type == "opslog": logger.info(data_point[9])
        yield {
            '_index': "opslog" if mi_type=="opslog" else index,
            '_type': mi_type if doc_type=='default' else doc_type,
            '_source':  {
                          '@timestamp': data_point[1],
                          'loghost': data_point[2] ,
                          'envname': data_point[3],
                          'procname':data_point[4],
                          'procid':data_point[5],
                          'module':data_point[2],
                          'keywname':data_point[8],
                          'keywvalue': float(data_point[9]) if mi_type=="opslog" else data_point[9],
                          'logtext':data_point[10], 
                        },
            }

def es_insert(es_object, data, index, doc_type='default', es_generator_data=es_basic_generator_data, bulk_fn=parallel_bulk):
    ok=True;result="NO_INSERTED"
    for ok, result in bulk_fn(es_object, es_generator_data(data, index, doc_type),thread_count=4):pass;
    return (ok, result)

def insert_datalab(data, kairos_server, data_filter, data_parser, es_object, index, doc_type='default', es_generator_data=es_basic_generator_data, bulk_fn=parallel_bulk, fetchsize=1000):
    n_data_inserted = 0; response = None
    (kairos_response, n_data_inserted) = kairos_insert(data, kairos_server, data_filter, data_parser)
    (es_ok, es_result) = es_insert(es_object, data, index, doc_type, es_generator_data, bulk_fn)
    return (kairos_response, n_data_inserted, es_ok, es_result)    


def collector_datalab(db_connection, sql_sentence, sql_args, kairos_server, data_filter, data_parser, es_object, index, doc_type='default', es_generator_data=es_basic_generator_data ,bulk_fn=parallel_bulk, fetchsize=1000):
    #es = es_connection_setUp([es_server])
    cursor = db_execute_query(db_connection, sql_sentence, sql_args)
    data = cursor.fetchmany(fetchsize)
    kairos_response="NO_RESPONSE";n_data_inserted=0;es_ok=True;es_result=0;
    while data:
        (kairos_response, n_data_inserted, es_ok, es_result) = insert_datalab(data, kairos_server, data_filter, data_parser, es_object,index, doc_type, es_generator_data, parallel_bulk, fetchsize=1000)
        data = cursor.fetchmany(fetchsize)
    return (kairos_response, n_data_inserted, es_ok, es_result) 

def collector_datalab_period(db_connection, period, kairos_server, data_filter, data_parser, es_object, index, doc_type='default', es_generator_data=es_basic_generator_data, bulk_fn=parallel_bulk, fetchsize=1000): 
    return collector_datalab(db_connection, "SELECT * FROM alog WHERE timestamp BETWEEN %s and %s", period, kairos_server, data_filter, data_parser, es_object, index, doc_type, es_generator_data, bulk_fn, fetchsize)


def collector_datalab_present(db_connection, kairos_server, data_filter, data_parser, es_object, index, doc_type='default', es_generator_data=es_basic_generator_data, bulk_fn=parallel_bulk, fetchsize=1000):
        present = datetime.datetime.now();
        past = present - datetime.timedelta(minutes=1);
        while True:
            (response, n_data_inserted, es_ok, es_result)  = collector_datalab_period(db_connection, (past, present), kairos_server, data_filter, data_parser, es_object, index, doc_type, es_generator_data, bulk_fn, fetchsize)
            if n_data_inserted == 0:
                logger.info("No data to insert, time %s - %s" % (past, present))
                time.sleep(100)
                present = present+datetime.timedelta(seconds=100)
                continue
            logger.info("Inserted: %d data, time %s - %s response [with compression]: \t%d (status code)" % (n_data_inserted, past, present, response.status_code))
            logger.debug(response.text)
            time.sleep(10)
            tmp=present;
            past = present;
            present=datetime.datetime.now()
            if DEBUG :  return (response, n_data_inserted, es_ok, es_result)


if __name__ == "__main__":
    import unittest
    class MyTest(unittest.TestCase):
        def test_kairos_insert(self, kairosdb_server="http://192.168.1.10:8080", data_filter=test_apply, data_parser= kairos_parser):
            data = [(1546655907, datetime.datetime(2017, 9, 25, 4, 11, 23), 'wat3tcs', 'lat3vcm', 'logManager', 0, '', 'FPAR', 'TESTER_KAIROS_INSERT', '2.71828182846', 'STS VCM chamber pressure [bar].', '', 0, '', ''), (1546636293, datetime.datetime(2017, 9, 25, 4, 12, 27), 'wat4tcs', 'lat4vcm', 'logManager', 0, '', 'FPAR', 'TESTER_KAIROS_INSERT', '3.14159265359', 'STS VCM chamber pressure [bar].', '', 0, '', '')]
            (response, n_data_inserted) = kairos_insert(data, kairosdb_server, data_filter, data_parser)
            self.assertEqual(response.status_code, 204)
            self.assertEqual(n_data_inserted, len(list(filter(data_filter, data))))
            logger.info("Test %s run successfully", MyTest.test_kairos_insert.__name__)
        
        def test_es_insert(self, es_server="http://192.168.1.10"):
            data = [(1546655907, datetime.datetime(2017, 9, 25, 4, 11, 23), 'wat3tcs', 'lat3vcm', 'logManager', 0, '', 'FPAR', 'TESTER_ES_INSERT', '4.669201', 'Es Trascendente', '', 0, '', ''), (1546636293, datetime.datetime(2017, 9, 25, 4, 12, 27), 'wat4tcs', 'lat4vcm', 'logManager', 0, '', 'FPAR', 'TESTER_KAIROS_INSERT', '1.6180339887', 'La perfeccion.', '', 0, '', '')] 
            es = es_connection_setUp(es_server)
            (ok, result) = es_insert(es, data, "tester_es_insert")
            self.assertEqual(ok, True)
            self.assertEqual(result['index']['status'], 201)
            logger.info("Test %s run successfully", MyTest.test_es_insert.__name__)


        def test_insert_datalab(self, kairos_server="http://192.168.1.10:8080", data_filter=test_apply, data_parser=kairos_parser, es_server="http://192.168.1.10", index="tester_insert_datalab_index", doc_type="tester_insert_datalab_doc_type", es_generator_data=es_basic_generator_data,bulk_fn=parallel_bulk, fetchsize=1000):
            data = [(1546655907, datetime.datetime(2017, 9, 25, 4, 11, 23), 'wat3tcs', 'lat3vcm', 'logManager', 0, '', 'FPAR', 'TESTER_DATALAB_INSERT', '4.669201', 'Es Trascendente', '', 0, '', ''), (1546636293, datetime.datetime(2017, 9, 25, 4, 12, 27), 'wat4tcs', 'lat4vcm', 'logManager', 0, '', 'FPAR', 'TESTER_DATALAB_INSERT', '1.6180339887', 'La perfeccion.', '', 0, '', '')]
            es_object = es_connection_setUp(es_server)
            (kairos_response, n_data_inserted, es_ok, es_result) = insert_datalab(data, kairos_server, data_filter, data_parser, es_object, index, doc_type, es_generator_data, bulk_fn, fetchsize) 
            self.assertEqual(kairos_response.status_code, 204)
            self.assertEqual(n_data_inserted, len(list(filter(data_filter, data))))
            self.assertEqual(es_ok, True)
            logger.info("Test %s run successfully", MyTest.test_insert_datalab.__name__)        


    def test_kairos_collector_period(kairosdb_server="http://192.168.1.10:8080", data_filter=test_apply, data_parser=kairos_parser, fetchsize=10000):
        db_connection = db_connection_setUp(DB_SESSION)
        period=(datetime.datetime.now()-datetime.timedelta(seconds=10), datetime.datetime.now())
        cursor = db_execute_query(db_connection, "SELECT * FROM alog WHERE timestamp BETWEEN %s and %s", period)
        data =  cursor.fetchall()
        (response, n_data_inserted) = kairos_collector_period(db_connection, period, kairosdb_server, data_filter, data_parser)
        assert response.status_code ==  204
        assert len(list(filter(data_filter, data))) == n_data_inserted
        logger.info("Test %s run successfully", test_kairos_collector_period.__name__) 

    def test_kairos_insert_backards(db_connection, kairosdb_server="http://192.168.1.10:8080", data_filter=test_apply, data_parser=kairos_parser, fetchsize=10000, datatime_batch=100):
        db_connection = db_connection_setUp(db_connection)
        global DEBUG
        DEBUG = True
        (response, n_data_insert) = kairos_insert_backards(db_connection, kairosdb_server, data_filter, data_parser)
        assert response.status_code ==  204
        logger.info("Test %s run successfully", test_kairos_insert_backards.__name__) 
        DEBUG = False


    def test_kairos_insert_present(db_connection, kairosdb_server="http://192.168.1.10:8080", data_filter=test_apply, data_parser=kairos_parser,fetchsize=10000, datatime_batch=1000, time_refresh=100):
        db_connection = db_connection_setUp(db_connection)
        global DEBUG
        DEBUG = True
        (response, n_data_insert) =  kairos_insert_present(db_connection, kairosdb_server, data_filter, data_parser)
        assert response.status_code ==  204
        logger.info("Test %s run successfully", test_kairos_insert_present.__name__)
        DEBUG = False

    def test_collector_datalab_period(db_connection, kairos_server="http://192.168.1.10:8080", data_filter=test_apply, data_parser=kairos_parser, es_server="http://192.168.1.10", index="tester_insert_datalab_index", doc_type='default', es_generator_data=es_basic_generator_data ,bulk_fn=parallel_bulk, fetchsize=1000):
        db_connection = db_connection_setUp(db_connection)
        es_object = es_connection_setUp(es_server)
        period = (datetime.datetime(year=2017,month=10,day=16,hour=21,minute=50,second=55), datetime.datetime(year=2017,month=10,day=16,hour=21,minute=52))
        (kairos_response, n_data_inserted, es_ok, es_result) = collector_datalab_period(db_connection, period, kairos_server, data_filter, data_parser, es_object, index, doc_type, es_generator_data, bulk_fn, fetchsize)
        assert kairos_response.status_code ==  204
        assert es_ok == True
        logger.info("Test %s run successfully", test_collector_datalab_period.__name__)


    #TESTER FUNCTION UNCOMENT TO PROVEE MODULE
    my_test = MyTest()
    #my_test.test_kairos_insert()
    #my_test.test_es_insert()
    #test_kairos_collector_period()
    #test_kairos_insert_backards(DB_SESSION)
    #test_kairos_insert_present(DB_SESSION)
    #my_test.test_insert_datalab()
    test_collector_datalab_period(DB_SESSION)


    #DO NOT USE THIS MODULE HERE. IMPORT IN YOUR .py AND THEN RUN FUNCTIONS DESIRED 
    #db_connection = db_connection_setUp(DB_SESSION)
    #kairos_server="http://192.168.1.10:8080"
    #es_object = es_connection_setUp("http://192.168.1.10")
    #index="vltlog"
    #collector_datalab_present(db_connection, kairos_server, test_apply, kairos_parser, es_object, index)
    #kairos_insert_backards(db_connection, "http://192.168.1.10:8080", test_apply, kairos_parser, fetchsize=10000, datatime_batch=100)
    #kairos_insert_present(db_connection, "http://192.168.1.10:8080", test_apply, kairos_parser)

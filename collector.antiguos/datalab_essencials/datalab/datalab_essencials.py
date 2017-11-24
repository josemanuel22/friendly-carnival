#!/opt/anaconda3/bin/python  -u

import psycopg2
import datetime
import pickle

import time
import re
import math

import requests
import json
import gzip

from elasticsearch import Elasticsearch
import elasticsearch.connection
from elasticsearch.helpers import streaming_bulk, parallel_bulk
import logging
import sys
import datalab_logger
from miscellaneous import *
import es_queries 
import xxhash

#Alog Connection
DB_SESSION = "dbname=alog host=nico.pl.eso.org user=alogreader" 
DEBUG = False

logger_format = '%(asctime)s, %(name)-12s, %(levelname)-8s, %(processName)s, %(process)d,  %(pathname)s, %(filename)s, %(module)s, %(funcName)s, %(lineno)d , %(message)s'
datalab_logger_object = datalab_logger.datalab_logger(my_format=logger_format)
datalab_logger_inserted= datalab_logger_object.datalab_logger

def set_debug(boolean):
    global DEBUG
    DEBUG = boolean

def db_connection_setUp(db_session):
    """
    Creates a new database session
      
    :param db_session: String database propierties example alogdb: db_session='dbname=alog host=nico.pl.eso.org user=alogreader'
    :type db_session: String
    :return: connection instance  

    note:: see http://initd.org/psycopg/docs/module.html
    """
    db_connection = psycopg2.connect(db_session)
    db_connection.set_client_encoding("latin1")
    return db_connection

def db_execute_query(db_connection, query, query_args):
    """
    Execute a query in a database sesion
      
    :param db_connection: connection db instance
    :param query: String query. example "SELECT * FROM alog WHERE timestamp BETWEEN %s and %s"
    :param query_args: tuple of args of the query. example (time.time(), time.time()-10)
    :type db_connection: Connection
    :type query: String
    :type query_args: tuple
    :return: cursor  

    note:: see http://initd.org/psycopg/docs/usage.html
    """
    cursor = db_connection.cursor()
    datalab_logger_inserted.info("reading database[Query. May Take Time]...")
    cursor.execute(query, query_args)
    datalab_logger_inserted.info("finish to query database")
    return cursor

def kairos_insert(data, kairosdb_server, data_filter, data_parser):
    """
    Insert data in kairosdb_server that verify data_filter condition with structure data_parser   

    :param data: list of datapoints to insert
    :param kairosdb_server: 'http://kairosdb_server:kairosdb_port'
    :data_filter: funcion of filter of a data point. Return true if point satisfy condicion
    :data_parser: Funcion to parse your data to insert in kairos. see kairos_parser 
    :type data: iterable of iterables...easily --> list of (tuples or list) (but also other fancy stuffs may work) 
    :type kairosdb_server: String
    :type data_filter: data_filter::data_point -> Boolean  

    :return: tuple with kairos response object and number of data inserted 
    """
    kairosdb_data=[]; n_data_inserted = 0;
    kairosdb_data = list(filter(data_filter, data))
    gzipped = gzip.compress(bytes(json.dumps(list(map(data_parser, kairosdb_data))), 'latin1'));
    headers = {'content-type': 'application/gzip'} 
    response = requests.post(kairosdb_server + "/api/v1/datapoints", gzipped, headers=headers)
    datalab_logger_inserted.info("KAIROS : Inserted %d data : %d (status code)" % (len(kairosdb_data) if response.status_code == 204 else 0, response.status_code))
    n_data_inserted = len(kairosdb_data) if response.status_code == 204 else 0
    return (response, n_data_inserted)

def kairos_parser(data_point):
    """
    Kairos datalab basic parser  

    :param data_point: your data point
    :type data_point: tuple, list etc of values 
    :return: dictionary of your datapoint parsed
    :rtype: diccionary 
    """
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
    """
    Collect data from db_connection that verify sql_sentence query with sql_args and inserted in kairosdb_server
    after filtering then with data_filter and parsing then with data_parser  

    :param kairosdb_server: 'http://kairosdb_server:kairosdb_port'
    :data_filter: funcion of filter of a data point. Return true if point satisfy condicion
    :data_parser: Funcion to parse your data to insert in kairos. see kairos_parser 
    :type data: iterable of iterables...easily --> list of (tuples or list) (but also other fancy stuffs may work) 
    :type kairosdb_server: String
    :type data_filter: data_filter::data_point -> Boolean 
    :param db_connection: connection db instance
    :param query: String query. example "SELECT * FROM alog WHERE timestamp BETWEEN %s and %s"
    :param query_args: tuple of args of the query. example (time.time(), time.time()-10)
    :type db_connection: Connection
    :type query: String
    :param fetchsize: number of data to fetch at a time 1000 by default
    :type fetchsize: unsigned int
    :return: tuple with kairos response object and number of data inserted 
    """
    cursor = db_execute_query(db_connection, sql_sentence, sql_args)
    data = cursor.fetchmany(fetchsize)
    n_data_inserted = 0; response = None
    while data:
        (response, n) = kairos_insert(data, kairosdb_server, data_filter, data_parser)
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
        datalab_logger_inserted.debug(response.text)
        present = past; past = present - datetime.timedelta(seconds=datatime_batch)
        if DEBUG == True and (start_time - past) > datetime.timedelta(0,300,0): return (response, n_data_inserted)
        pickle.dump(present, open(TM_FILE,"wb"))

def kairos_insert_present(db_connection, kairosdb_server, data_filter, data_parser,fetchsize=10000, datatime_batch=1000, time_refresh=100):
    present = datetime.datetime.now()+datetime.timedelta(hours=-3); #computer local time 3h+  
    past = present - datetime.timedelta(minutes=1);
    while True:
        (response, n_data_inserted)  = kairos_collector_period(db_connection, (past, present), kairosdb_server, data_filter, data_parser)
        if n_data_inserted == 0:
            datalab_logger_inserted.info("KAIROS : No data to insert : time %s - %s" % (past, present))
            time.sleep(time_refresh)
            present = present+datetime.timedelta(seconds=100)
            continue
        datalab_logger_inserted.debug(response.text)
        tmp=present;
        past = present; 
        present=tmp+datetime.timedelta(seconds=datatime_batch) if datetime.datetime.now()-present > datetime.timedelta(seconds=datatime_batch) else datetime.datetime.now()
        if DEBUG :  return (response, n_data_inserted)

def es_connection_setUp(es_server):
    datalab_logger_inserted.info("ES : setUp connection")
    return  Elasticsearch(es_server, timeout=100, ignore=[400, 404])
 
def es_basic_generator_data(data,index, doc_type='default'):
    for data_point in data:
        mi_type=es_get_type(data_point)
        yield {
            '_index': "opslog" if mi_type=="opslog" else index,
            '_type': mi_type if doc_type=='default' else doc_type,
            '_id': xxhash.xxh64(str(data_point[10])+str(data_point[11])+str(data_point[12])+str(data_point[13])).hexdigest()+xxhash.xxh64(str(data_point[1])+str(data_point[0])+str(data_point[2])+str(data_point[3])+str(data_point[4])+str(data_point[5])+str(data_point[6])+str(data_point[7])+str(data_point[8])+str(data_point[9])).hexdigest(), 
            '_source':  {
                          '@timestamp': data_point[1],
                          'loghost': data_point[2],
                          'envname': data_point[3],
                          'procname':data_point[4],
                          'procid':data_point[5],
                          'module':data_point[6],
                          'keywname':data_point[8],
                          'keywvalue': float(data_point[9]) if mi_type=="opslog" else data_point[9],
                          'logtext':data_point[10],
                          'errstack': data_point[11],
                          'errstackidx': data_point[12],
                          'errlocation': data_point[13],
                          'errseverity': data_point[14], 
                        },
            }

def es_insert(es_object, data, index, doc_type='default', es_generator_data=es_basic_generator_data, bulk_fn=parallel_bulk):
    ok=True;result="NO_INSERTED"
    datalab_logger_inserted.info("ES : Inserting data")
    for ok, result in bulk_fn(es_object, es_generator_data(data, index, doc_type)): pass;
    datalab_logger_inserted.info("ES : Finish Insert data : Ok? %s" % ok)
    datalab_logger_inserted.debug(result)
    return (ok, result)

def es_insert_2(data, index, doc_type='default', es_generator_data=es_basic_generator_data, bulk_fn=streaming_bulk):
    es_object = Elasticsearch(['http://192.168.1.10','http://192.168.1.11','http://192.168.1.12', 'http://192.168.1.13'], maxsize=-1, ignore=[400, 404])
    #es_object = Elasticsearch(['http://192.168.1.13:9205'], maxsize=-1, ignore=[400, 404])
    ok=True;result="NO_INSERTED"
    for ok, result in bulk_fn(es_object, es_generator_data(data, index, doc_type)): pass;
    datalab_logger_inserted.info("ES : Finish Insert data : Ok? %s" % ok)
    datalab_logger_inserted.debug(result)
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


db_logger = datalab_logger.datalab_logger(name="collector_presentLogger", index_es="collector_present_pigglet", my_format = logger_format)
lastInserted_logger = db_logger.datalab_logger
#TODO estudiar cual es el retraso respecto de now() de alogdb
def collector_datalab_present(db_connection, kairos_server, data_filter, data_parser, \
                              es_object, index, doc_type='default', es_generator_data=es_basic_generator_data, bulk_fn=parallel_bulk, fetchsize=1000):
        try:
            past =  es_queries.getcollector_present_piglet("collector_present_pigglet", "python_log") #datetime.datetime.now();
        except: #No se ha encontrado al indice, esto quiere decir que nunca se ejecuto el collector
            past = datetime.datetime.now()-datetime.timedelta(seconds=100);
        present = past + datetime.timedelta(seconds=100);
        while True:
            (response,n_data_inserted,es_ok,es_result)=collector_datalab_period(db_connection,(past,present),kairos_server,data_filter,data_parser,\
                                                                                es_object,index,doc_type,es_generator_data, bulk_fn, fetchsize)
            if n_data_inserted == 0 and es_result == "NO_INSERTED": #El Programa va demasiado rapido?? hay que esperar a que alog se refresque
                datalab_logger_inserted.info("No data to insert, time %s - %s" % (past, present))
                time.sleep(30)
                present = present+datetime.timedelta(seconds=5) #Por si hay hoyos de datos
                continue
            datalab_logger_inserted.info("collector_datalab_backwards : Inserted period: time %s - %s" % (past, present))
            #datalab_logger_inserted.debug(response.text)
            lastInserted_logger.info("collector_datalab_present: LAST_TIMESTAMP_INSERTED: %s %s"%(past, present), extra={'collector_piglet': present})
            tmp=present;
            past = present;
            present=datetime.datetime.now()
            time.sleep(10) #Insertamos con 10seg de restraso...con esto estamos seguros que alog tiene ya los datos (TODO estudiar el restraso de alogdb)
            if DEBUG :  return (response, n_data_inserted, es_ok, es_result)


db2_logger = datalab_logger.datalab_logger(name="collector_backwardsLogger", index_es="collector_backwards_piglet", my_format = logger_format)
collector_backwardsLogger = db2_logger.datalab_logger
def collector_datalab_backwards(db_connection, kairos_server, data_filter, data_parser, \
                              es_object, index, doc_type='default', es_generator_data=es_basic_generator_data, bulk_fn=parallel_bulk, fetchsize=1000):
    try:
        past =  es_queries.getcollector_backwards_piglet("collector_backwards_piglet", "python_log") #datetime.datetime.now();   
    except:
        past = datetime.datetime.now();
    present = past - datetime.timedelta(seconds=100);
    while True:
        result=collector_datalab_period(db_connection,(past,present),kairos_server,data_filter,data_parser,es_object,index,doc_type,es_generator_data, bulk_fn, fetchsize)
        datalab_logger_inserted.info("collector_datalab_backwards : Inserted period: time %s - %s" % (past, present))
        #datalab_logger_inserted.debug(result[0].text)
        collector_backwardsLogger.info("collector_datalab_backwards: LAST_TIMESTAMP_INSERTED: %s %s"%(past, present), extra={'collector_piglet': present})     
        past = present;
        present= present-datetime.timedelta(seconds=100)
        if DEBUG :  return result

if __name__ == "__main__":
    print(datalab_logger_object, datalab_logger_inserted)

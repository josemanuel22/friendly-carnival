import pandas as pd
import numpy as np
import time
import os
from datalab_essencials import *
import glob
from itertools import product
from multiprocessing import Pool, Lock
import multiprocessing
from functools import partial

def csv_filter(data_point):
    val = str(data_point[8])
    try:
         float(val)
         return True
    except (ValueError, TypeError):
        return False

def csv_kairos_parser(data_point):
    data_point_parsed = {
        "name": clean_for_kairos(data_point[7]),
        "timestamp": int(time.mktime(data_point[1]))*1000, #kairosdb time in miliseconds
        "value"    : float(data_point[8]),
        "tags": {
             "proc": clean_for_kairos(data_point[4]),
             "env": clean_for_kairos(data_point[3]),
             "loghost": clean_for_kairos(data_point[2]),
             "logtext": clean_for_kairos(str(data_point[9]).replace(" ", "_")),
        }
    }
    return data_point_parsed

csv_basic_header=['typelog', 'tm', 'loghost', 'env', 'procname', 'procid', 'module', 'keyw', 'keyv', 'logtext', 'errstack', 'errstackidx', 'errlocation', 'errseverity']
def csv_kairos_import(path_to_csv,kairos_server="http://192.168.1.10:8080", header=csv_basic_header,csv_filter=csv_filter, kairos_parser=csv_kairos_parser):
    df = pd.read_csv(filepath_or_buffer=path_to_csv, names=header , compression='gzip', na_values=[""], parse_dates=['tm'], date_parser=lambda x: time.strptime(x, "%Y-%m-%d %H:%M:%S"))
    df = df.fillna("")
    return kairos_insert(df.values[:len(df.values)], kairos_server, csv_filter, kairos_parser)
         
#kairos_import_csv(path_to_csv='/data/datalake/fitslake/datalab_backup/scripts_orig/kairos_insert/inputlog_processed/whki.2010-04-05.csv.gz', kairos_server="http://192.168.1.10:8080", header=basic_csv_header,csv_filter=csv_filter, kairos_parser=kairos_csv_parser))


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


def csv_es_generator_data(data,index, doc_type):
    for data_point in data:
        mi_type=es_get_type(data_point)
        yield {
            '_index': 'csv_opslog' if mi_type=='opslog' else index,
            '_type': mi_type if doc_type=='default' else doc_type,
            '_source':  {
                          '@timestamp': data_point[1],
                          'loghost': data_point[2] ,
                          'envname': data_point[3],
                          'procname':data_point[4],
                          'procid':data_point[5],
                          'module':data_point[6],
                          'keywname':data_point[7],
                          'keywvalue': float(data_point[8]) if mi_type=='opslog' else data_point[8],
                          'logtext':data_point[9], 
                        },
            }

def csv_es_import(path_to_csv, es_server, index, doc_type, header=csv_basic_header, es_generator_data=csv_es_generator_data):
    es = es_connection_setUp(es_server)
    df = pd.read_csv(filepath_or_buffer=path_to_csv, names=header, compression='gzip', na_values=[""], parse_dates=['tm'], date_parser=lambda x: time.strptime(x, "%Y-%m-%d %H:%M:%S"), encoding='latin-1')
    df = df.fillna("")
    return es_insert(es, df.values[:len(df.values)], index, doc_type, es_generator_data, parallel_bulk)


def csv_datalab_import_file(path_to_csv, kairos_server, es_server, index, doc_type='default', kairos_parser=csv_kairos_parser, header=csv_basic_header, es_generator_data=csv_es_generator_data):
    es = es_connection_setUp(es_server)
    df = pd.read_csv(filepath_or_buffer=path_to_csv, names=header, compression='gzip', na_values=[""], parse_dates=['tm'], date_parser=lambda x: time.strptime(x, "%Y-%m-%d %H:%M:%S"), encoding='latin-1')
    df = df.fillna("")
    (kairos_response, n_data_insert)=kairos_insert(df.values[:len(df.values)], kairos_server, csv_filter, kairos_parser)
    (es_response, es_ok)=es_insert(es_object, df.values[:len(df.values)], index, doc_type, es_generator_data, parallel_bulk)
    return (es_response, es_ok) 


class Import_params:
    def __init__(self):
        self.kairos_server = None
        self.es_object = None
        self.index = None
        self.doc_type = None
        self.header = None
        self.es_generator_data = None
    
    def set_params(self, kairos_server, es_object, index, doc_type, header, es_generator_data):
        self.kairos_server = kairos_server
        self.es_object = es_object
        self.index = index
        self.doc_type = doc_type
        self.header = header
        self.es_generator_data = es_generator_data
   
def f(mi_file):
        global total; global total_insert
        global import_params
        p = multiprocessing.current_process()
        df = pd.read_csv(filepath_or_buffer=mi_file, names=import_params.header, compression='gzip', na_values=[""], parse_dates=['tm'], date_parser=lambda x: time.strptime(x, "%Y-%m-%d %H:%M:%S"), encoding='latin-1')
        df = df.fillna("")
        s = time.time()
        (es_response, es_ok)=es_insert(import_params.es_object, df.values[:len(df.values)], import_params.index, import_params.doc_type, import_params.es_generator_data, parallel_bulk)
        print("finish to import file", mi_file)
        total_insert+=os.path.getsize(mi_file)
        print('Total procesed by %d:  %.2f MB'%((p.pid,(total_insert/1000000))))
        return (es_response, es_ok)

total=0;total_insert=0
import_params = Import_params()
def csv_datalab_import_set_params(path_to_csv, index, doc_type='default', header=csv_basic_header, es_generator_data=csv_es_generator_data):
    files = glob.glob(path_to_csv)
    global import_params
    global total
    global total_insert
    total = 0;total_insert=0
    for mi_file in files: total+=os.path.getsize(mi_file)
    es = Elasticsearch(['http://192.168.1.10', 'http://192.168.1.11','http://192.168.1.12', 'http://192.168.1.13'], request_timeout=30, maxsize=1000,ignore=[400, 404])
    import_params.set_params(kairos_server, es, index, doc_type, header, es_generator_data)

def csv_datalab_import(path_to_cs):
    files = glob.glob(path_to_csv)
    global total_insert
    p = Pool(4)
    result=p.map(f, files)
    cleaned = [x for x in result if not x is None]
    pool.close()
    pool.join()
    return result

if __name__ == "__main__":
    #csv_kairos_import('/data/datalake/fitslake/datalab_backup/scripts_orig/kairos_insert/inputlog_processed/whki.2010-04-06.csv.gz', kairos_server="http://192.168.1.10:8080", header=csv_basic_header,csv_filter=csv_filter, kairos_parser=csv_kairos_parser)
    #csv_es_import('/data/datalake/fitslake/datalab_backup/scripts_orig/kairos_insert/inputlog_processed/whki.2010-04-06.csv.gz', "http://192.168.1.10", "csv_import_prueba", "csv_import_prueba_doc_type")
    path_to_csv='/root/03/*.csv.gz'
    csv_datalab_import_set_params(path_to_csv, "http://192.168.1.10:8080", "csv_import_prueba")
    s=time.time()
    csv_datalab_import(path_to_csv)
    print(time.time()-s)


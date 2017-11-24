import pandas as pd
import numpy as np
import time
import os
from datalab_essencials import *
import glob
from itertools import product
from multiprocessing import Pool
import multiprocessing
from elasticsearch.helpers import streaming_bulk, parallel_bulk
from pathos.multiprocessing import ProcessingPool as Pool
import miscellaneous 

from pathos.parallel import stats
from pathos.parallel import ParallelPool
from pathos.secure import Tunnel
from PyCRC.CRC16 import CRC16

import xxhash


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
        "timestamp": int(time.mktime(time.strptime(str(data_point[1]), "%Y-%m-%d %H:%M:%S")))*1000, #kairosdb time in miliseconds
        "value"    : float(data_point[8]),
        "tags": {
             "proc": clean_for_kairos(data_point[4]),
             "env": clean_for_kairos(data_point[3]),
             "logh4pysost": clean_for_kairos(data_point[2]),
             "logtext": clean_for_kairos(str(data_point[9]).replace(" ", "_")),
        }
    }
    return data_point_parsed

csv_basic_header=['typelog', 'tm', 'loghost', 'env', 'procname', 'procid', 'module', 'keyw', 'keyv', 'logtext', 'errstack', 'errstackidx', 'errlocation', 'errseverity']

def csv_kairos_import(path_to_csv,kairos_server="http://192.168.1.10:8080", header=csv_basic_header,csv_filter=csv_filter, kairos_parser=csv_kairos_parser):
    chunksize=int((int(os.path.getsize(path_to_csv)+1)/int(miscellaneous.CSV_NUM_BYTESXLINE))/4)
    df = pd.read_csv(filepath_or_buffer=path_to_csv, names=header, compression='gzip', na_values=[""], parse_dates=['tm'], engine='c', encoding='latin-1', usecols=[0,1,2,3,4,5,6,7,8,9], na_filter=False,  chunksize=chunksize)
    #result=[]
    pool = multiprocessing.Pool(16)
    for chunk in df:
        r=pool.apply_async(kairos_insert,args=[chunk.values, kairos_server, csv_filter, kairos_parser])#,callback=callBack)
        #result.append(r.get()) #RECOJER LOS RESULTADOS RALENTIZA X4
    pool.close()
    pool.join()
    return r.get() #DEVOLVEMOS SOLO EL ULTIMO RESULTADO POR MOTIVOS DE VELOCIDAD. CON ESTE SE PUEDE DEDUCIR SI LA INSERCION FUE BIEN
    
def is_keywvalue_num(keywvalue):
    try:
        float(keywvalue)
        return True
    except:
        return False

def es_get_type(data_point):
    if is_keywvalue_num(data_point[8]) and data_point[0].lower()=='fpar':
        return "opslog"
    else:
        return data_point[0].lower()


def csv_es_generator_data(data,index, doc_type='default'):
    for data_point in data:
        mi_type=es_get_type(data_point)
        yield {
            '_index': 'jose_opslog' if mi_type=='opslog' else index,
            '_type': mi_type if doc_type=='default' else doc_type,
            '_id': xxhash.xxh64(str(data_point[10])+str(data_point[11])+str(data_point[12])+str(data_point[13])).hexdigest()+xxhash.xxh64(str(data_point[1])+str(data_point[0])+str(data_point[2])+str(data_point[3])+str(data_point[4])+str(data_point[5])+str(data_point[6])+str(data_point[7])+str(data_point[8])+str(data_point[9])).hexdigest(), 
            '_source':  {
                          '@timestamp': data_point[1],
                          'loghost': data_point[2] ,
                          'envname': data_point[3],
                          'procname':data_point[4],
                          'procid':data_point[5],
                          'module':data_point[6],
                          'keywname':data_point[7],
                          'keywvalue': float(data_point[8]) if mi_type=='opslog' else str(data_point[8]),
                          'logtext':data_point[9],
                          'errstack': data_point[10],
                          'errstackidx': data_point[11],
                          'errlocation': data_point[12],
                          'errseverity': data_point[13],
                        },
            }


def csv_es_import(path_to_csv, index, doc_type='default', header=csv_basic_header, es_generator_data=csv_es_generator_data):
    chunksize=int((int(os.path.getsize(path_to_csv)+1)/int(miscellaneous.CSV_NUM_BYTESXLINE))/(multiprocessing.cpu_count()))
    df = pd.read_csv(filepath_or_buffer=path_to_csv, names=header, compression='gzip', na_values=[""], parse_dates=['tm'], engine='c', encoding='latin-1', usecols=[0,1,2,3,4,5,6,7,8,9], na_filter=False,  chunksize=chunksize)
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    for chunk in df:
        result=pool.apply_async(es_insert_2,args=[chunk.values, index, doc_type, es_generator_data ])#,callback=callBack)
    pool.close()
    pool.join()
    return result

def csv_datalab_import_file(path_to_csv,kairos_server,index,doc_type='default',kairos_parser=csv_kairos_parser,header=csv_basic_header,es_generator_data=csv_es_generator_data):
    nproc_to_use=4
    es_chunksize=10000 #int((int(os.path.getsize(path_to_csv)+1)/int(miscellaneous.CSV_NUM_BYTESXLINE))/(nproc_to_use))
    df = pd.read_csv(filepath_or_buffer=path_to_csv, names=header, compression='gzip', na_values=[""], parse_dates=['tm'], engine='c', encoding='latin-1', na_filter=False, chunksize=es_chunksize)

    pool = multiprocessing.Pool(nproc_to_use)
    for chunk in df:
        kairos_result=pool.apply_async(kairos_insert,args=[chunk.values, kairos_server, csv_filter, kairos_parser])#,callback=callBack)
        es_result=pool.apply_async(es_insert_2,args=[chunk.values, index, doc_type, es_generator_data ])
    pool.close()
    pool.join()
    return (kairos_result, es_result)


def csv_datalab_import(path_to_csv,kairos_server,index,doc_type='default',kairos_parser=csv_kairos_parser,header=csv_basic_header,es_generator_data=csv_es_generator_data):
    start=time.time()
    files = glob.glob(path_to_csv)
    total = 0;total_inserted=0;bash=0
    for mi_file in files: total+=os.path.getsize(mi_file)
    print(total)
    for f in files:
        s = time.time()
        result = csv_datalab_import_file(f,kairos_server,index,doc_type,kairos_parser,header,es_generator_data)
        time.sleep(1)
        total_inserted+=os.path.getsize(f);bash+=os.path.getsize(f)
        if (bash/1000000)>10: #Cada 100MB damos un respiro al sistema
            print("Descando")
            time.sleep(10)
            bash = 0
        #timeToFinish=int((((total/total_inserted)*(time.time()-s))+s)-time.time())/60
        print("Inserted: %s file in %.2f secs, %.2f MB-%.2f MB" % (f,time.time()-s,total_inserted/1000000.0,total/1000000.0)) 
    return result 


if __name__ == "__main__":
    path_to_csv='/data/datalake2/vltlogs/csv_logs/2007/??/*.csv.gz'
    s=time.time()
    #csv_kairos_import(path_to_csv,kairos_server="http://192.168.1.11:8080")
    es_server='http://192.168.1.10';index="jose_vltlog"
    #csv_datalab_import_file(path_to_csv, "http://192.168.1.10:8080", index)
    csv_datalab_import(path_to_csv, "http://192.168.1.10:8080", index)
    #csv_es_import(path_to_csv, es_server, index)
    print("Tiempo total:", time.time()-s)
    #csv_datalab_import(path_to_csv)



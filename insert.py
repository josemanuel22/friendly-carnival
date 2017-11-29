import datalab.src.connections.connections
import datalab.miscellaneous
import datalab.src.importers.csv_importers
import argparse
import datetime
import re
import glob
import os

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from datalab.src.loggers import datalab_loggers

datalab_logger_object_importer = datalab_loggers.datalab_logger(my_format=datalab_loggers.DATALAB_LOGGER_FORMAT)
datalab_logger_importer = datalab_logger_object_importer.datalab_logger



def _getoghost(f):
    i = f.find('/', 0)
    while -1 != i:
        i+=1
        j=i
        i = f.find('/', i)
    k = f[j:].find('.')
    return f[j:j+k]



parser = argparse.ArgumentParser(prog='Insert csv')
subparser = parser.add_subparsers(dest="subparser")

parser_kairos = subparser.add_parser('kairos')
parser_kairos.add_argument(
        '-path_to_csvs', type=str, help='path to csv directories', required=True)
parser_kairos.add_argument(
        '-kairos_server', type=str, help='kairos server', required=False, default="http://192.168.1.10:8080")

parser_es = subparser.add_parser('es')
parser_es.add_argument(
        '-path_to_csvs', type=str, help='path to csv directories', required=True)
parser_es.add_argument(
        '-es_server', type=str, help='es server', nargs='*', required=False, default="http://192.168.1.10:9200")
parser_es.add_argument(
        '-auto_generate_index_suffix', type=bool, help='If true docs timestamps suffix name will set as index suffix', required=False, default=True)
parser_es.add_argument(
        '-frecuency', type=str, help='frecuency add to es index', choices=['DAILY','MONTHLY','YEARLY'], required=False, default='MONTHLY')
parser_es.add_argument(
        '-index_suffix', type=bool, help='Index suffix to add in case auto_generate_index_suffix is false', required=False)
parser_datalab.add_argument(
        '-reinsert',  type=bool, help='if true it reinsert the files even if it is already in the database', required=False, default=False)
parser_es.add_argument(
        '-chunksize',  type=str, help='Number of data readof the csv in one shot', required=False, default="50000")

parser_datalab = subparser.add_parser('datalab')
parser_datalab.add_argument(
        '-path_to_csvs', type=str, help='path to csv directories', required=True)
parser_datalab.add_argument(
        '-kairos_server', type=str, help='kairos server', required=False, default="http://192.168.1.10:8080")
parser_datalab.add_argument(
        '-es_server', type=str, help='es server', nargs='*', required=False, default="192.168.1.10")
parser_datalab.add_argument(
        '-auto_generate_index_suffix', type=bool, help='If true docs timestamps suffix name will set as index suffix', required=False, default=True)
parser_datalab.add_argument(
        '-frecuency', type=str, help='frecuency add to es index', choices=['DAILY','MONTHLY','YEARLY'], required=False, default='MONTHLY')
parser_datalab.add_argument(
        '-index_suffix', type=bool, help='Index suffix to add in case auto_generate_index_suffix is false', required=False)
parser_datalab.add_argument(
        '-reinsert',  type=bool, help='if true it reinsert the files even if it is already in the database', required=False, default=False)
parser_datalab.add_argument(
        '-chunksize',  type=str, help='Number of data readof the csv in one shot', required=False, default="50000")

parser_datalab = subparser.add_parser('samp')
parser_datalab.add_argument(
        '-path_to_samp', type=str, help='path to samp directories', required=True)
parser_datalab.add_argument(
        '-kairos_server', type=str, help='kairos server', required=False, default="http://192.168.1.10:8080")


args = parser.parse_args()
if args.subparser == "kairos":
    files = glob.glob(args.path_to_csvs)
    kairos_server=args.kairos_server
    total = 0;
    total_inserted = 0;
    for f in files: total += os.path.getsize(f)
    for f in files:
        datalab.src.importers.csv_importers.csv_kairos_import(f, kairos_server)
        total_inserted += os.path.getsize(f);
        datalab_logger_importer.info("File %s %f MB of %f MB"%(f, total_inserted/1000000.0, total/1000000.0))
elif args.subparser == "es":
    files = glob.glob(args.path_to_csvs)
    es_server= args.es_server
    chunksize=int(args.chunksize)
    total = 0; total_inserted = 0;
    for f in files: total += os.path.getsize(f)
    for f in files:
        if args.auto_generate_index_suffix==True:
            date = re.findall(r"\b([0-9]{4})[-/:]([0-9]{1,2})[-/:]([0-9]{1,2})\b",f)
            if args.frecuency == 'DAILY':
                index_suffix=date[0][0]+"."+date[0][1]+"."+date[0][2]
            elif args.frecuency == 'MONTHLY':
                index_suffix=date[0][0]+"."+date[0][1]
            elif args.frecuency == 'YEARLY':
                index_suffix=date[0][0]
        else:
            index_suffix=args.index_suffix
        if args.reinsert == False:
            client = Elasticsearch(es_server)
            loghost = _getoghost(f)
            s = Search(using=client, index="vltlog-"+index_suffix).query("match", logtext="Creating empty file to archive normal logs").filter("term",loghost=loghost)
            if s.execute():
                continue; 
        datalab.src.importers.csv_importers.csv_es_import(f, es_server, chunksize=chunksize, index_suffix=index_suffix)
        total_inserted += os.path.getsize(f)
        datalab_logger_importer.info("File $s %f MB of %f MB"%(f, total_inserted/1000000.0, total/1000000.0))
elif args.subparser == "datalab":
    files = glob.glob(args.path_to_csvs)
    kairos_server=args.kairos_server
    es_server= args.es_server
    chunksize=int(args.chunksize)
    total = 0; total_inserted = 0;
    for f in files: total += os.path.getsize(f)
    for f in files:
        if args.auto_generate_index_suffix==True:
            date = re.findall(r"([0-9]{4})[-/:]([0-9]{1,2})[-/:]([0-9]{1,2})",f)
            if args.frecuency == 'DAILY':
                index_suffix=date[0][0]+"."+date[0][1]+"."+date[0][2]
            elif args.frecuency == 'MONTHLY':
                index_suffix=date[0][0]+"."+date[0][1]
            elif args.frecuency == 'YEARLY':
                index_suffix=date[0][0]
        else:
            index_suffix=args.index_suffix
        if args.reinsert == False:
            client = Elasticsearch(es_server)
            loghost = _getoghost(f)
            s = Search(using=client, index="vltlog-"+index_suffix).query("match", logtext="Creating empty file to archive normal logs").filter("term",loghost=loghost)
            if s.execute():
                continue;
        datalab.src.importers.csv_importers.csv_import_file(f, kairos_server, es_server, es_chunksize=chunksize, index_suffix=index_suffix)
        total_inserted += os.path.getsize(f)
        datalab_logger_importer.info("File %s %f MB of %f MB"%(f, total_inserted/1000000.0, total/1000000.0))

elif args.subparser == "samp":
    files = glob.glob(args.path_to_csvs)
    kairos_server=args.kairos_server
    total = 0;total_inserted = 0;
    for f in files: total += os.path.getsize(f)
    for f in files:
        datalab.src.importers.samp_importers.samp_import(f,kairos_server)
        total_inserted += os.path.getsize(f);
        datalab_logger_importer.info("File %s %f MB of %f MB"%(f, total_inserted/1000000.0, total/1000000.0))

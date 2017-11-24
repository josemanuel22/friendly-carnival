
from datalab_essencials.datalab.datalab_essencials import *
import sys
import argparse
import os.path
sys.path.append('/vlt/collector/datalab_essencials')

parser = argparse.ArgumentParser(description='Collector backwards 0XBEBACAFE')
parser.add_argument(
        '-s', '--server', type=str, help='Server name', required=False, default=DB_SESSION)
parser.add_argument(
        '-kairos', '--kairos_server', type=str, help='kairos_server', required=False, default="http://192.168.1.10")

parser.add_argument(
        '-k_p', '--kairos_port', type=str, help='kairos_port', required=False, default="8080")

parser.add_argument(
        '-es', '--es_server', type=str, help='es server', required=False, default="http://192.168.1.10")

parser.add_argument(
        '-es_p', '--es_port', type=str, help='es port', required=False, default="9200")

parser.add_argument(
        '-index', '--index', type=str, help='Index where insert', required=False, default="vltlog")

parser.add_argument(
        '-doc_type', '--doc_type', type=str, help='doc es type Using the default you will have FPARS, LOGS etc', required=False, default="default")

parser.add_argument(
        '-fetchSize', '--fetchSize', type=str, help='Number of intem inserted each time', required=False, default="1000")


args = parser.parse_args()

db_connection=args.server
kairos_server=args.kairos_server+":"+args.kairos_port
data_filter=test_apply
data_parser=kairos_parser
es_server=args.es_server+":"+args.es_port#"http://192.168.1.10"

index=args.index   #"jose_vltlog"
doc_type=args.doc_type

es_generator_data=es_basic_generator_data
db_connection = db_connection_setUp(db_connection)
es_object = es_connection_setUp(es_server)
collector_datalab_backwards(db_connection,kairos_server,data_filter,data_parser,es_object,index,doc_type,es_generator_data)




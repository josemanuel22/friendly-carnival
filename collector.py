import datalab.src.connections.connections
import datalab.miscellaneous
import datalab.src.importers.collectors_importers
import argparse
import datetime
#sys.path.append('/vlt/collector/datalab_essencials')

#ADD Sub-commands present, backwards...


parser = argparse.ArgumentParser(prog='Collector')
subparser = parser.add_subparsers(dest="subparser")

parser_present = subparser.add_parser('present')
parser_present.add_argument(
        '-db_server', type=str, help='alogdb Server name', required=False, default=datalab.src.connections.connections.DB_SESSION)
parser_present.add_argument(
        '-kairos_server', type=str, help='kairos server', required=False, default="http://192.168.1.10:8080")
parser_present.add_argument(
        '-es_server', type=str, help='es server', nargs='*', required=False, default="134.171.189.10:9200")
parser_present.add_argument(
        '-frecuency', type=str, help='frecuency add to es index', choices=['NONE','DAILY','MONTHLY','YEARLY'], required=False, default='MONTHLY')
parser_present.add_argument(
        '-delay', type=int, help='delay n of sec collector run behind present', required=False, default=100)
parser_present.add_argument(
        '-fetchsize',  type=str, help='Number of data read of alogdb in one shot', required=False, default="1000")

parser_backwards = subparser.add_parser('backwards')
parser_backwards.add_argument(
        '-db_server', type=str, help='alogdb Server name', required=False, default=datalab.src.connections.connections.DB_SESSION)
parser_backwards.add_argument(
        '-kairos_server', type=str, help='kairos server', required=False, default="http://192.168.1.10:8080")
parser_backwards.add_argument(
        '-es_server', type=str, help='es server', nargs='*', required=False, default="http://192.168.1.10:9200")
parser_backwards.add_argument(
        '-frecuency', type=str, help='frecuency add to es index', choices=['NONE','DAILY','MONTHLY','YEARLY'], required=False, default='MONTHLY')
parser_backwards.add_argument(
        '-start', type=str, help='frecuency add to es index', required=False, default="-1")
parser_backwards.add_argument(
        '-fetchsize',  type=str, help='Number of data read of alogdb in one shot', required=False, default="1000")

parser_period = subparser.add_parser('period')
parser_period.add_argument(
        '-db_server', type=str, help='alogdb Server name', required=False, default=datalab.src.connections.connections.DB_SESSION)
parser_period.add_argument(
        '-kairos_server', type=str, help='kairos server', required=False, default="http://192.168.1.10:8080")
parser_period.add_argument(
        '-es_server', type=str, help='es server', required=False, default="192.168.1.10")
parser_period.add_argument(
        '-frecuency', type=str, help='frecuency add to es index', required=False, default='MONTHLY')
parser_period.add_argument(
        '-fetchsize',  type=str, help='Number of data read of alogdb in one shot', required=False, default="1000")
parser_period.add_argument(
        '-from_', type=str, help='time to start collector', required=True)
parser_period.add_argument(
        '-to_', type=str, help='time end collector', required=True)

args = parser.parse_args()
if args.subparser == "present":
    db_connection=datalab.src.connections.connections.db_connection_setUp(args.db_server)
    kairos_server=args.kairos_server
    es_server= args.es_server
    es_object = datalab.src.connections.connections.es_connection_setUp(es_server)
    if args.frecuency == 'DAILY':
        index_suffix_frecuency=datalab.miscellaneous.SuffixNameFrecuency.DAILY
    elif args.frecuency == 'MONTHLY':
        index_suffix_frecuency=datalab.miscellaneous.SuffixNameFrecuency.MONTHLY
    elif args.frecuency == 'YEARLY':
        index_suffix_frecuency=datalab.miscellaneous.SuffixNameFrecuency.YEARLY
    else:
        index_suffix_frecuency=datalab.miscellaneous.SuffixNameFrecuency.NONE
    delay=int(args.delay)
    fetchsize=int(args.fetchsize)
    datalab.src.importers.collectors_importers.collector_datalab_present(db_connection, kairos_server, es_object , delay=delay, index_suffix_frecuency=index_suffix_frecuency, fetchsize=fetchsize)
elif args.subparser == "backwards":
    db_connection=datalab.src.connections.connections.db_connection_setUp(args.db_server)
    kairos_server=args.kairos_server
    es_server= args.es_server
    es_object = datalab.src.connections.connections.es_connection_setUp(es_server)
    if args.frecuency == 'DAILY':
        index_suffix_frecuency=datalab.miscellaneous.SuffixNameFrecuency.DAILY
    elif args.frecuency == 'MONTHLY':
        index_suffix_frecuency=datalab.miscellaneous.SuffixNameFrecuency.MONTHLY
    elif args.frecuency == 'YEARLY':
        index_suffix_frecuency=datalab.miscellaneous.SuffixNameFrecuency.YEARLY
    else:
        index_suffix_frecuency=datalab.miscellaneous.SuffixNameFrecuency.NONE
    if args.start=="-1":
        start=-1
    else:
        start=datetime.datetime.strptime(args.start, "%Y %m %d %H:%M:%S.%f")
    fetchsize=int(args.fetchsize)
    datalab.src.importers.collectors_importers.collector_datalab_backwards(db_connection, kairos_server, es_object , start=start, index_suffix_frecuency=index_suffix_frecuency, fetchsize=fetchsize)
   
elif args.subparser == "period":
    db_connection=datalab.src.connections.connections.db_connection_setUp(args.db_server)
    kairos_server=args.kairos_server
    es_server= args.es_server
    es_object = datalab.src.connections.connections.es_connection_setUp(es_server)
    if args.frecuency == 'DAILY':
        index_suffix_frecuency=datalab.miscellaneous.SuffixNameFrecuency.DAILY
    elif args.frecuency == 'MONTHLY':
        index_suffix_frecuency=datalab.miscellaneous.SuffixNameFrecuency.MONTHLY
    elif args.frecuency == 'YEARLY':
        index_suffix_frecuency=datalab.miscellaneous.SuffixNameFrecuency.YEARLY
    else:
        index_suffix_frecuency=datalab.miscellaneous.SuffixNameFrecuency.NONE
    from_ = datetime.datetime.strptime(args.from_, "%Y %m %d %H:%M:%S.%f")
    to_ = datetime.datetime.strptime(args.to_, "%Y %m %d %H:%M:%S.%f")    
    fetchsize=int(args.fetchsize)
    datalab.src.importers.collectors_importers.collector_datalab_period_2(db_connection, kairos_server, es_object , (from_, to_), index_suffix_frecuency=index_suffix_frecuency, fetchsize=fetchsize)




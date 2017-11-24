import unittest
import sys
import os.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
sys.path.append(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)),'datalab'))
from datalab.datalab_essencials import *

class MyTest(unittest.TestCase):
    def test_kairos_insert(self, kairosdb_server="http://192.168.1.10:8080", data_filter=test_apply, data_parser= kairos_parser):
        data=[(1546655907,datetime.datetime(2017,9,25,4,11,23),'wat3tcs','lat3vcm','logManager',0,'','FPAR','TESTER_KAIROS_INSERT','2.7182818','Prueba msg','',0,'',''),\
	      (1546636293,datetime.datetime(2017,9,25,4,12,27),'wat4tcs','lat4vcm','logManager',0,'','FPAR','TESTER_KAIROS_INSERT','3.1415926','Prueba msg','',0,'','')]
        (response, n_data_inserted) = kairos_insert(data, kairosdb_server, data_filter, data_parser)
        self.assertEqual(response.status_code, 204)
        self.assertEqual(n_data_inserted, len(list(filter(data_filter, data))))

    def test_es_insert(self, es_server="http://192.168.1.10"):
        data = [(1546655907,datetime.datetime(2017,9,25,4,11,23),'wat3tcs','lat3vcm','logManager',0,'','FPAR','TESTER_ES_INSERT','4.6692','Es Trascendente','',0,'',''),\
                (1546636293,datetime.datetime(2017,9,25,4,12,27),'wat4tcs','lat4vcm','logManager',0,'','FPAR','TESTER_ES_INSERT','1.6180','La perfeccion.','',0,'','')]
        es = es_connection_setUp(es_server)
        (ok, result) = es_insert(es_object=es, data=data, index="1")
        self.assertEqual(ok, True)

    def test_insert_datalab(self,kairos_server="http://192.168.1.10:8080",data_filter=test_apply,data_parser=kairos_parser,es_server="http://192.168.1.10",\
                            index="tester_insert_datalab_index",doc_type="tester_insert_datalab_doc_type",es_generator_data=es_basic_generator_data,\
                            bulk_fn=parallel_bulk, fetchsize=1000):
        data = [(1546655907,datetime.datetime(2017,9,25,4,11,23),'wat3tcs','lat3vcm','logManager',0,'','FPAR','TESTER_DATALAB_INSERT','4.669201','Es Trascendente','',0,'',''),\
                (1546636293,datetime.datetime(2017,9,25,4,12,27),'wat4tcs','lat4vcm','logManager',0,'','FPAR','TESTER_DATALAB_INSERT','1.618033','La perfeccion.','',0,'','')]
        es_object = es_connection_setUp(es_server)
        (kairos_response,n_data_inserted,es_ok,es_result)=insert_datalab(data,kairos_server,data_filter,data_parser,es_object,index,doc_type,es_generator_data,bulk_fn, fetchsize)
        self.assertEqual(kairos_response.status_code, 204)
        self.assertEqual(n_data_inserted, len(list(filter(data_filter, data))))
        self.assertEqual(es_ok, True)

    def test_kairos_collector_period(self, kairosdb_server="http://192.168.1.10:8080", data_filter=test_apply, data_parser=kairos_parser, fetchsize=10000):
        db_connection = db_connection_setUp(DB_SESSION)
        period=(datetime.datetime.now()-datetime.timedelta(seconds=10), datetime.datetime.now())
        cursor = db_execute_query(db_connection, "SELECT * FROM alog WHERE timestamp BETWEEN %s and %s", period)
        data =  cursor.fetchall()
        (response, n_data_inserted) = kairos_collector_period(db_connection, period, kairosdb_server, data_filter, data_parser)
        assert response.status_code ==  204
        assert len(list(filter(data_filter, data))) == n_data_inserted

    def test_kairos_collector_period(self, kairosdb_server="http://192.168.1.10:8080", data_filter=test_apply, data_parser=kairos_parser, fetchsize=10000):
        db_connection = db_connection_setUp(DB_SESSION)
        period=(datetime.datetime.now()-datetime.timedelta(seconds=10), datetime.datetime.now())
        cursor = db_execute_query(db_connection, "SELECT * FROM alog WHERE timestamp BETWEEN %s and %s", period)
        data =  cursor.fetchall()
        (response, n_data_inserted) = kairos_collector_period(db_connection, period, kairosdb_server, data_filter, data_parser)
        assert response.status_code ==  204
        assert len(list(filter(data_filter, data))) == n_data_inserted

    def test_kairos_insert_backards(self, db_connection=DB_SESSION,kairosdb_server="http://192.168.1.10:8080",data_filter=test_apply,data_parser=kairos_parser,\
                                    fetchsize=10000,datatime_batch=100):
        db_connection = db_connection_setUp(db_connection)
        DEBUG = True
        (response, n_data_insert) = kairos_insert_backards(db_connection, kairosdb_server, data_filter, data_parser)
        assert response.status_code ==  204
        DEBUG = False

    def test_kairos_insert_present(self, db_connection=DB_SESSION, kairosdb_server="http://192.168.1.10:8080", data_filter=test_apply,\
                       data_parser=kairos_parser,fetchsize=10000, datatime_batch=1000, time_refresh=100):
        db_connection = db_connection_setUp(db_connection)
        DEBUG = True
        (response, n_data_insert) =  kairos_insert_present(db_connection, kairosdb_server, data_filter, data_parser)
        assert response.status_code ==  204
        DEBUG = False

    def test_collector_datalab_period(self, db_connection=DB_SESSION, kairos_server="http://192.168.1.10:8080", data_filter=test_apply, data_parser=kairos_parser,\
                                      es_server="http://192.168.1.10", index="tester_insert_datalab_index", doc_type='default',\
                                      es_generator_data=es_basic_generator_data ,bulk_fn=parallel_bulk, fetchsize=1000):
        db_connection = db_connection_setUp(db_connection)
        es_object = es_connection_setUp(es_server)
        period = (datetime.datetime.now()-datetime.timedelta(seconds=10), datetime.datetime.now())
        (kairos_response, n_data_inserted, es_ok, es_result) = collector_datalab_period(db_connection, period, kairos_server, data_filter,\
                                                                                        data_parser, es_object, index, doc_type, es_generator_data, bulk_fn, fetchsize)
        assert kairos_response.status_code ==  204
        assert es_ok == True
    
    def test_collector_datalab_present(self, db_connection=DB_SESSION, kairos_server="http://192.168.1.10:8080", data_filter=test_apply, data_parser=kairos_parser,\
                                      es_server="http://192.168.1.10", index="tester_insert_datalab_index", doc_type='default',\
                                      es_generator_data=es_basic_generator_data ,bulk_fn=parallel_bulk, fetchsize=1000):
        
        db_connection = db_connection_setUp(db_connection)
        es_object = es_connection_setUp(es_server)
        (kairos_response, n_data_inserted, es_ok, es_result) = collector_datalab_present(db_connection, kairos_server, data_filter, data_parser, \
                                  es_object, index, doc_type, es_generator_data, bulk_fn, fetchsize)
        assert kairos_response.status_code ==  204
        assert es_ok == True
    

if __name__ == '__main__':
    import logging
    import os
    set_debug(True)
    #datalab_logger.setLevel(logging.CRITICAL) # Cambiar para que pueda eliminar el hander de elasticsearch
    unittest.main()
    set_debug(False)

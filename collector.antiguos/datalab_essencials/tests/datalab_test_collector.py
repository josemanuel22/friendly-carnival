import unittest
import sys
import os.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
sys.path.append(os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)),'datalab'))
from datalab.datalab_essencials import *

class MyTest(unittest.TestCase):
    def test_collector_datalab_present(self):
        db_connection=DB_SESSION
        kairos_server="http://192.168.1.10:8080"
        data_filter=test_apply
        data_parser=kairos_parser
        es_server="http://192.168.1.10"
        index="jose_vltlog"
        doc_type='default'
        es_generator_data=es_basic_generator_data
        db_connection = db_connection_setUp(db_connection)
        es_object = es_connection_setUp(es_server)
        (kairos_response,n_data_inserted,es_ok,es_result)=collector_datalab_present(db_connection,kairos_server,data_filter,data_parser,es_object,index,doc_type,es_generator_data)
        assert kairos_response.status_code ==  204
        assert es_ok == True
    
    def test_collector_datalab_backwards(self):
        db_connection=DB_SESSION
        kairos_server="http://192.168.1.10:8080"
        data_filter=test_apply
        data_parser=kairos_parser
        es_server="http://192.168.1.10"
        index="jose_vltlog"
        doc_type='default'
        es_generator_data=es_basic_generator_data
        db_connection = db_connection_setUp(db_connection)
        es_object = es_connection_setUp(es_server)
        (kairos_response,n_data_inserted,es_ok,es_result)=collector_datalab_backwards(db_connection,kairos_server,data_filter,data_parser,es_object,index,doc_type,es_generator_data)
        assert kairos_response.status_code ==  204
        assert es_ok == True

if __name__ == '__main__':
    import logging
    import os
    set_debug(True)
    #datalab_logger.setLevel(logging.CRITICAL) # Cambiar para que pueda eliminar el hander de elasticsearch
    unittest.main()
    set_debug(False)

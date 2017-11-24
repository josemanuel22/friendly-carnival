import datalab_essencials

db_connection = datalab_essencials.db_connection_setUp(datalab_essencials.DB_SESSION)
kairos_server="http://192.168.1.10:8080"
es_object = datalab_essencials.es_connection_setUp("http://192.168.1.10")
index="vltlog"
datalab_essencials.collector_datalab_present(db_connection, kairos_server, datalab_essencials.test_apply, datalab_essencials.kairos_parser, es_object, index)

 

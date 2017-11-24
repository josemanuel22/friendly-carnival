import logging
import sys
import os.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
from libs.cmreslogging.handlers import CMRESHandler
import coloredlogs

class datalab_logger:

    def __init__(self, my_format, name=__name__, index_es="datalab_logger",  es_host=[{'host': '134.171.189.10', 'port': 9200}]):
        datalab_format= my_format #'%(asctime)s, %(name)-12s, %(levelname)-8s, %(processName)s, %(process)d,  %(pathname)s, %(filename)s, %(module)s, %(funcName)s, %(lineno)d , %(message)s'
        logging.basicConfig(level=logging.INFO)
        self.datalab_logger = logging.getLogger(name)
        #self.datalab_logger.setLevel(logging.INFO) 
        ch = logging.StreamHandler()
        formatter = logging.Formatter(my_format)
        ch.setFormatter(formatter)
        self.datalab_logger.addHandler(ch)
        
        handler = CMRESHandler(hosts=es_host,
                               auth_type=CMRESHandler.AuthType.NO_AUTH,
                               index_name_frequency=CMRESHandler.IndexNameFrequency.NONE,
                               es_index_name=index_es)
        self.datalab_logger.addHandler(handler)

    def set_log(self, your_logger):
        """
        Set your logging propierties
    
        :param your_logger: object logger with your logging fancy propierties
        :type your_logger: logger
        """
        self.datalab_logger = your_logger

if __name__ == "__main__":
    pass;

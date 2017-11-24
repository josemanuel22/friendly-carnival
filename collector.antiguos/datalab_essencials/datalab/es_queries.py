from elasticsearch import Elasticsearch
from dateutil.parser import parse as parse_date
import datetime
import elasticsearch

def print_search_stats(results):
    print('=' * 80)
    print('Total %d found in %dms' % (results['hits']['total'], results['took']))
    print('-' * 80)

def print_hits(es, page):
    " Simple utility function to print results of a search query. "
    print_search_stats(page)

    sid = page['_scroll_id']
    scroll_size = page['hits']['total']

    while (scroll_size > 0):
       page = es.scroll(scroll_id = sid, scroll = '2m')
       # Update the scroll ID
       sid = page['_scroll_id']
       # Get the number of results that we returned in the last scroll
       scroll_size = len(page['hits']['hits'])
       if scroll_size == 0: break
       #print("scroll size: " + str(scroll_size))
       # Do something with the obtained page
       for hit in page['hits']['hits']:
           # get created date for a repo and fallback to authored_date for a commit
           created_at = parse_date(hit['_source'].get('@timestamp', hit['_source']['@timestamp']))
           s = "/%s/%s/%s (%s):\tloghost: %s\tenvname: %s\tprocname: %s\tprocid: %s\tmodule: %s\n\
                     "+33*" "+"\tkeywname: %s\t keywvalue: %s\n\
                     "+33*" "+"\tlogtext: %s\n\
                     "+42*" "+"\t %s\n"
           print(s % (
                    hit['_index'], hit['_type'], hit['_id'],
                    created_at.strftime('%Y-%m-%d %H:%m:%S'),
                    hit['_source']['loghost'].replace('\n', ' '),
                    hit['_source']['envname'].replace('\n', ' '),
                    hit['_source']['procname'].replace('\n', ' '),
                    hit['_source']['procid'],
                    hit['_source']['module'].replace('\n', ' '),
                    hit['_source']['keywname'].replace('\n', ' '),
                    hit['_source']['keywvalue'],
                    hit['_source']['logtext'].replace('\n', ' ')[:80],
                    hit['_source']['logtext'].replace('\n', ' ')[80:]))
           print()
    print('=' * 80)
    print()


def es_query(index, doc_type, logtext_contain=None, logtext_not_contain=None):
    """
    {   
        '_index'
        '_type':
        '_source':  {
            '@timestamp':
            'loghost': 
            'envname':
            'procname':
            'procid':
            'module':
            'keywname':
            'keywvalue':
            'logtext': 
        },
    }

    """
    es = Elasticsearch(['http://192.168.1.10','http://192.168.1.11','http://192.168.1.12', 'http://192.168.1.13'], maxsize=100)
    result = es.search(
        index=index,
        doc_type=doc_type,
        scroll = '2m',
        #search_type = 'scan',
        #size = 1000,
        body={
          'query': {
            'bool': {
              'must': { 'match': {'logtext': logtext_contain } },
              'must_not': { 'term': {'logtext': logtext_not_contain} }
            }
          }        
        } 
    )
    print_hits(es, result)
    return result

#Example
#es_query(index='vltlog', doc_type='log' ,logtext_contain='e', logtext_not_contain='.')

def getcollector_present_piglet(index, doc_type):
    es = Elasticsearch(['http://192.168.1.10','http://192.168.1.11','http://192.168.1.12', 'http://192.168.1.13'], maxsize=100)
    body = {
        "query": {"match_all": {}},
        "size": 1,
        "sort": [{"collector_piglet": { "order": "desc"}}]
    }
    result = es.search(index=index, doc_type=doc_type, body=body)
    timestamp = result['hits']['hits'][0]['_source']['collector_piglet'] 
    x = datetime.datetime(year=int(timestamp[:4]), month=int(timestamp[5:7]),day=int(timestamp[8:10]),hour=int(timestamp[11:13]),minute=int(timestamp[14:16]),\
                          second=int(timestamp[17:19]),microsecond=int(timestamp[20:]))  #2017-10-17T23:06:23.165623
    return x
    
#print(es_get_last_time_inserted("ollector_present_last_inserted", "python_log"))

def getcollector_backwards_piglet(index, doc_type):
    es = Elasticsearch(['http://192.168.1.10','http://192.168.1.11','http://192.168.1.12', 'http://192.168.1.13'], maxsize=100)
    body = {
        "query": {"match_all": {}},
        "size": 1,
        "sort": [{"collector_piglet": { "order": "asc"}}]
    }
    result = es.search(index=index, doc_type=doc_type, body=body)
    timestamp = result['hits']['hits'][0]['_source']['collector_piglet']
    x = datetime.datetime(year=int(timestamp[:4]), month=int(timestamp[5:7]),day=int(timestamp[8:10]),hour=int(timestamp[11:13]),minute=int(timestamp[14:16]),\
                          second=int(timestamp[17:19]),microsecond=int(timestamp[20:]))
    return x   




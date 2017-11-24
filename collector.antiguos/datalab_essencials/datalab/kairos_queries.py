import requests
import json

def kairos_query_basic(kairos_server, query):
    response = requests.post(kairos_server + "/api/v1/datapoints/query", data=json.dumps(query))
    print("Status code: %d" % response.status_code)
    print("JSON response:")
    print(response.json())
    return response

query = {
  "metrics": [
    {
      "tags": {},
      "name": "kairosdb.http.request_time",
    }
  ],
  "start_relative": {
    "value": "1",
    "unit": "hours"
  }
}

x=kairos_query_basic('http://192.168.1.10:8080', query)
print(x.json()['queries'][0]['sample_size'])


def kairos_query_get_metric_names(kairos_server):
    r = requests.get(kairos_server+'/api/v1/metricnames')
    print("Metric names:\n", r.json())
    return r
#kairos_query_get_metric_names('http://192.168.1.10:8080')

def kairos_query_list_tag_names(kairos_server):
    r = requests.get(kairos_server+'/api/v1/tagnames')
    print("tag names:\n", r.json())
    return r
#kairos_query_list_tag_names(kairos_server)

def kairos_query_list_tag_values(kairos_server):
    r = requests.get(kairos_server+'/api/v1/tagvalues')
    print("Tag values:\n", r.json())
    return r
#kairos_query_list_tag_values('http://192.168.1.10:8080')


def kairos_query_basic(kairos_server, query):
    response = requests.post(kairosdb_server + "/api/v1/datapoints/query", data=json.dumps(query))
    print("Status code: %d" % response.status_code)
    print("JSON response:") 
    print(response.json())
    return response

def kairos_query_health_status(kairosdb_server):
    response = requests.get(kairosdb_server + "/api/v1/health/status")
    print(response.json())
    print("Is everything okay with KairosDB? %s" % (response.status_code == 204)) # If all are healthy it returns status 204 otherwise it returns 500.
    return response
    
def kairos_version(kairosdv_server):
   response = requests.get(kairosdb_server + "/api/v1/version")
   print(response.json())
   return response

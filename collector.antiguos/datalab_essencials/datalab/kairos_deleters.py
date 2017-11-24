import requests
import json

def delete_metric(kairos_server, metric):
    delete_url = kairos_server+"/api/v1/metric/"+str(metric)
    r = requests.delete(delete_url)
    if r.status_code != 204:
        print('deletion of metric %s failed. Status code: %s') % (metric, r.status_code)
    return r

def delete_metrics(kairos_server, metric_names_list):
    for metric in metric_names_list:
        delete_metric(kairos_server, metric)

def delete_datapoints(kairos_server, metric_names_list, start_time, end_time=None, tags=None):
    start_time = float(start)    
    query = {
        "start_absolute" : int(start_time * 1000)
    }

    if end_time is not None:
        end = float(end_time)
        query["end_absolute"] = int(end * 1000)

    query["metrics"] = [{"name" : m } for m in metric_names_list]
    if tags:
        query['metrics'][0]['tags'] = tags
    delete_url = kairos_server+"/api/v1/datapoints/delete/"+
    return requests.post(delete_url, json.dumps(query))


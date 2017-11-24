
curl -XPUT 'localhost:9200/_all?pretty' -H 'Content-Type: application/json' -d'
{
  "mappings": {
      "_all": {
        "enabled": false 
      }
  },
}
'


curl -XPUT 'localhost:9200/joseis_opslog?pretty' -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "opslog": {
      "_all": {
        "enabled": false 
      },
      "properties": {
	"@timestamp": {
          "type": "date"
        },
        "envname": {
          "type": "text",
          "fields": {
            "keyword": {
        "type": "keyword",
        "ignore_above": 20
            }
          }
        },
        "errlocation": {
          "type": "text",
          "fields": {
            "keyword": {
        "type": "keyword",
        "ignore_above": 20
            }
          }
        },
        "errseverity": {
          "type": "text",
          "fields": {
            "keyword": {
        "type": "keyword",
        "ignore_above": 20
            }
          }
        },
        "errstack": {
          "type": "text",
          "fields": {
            "keyword": {
        "type": "keyword",
        "ignore_above": 20
            }
          }
        },
        "errstackidx": {
          "type": "long"
        },
        "keywname": {
          "type": "text",
          "fields": {
            "keyword": {
        "type": "keyword",
        "ignore_above": 100
            }
          }
        },
        "keywvalue": {
          "type": "float"
        },
        "loghost": {
          "type": "text",
          "fields": {
            "keyword": {
        "type": "keyword",
        "ignore_above": 20
            }
          }
        },
        "logtext": {
          "type": "text",
          "fields": {
            "keyword": {
        "type": "keyword",
        "ignore_above": 256
            }
          }
        },
        "module": {
          "type": "text",
          "fields": {
            "keyword": {
        "type": "keyword",
        "ignore_above": 20
            }
          }
        },
        "procid": {
          "type": "long"
        },
        "procname": {
          "type": "text",
          "fields": {
            "keyword": {
        "type": "keyword",
        "ignore_above": 20
            }
          }
        }
      }
    }
  },
  "settings": {
    "index.number_of_shards" : "4",
    "index.number_of_replicas" : "0",
    "index.query.default_field": "logtext",
    "index.write.wait_for_active_shards": "0"
  }
}
'

curl -XPUT 'localhost:9200/jose_opslog/_settings?pretty' -H 'Content-Type: application/json' -d'
{
  "refresh_interval" : "4s"
}
'


curl -XPUT 'localhost:9200/vltlog-20e16-89?pretty' -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "fevt": {
      "_all": {
        "enabled": false 
      },
      "properties": {
        "@timestamp": {
          "type": "date"
        },
        "envname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errlocation": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errseverity": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errstack": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errstackidx": {
          "type": "long"
        },
        "keywname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 100
            }
          }
        },
        "keywvalue": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 100
            }
          }
        },
        "loghost": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "logtext": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "module": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "procid": {
          "type": "long"
        },
        "procname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        } 
      }
    },
    "log": {
      "_all": {
        "enabled": false 
      },
      "properties": {
        "@timestamp": {
          "type": "date"
        },
        "envname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errlocation": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errseverity": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errstack": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errstackidx": {
          "type": "long"
        },
        "keywname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 100
            }
          }
        },
        "keywvalue": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 100
            }
          }
        },
        "loghost": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "module": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "procid": {
          "type": "long"
        },
        "procname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        }
      }
    },
   "flog": {
      "_all": {
        "enabled": false 
      },
      "properties": {
        "@timestamp": {
          "type": "date"
        },
        "envname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errlocation": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errseverity": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errstack": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errstackidx": {
          "type": "long"
        },
        "keywname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 100
            }
          }
       },
       "keywvalue": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 100
            }
          }
        },
        "loghost": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "module": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "procid": {
          "type": "long"
        },
        "procname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        }
      } 
    },
   "err": {
      "_all": {
        "enabled": false 
      },
      "properties": {
        "@timestamp": {
          "type": "date"
        },
        "envname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errlocation": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errseverity": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errstack": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errstackidx": {
          "type": "long"
        },
        "keywname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 100
            }
          }
        },
       "keywvalue": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 100
            }
          }
        },
        "loghost": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "module": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "procid": {
          "type": "long"
        },
        "procname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        }
      }
    },
   "fpar": {
      "_all": {
        "enabled": false 
      },
      "properties": {
        "@timestamp": {
          "type": "date"
        },
        "envname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errlocation": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errseverity": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errstack": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "errstackidx": {
          "type": "long"
        },
        "keywname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 100
            }
          }
        },
       "keywvalue": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 100
            }
          }
        },
        "loghost": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "module": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        },
        "procid": {
          "type": "long"
        },
        "procname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 20
            }
          }
        }
      }
    }
  },
  "settings": {
    "index.number_of_shards" : "4",
    "index.number_of_replicas" : "0",
    "index.query.default_field": "logtext",
    "index.write.wait_for_active_shards": "0"
  }
}
'

curl -XPUT 'localhost:9200/jose_vltlog/_settings?pretty' -H 'Content-Type: application/json' -d'
{
  "refresh_interval" : "5s"
}

'

# Reindex

## small index

* To create a smaller index with a subset of the documents

* to avoid timeouts, does not wait for completion



```

POST _reindex?wait_for_completion=false
{
    "source": {
        "index": "lora-index",
        "query": {
            "bool": {
                "filter": [{
                    "range":{
                        "mqtt_time":{
                            "gte": "2020-10-01",
                            "lte": "2020-10-30" 
                        }
                    }
                }]
            }
        }
    },
    "dest": {
        "index": "lora-index-short"
    }
}

```


## tasks management

* to stop possible ongoing tasks


```
GET /_tasks
GET /_tasks/Cvx16n6KSniEfkHuLnOm_g:120851
POST _tasks/Cvx16n6KSniEfkHuLnOm_g:120851/_cancel

```

# Reindex v3 to common

* delete the previous index

```
DELETE /lora-index-new
```

* force the mapping to avoid ambiguity and to change field types

````
PUT /lora-index-new
{
  "mappings": {
    "properties": {
      "devAddr": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "devEui": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "mType": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "phyPayload": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "rxInfo": {
        "properties": {
          "channel": {
            "type": "long"
          },
          "context": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "crcStatus": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "gatewayId": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "location": {
            "properties": {
              "altitude": {
                "type": "long"
              },
              "latitude": {
                "type": "float"
              },
              "longitude": {
                "type": "float"
              },
              "source": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              }
            }
          },
          "metadata": {
            "properties": {
              "region_common_name": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "region_config_id": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              }
            }
          },
          "rfChain": {
            "type": "long"
          },
          "rssi": {
            "type": "long"
          },
          "snr": {
            "type": "long"
          },
          "time": {
            "type": "date"
          },
          "uplinkId": {
            "type": "long"
          }
        }
      },
      "time": {
        "type": "date"
      },
      "txInfo": {
        "properties": {
          "frequency": {
            "type": "long"
          },
          "modulation2": {
            "properties": {
              "lora": {
                "properties": {
                  "bandwidth": {
                    "type": "long"
                  },
                  "codeRate": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "spreadingFactor": {
                    "type": "long"
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
```


* reindexing with the rewritting to the new mapping


```

POST _reindex
{
  "source": {
    "index": "lora-index",
    "query": {
      "bool": {
        "filter": [
          {
            "range": {
              "mqtt_time": {
                "gte": "2021-03-26T16:58:54.571867Z",
                "lte": "2021-03-26T16:58:54.571867Z"
              }
            }
          }
        ]
      }
    }
  },
  "dest": {
    "index": "lora-index-new"
  },
  "script": {
    "source": "ctx._source['src_version'] = 3; ctx._source['time']  = ctx._source.remove('mqtt_time');  ctx._source.txInfo.type = ctx._source.txInfo.remove('modulation'); ctx._source.txInfo.modulation = [:]; ctx._source.txInfo.modulation.lora = ctx._source.txInfo.remove('loRaModulationInfo'); ctx._source.txInfo.modulation.type  = ctx._source.txInfo.remove('type'); ctx._source.txInfo.modulation.lora.codeRate = 'C_' + ctx._source.txInfo.modulation.lora.codeRate.replace('/','_'); ctx._source.devAddr = ctx._source.extra_infos.phyPayload.macPayload.fhdr.devAddr; ctx._source.mType = ctx._source.extra_infos.phyPayload.mhdr.mType; ctx._source.rxInfo.snr = ctx._source.rxInfo.remove('loRaSNR'); ctx._source.rxInfo.gatewayId = ctx._source.rxInfo.remove('gatewayID'); ctx._source.rxInfo.uplinkIdText = ctx._source.rxInfo.remove('uplinkID');"
  }
}

```

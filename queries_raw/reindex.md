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



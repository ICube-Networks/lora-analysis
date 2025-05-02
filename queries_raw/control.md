# Progress

	
* NB de paquets sans dup_info

```
GET /lora-index/_count?pretty=true
	{
	  "query": {
	    "bool": {
	      "must_not": [
          {"exists": {"field": "dup_infos"}}
        ]        
      }
	}
}
```

* to get the number of records for which the frame has not been decoded (the records are modified through `dataset_addfields.py`)

```

	GET /lora-index/_search?pretty=true
	{
	  "size":0,
	  "query": {
	    "bool": {
	      "must_not": [
	            {
	              "exists": {
	              "field": "extra_infos"
	             }
	          }
	        ]
	    }
	  },
	  "aggs":{
	    "rxInfo":{
	      "terms":{
	        "field":"txInfo.modulation.keyword",
	        "size":1000000
	      }
	    }
	  }
	}
```


* a doc with a given payload and no dup

```
GET /lora-index/_search?pretty=true
{
    "size": 10000,
	"timeout": "3000s",
	"query": {
		"bool": {
            "filter" : [
                {"match": {"phyPayload": "gMvWowQAfwECzs5nB4lyjw=="}}
            ],
	        "must_not": [
                {"exists": {"field": "dup_infos"}}
            ]
        }
	}
}
```


# number of docs after a given date



```
	GET /lora-index-short/_search?pretty=true
	{
	  "size":0,	  
	  "query": {
            "bool": {
                "filter": [{
                    "range":{
                        "mqtt_time":{
                            "gte": "2020-11-01"
                        }
                    }
                }]
            }
        },
        "aggs":{
	    "rxInfo":{
	      "terms":{
	        "field":"txInfo.modulation.keyword",
	        "size":100000000
	      }
	    }
	  }
	}

```


# Parameters

* to change the default timeout value

```

PUT _cluster/settings
{
  "persistent": {
    "search.default_search_timeout": "60s"
  }
}
```

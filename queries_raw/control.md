# Progress

to get the number of records for which the frame has not been decoded (the records are modified through `dataset_addfields.py`)

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

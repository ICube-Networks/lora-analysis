# Progress

to get the number of records for which the frame has not been decoded (the records are modified through `dataset_addfields.py`)


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




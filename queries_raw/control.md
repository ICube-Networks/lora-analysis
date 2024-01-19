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


# clone an index

to clone lora-index into lora-index-short (working copy)

	POST _reindex
	{
	  "source": {
	    "index": "lora-index"
	  },
	  "dest": {
	    "index": "lora-index-short"
	  }
	}



# search for an entry


To get an entry with its id


	GET /lora-index/_search?pretty=true
	{
	  "size": 10,
	  "timeout": "3000s",
	  "query": {
		    "bool": {
		       "must": [
		         {
	            "term": {
	                    "_id": "X7jrIHUBJ8aGN70ZB0tO"
	                }
		         }
	          ]
		    }
		  }
	}
	
	
	
To get an entry with its Phy payload

	

	GET /lora-index-short/_search?pretty=true
		{
		  "size": 10,
		  "timeout": "3000s",
		  "query": {
			    "bool": {
			       "must": [
			         {
		            "term": {
		                    "phyPayload.keyword": "QPXn2g6A4LUCf+qViIUpWAM4gQYUZvK82tsc"
		                }
			         }
		          ]
			    }
			  }
		}

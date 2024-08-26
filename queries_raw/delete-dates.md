# Get / del the packets for a period 


	GET /lora-index/_search?pretty=true
	{
	  "size": 0,
	  "query": {
	    "bool": {
	      "filter": [
	        {
	          "range":{
	            "mqtt_time":{
	                 "gte": "2020-11-01",
	                 "lte": "2022-11-02"            
	
	            }
	          }
	        }
	      ]
	    }
	  }
	 }
 
 
 Be careful: limit the nb of docs to delete simultaneously (15 days = 15,000 docs are ok)
  
	POST /lora-index/_delete_by_query
	{
	  "query": {
	    "bool": {
	      "filter": [
	        {
	          "range":{
	            "mqtt_time":{
	                 "gte": "2022-05-01",
	                 "lte": "2022-06-02"  
	            }
	          }
	        }
	      ]
	    }
	  }
	 }
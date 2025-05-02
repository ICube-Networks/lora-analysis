

# search : id or payload


To get an entry with its id

```

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
	
```
	
	
To get an entry with its Phy payload

	
```

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
```




# phypayload + mqtt_time min

```

GET /lora-index/_search?pretty=true
{
  "query": {
    "bool" : {
      "filter" : [
        {
          "match": {
            "phyPayload": "+0EPOH56oFbkkTeqvTKO6MVUrYoZ"
          
          }
        }
        ]      ,
      "must": [
          {
          "range": {
            "mqtt_time": {
              "gte": "2020-10-10T09:40:19.485088Z"
            }
          }
        }
      ]
    }
	}
}
```


# between two dates


```	
	GET /lora-index/_search?pretty=true
	{
	  "size": 10000,
	  "query": {
	    "bool": {
	      "filter": [
	        {
	          "range":{
	            "mqtt_time":{
	                 "gte": "2022-03-31",
	                 "lte": "2022-04-30"            
	
	            }
	          }
	        }
	      ]
	    }
	  }
	 }
```

	 
# a PhyPayload without dup_info

```
GET /lora-index/_search?pretty=true
	{
	 "size": 2000,
	  "query": {
	    "bool": {
	      "must_not": [
          {"exists": {"field": "dup_infos"}}
        ],
        "filter": [ 
          { "match":  { "phyPayload": "+0EPOH56oFbkkTeqvTKO6MVUrYoZ"}}
        ]
      }
	  },
      "sort" : [
        { "mqtt_time" : "asc" }
      ]
	}
	
```
	
	 
# a PhyPayload without extra_infos

```
GET /lora-index/_search?pretty=true
	{
	 "size": 2000,
	 "timeout": "100s",
	  "query": {
	    "bool": {
	      "must_not": [
          {"exists": {"field": "extra_infos"}}
        ]
      }
	  },
      "sort" : [
        { "mqtt_time" : "asc" }
      ]
	}
	
```
	

	

# complex query

```

GET /lora-index/_search?pretty=true
	{
	 "size": 2000,
	  "query": {
	    "bool": {
	      "must": [
          {"exists": {"field": "extra_infos"}}
        ],
        "filter": [ 
          { "match":  { "dup_infos.is_duplicate": false }},
          { "match":  { "extra_infos.phyPayload.macPayload.fhdr.devAddr": "000173b7" }}
        ]
      }
	  },
	   "fields": [
          "extra_infos.phyPayload.macPayload.fhdr.devAddr",
          "phyPayload",
          "mqtt_time",
          "extra_infos.phyPayload.macPayload.fhdr.fCnt"
      ],
      "_source": false,
      "sort" : [
        { "mqtt_time" : "asc" }
      ]
	}
```
	
	
# comptage

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
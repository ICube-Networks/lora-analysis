

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



# no-dup crawling


* one phyPayload without dup_info

```
GET /lora-index/_search?pretty=true
{
	"size": 1,
    "query": {
        "bool": {
            "must_not" : [{
                "range": {
                    "dup_infos.version": {
                        "gte": "1.0"
                    }
                }
            }]
        }
    },
    "fields" :[
        "phyPayload"
    ],
    
}
```

* all the packets with this phyPayload

```
GET /lora-index/_search?pretty=true
{
    "size": 10000,
	"timeout": "3000s",
	"query": {
		"bool": {
            "filter" : [{
                "match": {"phyPayload": "AQM3gJ0bNzcAEkEQCAAA"}
            }] 
        }
	}
}
```




# phypayload + time min

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
            "time": {
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
	            "time":{
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
GET /lora-index/_search?pretty=true&_source=false
{
	"size": 1,
    "query": {
        "bool": {
            "must_not" : [
                {"range": { "dup_infos.version": { "gte": "1.0" } } }
           ]
        }
    },
    "fields" :[
        "phyPayload"
    ],
    "sort":[
        "phyPayload.keyword"
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
        { "time" : "asc" }
      ]
	}
	
```

* count the nb of packets without an extra_infos field
```
GET /lora-v4/_count?pretty=true
	{
	"query": {
    "exists": {
      "field": "extra_infos"
    }
	}
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
          "time",
          "extra_infos.phyPayload.macPayload.fhdr.fCnt"
      ],
      "_source": false,
      "sort" : [
        { "time" : "asc" }
      ]
	}
	
```
	
	
# Phypayload and date

```
GET /lora-index/_search?pretty=true
{
    "size": 10000,
	"timeout": "3000s",
	"query": {		
		"bool": {
            "must" : [
                {"match": {"phyPayload": "ALSSkTUnCNIMly0D/v9YF6jdJmlLtPg"}},
				{"range":{
	            	"time":{
	                	"gte": "2021-03-25T20:48:13.590911Z"
	            	}
				}}
            ]            
        }		
	}
}
```

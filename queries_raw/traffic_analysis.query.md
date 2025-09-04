# Mappings

## days of the week

	PUT lora-index/_mapping
	{
	  "runtime": {
	    "day_of_week": {
	      "type": "keyword",
	      "script": {
	        "source": "emit(doc['time'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ROOT))"
	      }
	    }
	  },
	  "properties": {
	    "time": {
	      "type": "date"
	    }
	  }
	}


## hour of the day

	
	PUT lora-index/_mapping
	{
	  "runtime": {
	    "hour": {
	      "type": "keyword",
	      "script": {
	        "source": "Date date = new Date(doc['time'].value) ; java.text.SimpleDateFormat format = new java.text.SimpleDateFormat('HH'); emit(format.format(date))  "
	      }
	    }
	  },
	  "properties": {
	    "time": {
	      "type": "date"
	    }
	  }
	}

  
# Queries


# time histogram of the traffic per hour of the day

Defined with a dynamic mapping, directly in the query

	GET /lora-index/_search?pretty=true
	{
	  "size": 0,
	  "query": {
	    "bool": {
	      "filter": [
	        {"match": {"rxInfo.crcStatus": "CRC_OK"}},
	        {
	          "range":{
	            "time":{
	                 "gte": "2020-11-01",
	                 "lte": "2020-11-02"            
	              
	            }
	          }
	        }
	      ]
	    }
	  },
	  "runtime_mappings": {
	    "hour": {
	      "type": "long",
	      "script": {
	        "source": """
	         emit(doc['time'].value.getHour());
	         """
	      }
	    }
	  },
	  "aggs": {
	    "hour": {
	      "histogram": {
	        "field": "hour",
	        "interval": "1"
	      }
	    }
	  }
	}



# time histogram of the traffic per day of the week

	GET /lora-index/_search?pretty=true
	{
	  "size": 0,
	  "query": {
	    "bool": {
	      "filter": [
	        {"match": {"rxInfo.crcStatus": "CRC_OK"}},
	        {
	          "range":{
	            "time":{
	                 "gte": "2020-09-01",
	                 "lte": "2020-10-30",
	                 "format": "year_month_day"
	            }
	          }
	        }
	      ]
	    }
	  },
	  "aggs": {
	    "day_of_week": {
	      "terms": {
	        "field": "day_of_week"
	      }
	    }
	  }
	}


# time histogram of the traffic per morning/afternoon


	GET /lora-index/_search?pretty=true
	{
	  "size": 0,
	  "query": {
	    "bool": {
	      "filter": [
	        {"match": {"rxInfo.crcStatus": "CRC_OK"}},
	        {
	          "range":{
	            "time":{
	                 "gte": "2020-09-01",
	                 "lte": "2020-10-30",
	                 "format": "year_month_day"
	            }
	          }
	        }
	      ]
	    }
	  },
	  "aggs": {
	    "am-pm-count": {
	      "terms": {
	        "script": "return doc[\"time\"].value.getHour() < 12 ? \"AM\" : \"PM\";"
	      }
	    }
	  }
	}

# time histogram of the traffic per day/night

The day is the period comprised between 7am and 7pm


	GET /lora-index/_search?pretty=true
	{
	  "size": 0,
	  "query": {
	    "bool": {
	      "filter": [
	        {"match": {"rxInfo.crcStatus": "CRC_OK"}},
	        {
	          "range":{
	            "time":{
	                 "gte": "2020-09-01",
	                 "lte": "2020-10-30",
	                 "format": "year_month_day"
	            }
	          }
	        }
	      ]
	    }
	  },
	  "aggs": {
	    "day-night-count": {
	      "terms": {
	        "script": "return doc[\"time\"].value.getHour() <= 7 || doc[\"time\"].value.getHour() >= 19  ? \"night\" : \"day\";"
	      }
	    }
	  }
	}


# number of mtypes per devADDR

	GET /lora-index/_search?pretty=true
	{
	"size":0,
	"query": {
	    "bool": {
	      "must_": [
	            {
	              "exists": {
	              "field": "extra_infos.phyPayload.macPayload.fhdr.devAddr"
	             }
	          }
	        ]
	    }
	  },
	  "aggs":{
	    "mtype":{
	      "terms":{
	        "field":"extra_infos.phyPayload.mhdr.mType",
	        "size":1000000
	      }
	    }
	  }
	}
	
	
# Distributions


List of first reception of a packet for each gateway ID

	GET /lora-index/_search?pretty=true
	{
	  "size":0,
	  "timeout": "3000s",
	  "query": {
		    "bool": {
		      "must": [
		            {
		              "exists": {
		              "field": "extra_infos"
		             }
		          }
		        ]
		    }
		  },
	  "aggs":{
	    "devAddr": {
	      "terms": {
	        "field": "rxInfo.gatewayID.keyword",
	        "size": 10000,
	        "order": {"min_time": "desc"}
	      },
	      "aggs":{
	        "min_time": {"min": {"field": "time"}}
	      }
	    }
	  }
	}
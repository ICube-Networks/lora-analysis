# elastic search for the queries
from elasticsearch import Elasticsearch

# format
import requests, json, os, tarfile, pathlib
from datetime import datetime

# configuration parameters
import myconfig

# numerical libraries
import pandas as pd
import numpy as np


#number of results for a query
QUERY_NB_RESULT=10000


#elastic connection
clientES = Elasticsearch(
    "https://localhost:9200",
    verify_certs=False,
    ssl_show_warn=False,
    basic_auth=(myconfig.user, myconfig.password)
)

#create a PIT to fix the index for the search query
result = clientES.open_point_in_time(index=myconfig.index_name, keep_alive="1m")
pit_id = result['id']
print("ID of the elastic search connection for the query: ", pit_id)



    

# list of clients with specific SF
numRecords = pd.Series()
    
resp = clientES.search(
    size=0,
    body={
        "query":{
            "bool": {
                "filter": [
                    {"match": {"rxInfo.crcStatus": "CRC_OK"}},
                ],
            },
        },
        "aggregations": {
            "SF": {
                "terms" : { "field" : "txInfo.loRaModulationInfo.spreadingFactor" },
                "aggregations": {
                    "channels": { "terms" : { "field" : "rxInfo.channel" }},
                },
            },
        },
     }
)
    
#"dates" : A("date_histogram", field="date", interval="1M", time_zone="Europe/Berlin"),
   
          
          
# get source data from document
print(resp["aggregations"]["SF"])


#for elem in source_data["SF"]["buckets"]:
#    print(elem)

print("------------")
print("------------")



# iterate source data (use iteritems() for Python 2)
fields = {}
for key in resp["aggregations"]["SF"]["buckets"]:
    print(key)
    if False:
        try:
            print(key, "///", val)
            fields[key] = np.append(fields[key], val)
        except KeyError:
            print(key, "//", val)
            fields[key] = np.array([val])

    print("       ")

print("------------")
print(fields)

#delete the PIT
clientES.close_point_in_time(id=pit_id)


    

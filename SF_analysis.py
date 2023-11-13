from datetime import datetime
from elasticsearch import Elasticsearch
import requests, json, os, tarfile, pathlib
import myconfig
import pandas as pd

from elasticsearch_dsl import Search

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



if True:

    # list of clients with specific SF
    numRecords = pd.Series()
    for SF in range(7,12):

        #initialization with an empty value for this SF
        if SF not in numRecords.index:
            numRecords = pd.concat([numRecords, pd.Series(data=[0], index=[SF])])

        # initially, the first date in the range is nothing (=0)
        datemin="0"
        while True:
            
            resp = clientES.search(
                size=QUERY_NB_RESULT,
                query={
                    "bool": {
                        "filter": [
                            {"match": {"rxInfo.crcStatus": "CRC_OK"}},
                            {"term": {"txInfo.loRaModulationInfo.spreadingFactor": SF}},
                        ],
                    },
                },
                pit={
                    "id": pit_id,
                    "keep_alive": "1m",
                },
                sort=[
                    {"mqtt_time": {"order": "asc"}},
                    {"_score": {"order": "desc"}},
                ],
                search_after=[
                    datemin,
                    0
                ],
            )
            
            #metadata of the response
            length = len(resp['hits']['hits'])
            numRecords[SF] = numRecords[SF] + length
            #print("Got %d Hits:" % length)
             
            #stops if we have less than QUERY_SIZE elements
            if (length < QUERY_NB_RESULT):
                break

            #extracts the mqtt-time of the last element for the next query
            datemin = resp['hits']['hits'][length-1]['_source']['mqtt_time']
             

    #final results
    print(numRecords)

    

    
    
    
if True:

    # list of clients with specific SF
    numRecords = pd.Series()

    # initially, the first date in the range is nothing (=0)
    datemin="0"

            
    resp = clientES.search(
        size=0,
        query={
            "bool": {
                "filter": [
                    {"match": {"rxInfo.crcStatus": "CRC_OK"}} #,
                  #  {"term": {"txInfo.loRaModulationInfo.spreadingFactor": SF}},
                ],
            },
        },
        aggs={
            "toto": { "terms" : { "field" : "txInfo.loRaModulationInfo.spreadingFactor" }},
            
        },
        pit={
            "id": pit_id,
            "keep_alive": "1m",
        },
        sort=[
            {"mqtt_time": {"order": "asc"}},
            {"_score": {"order": "desc"}},
        ],
        search_after=[
            datemin,
            0
        ],
    )
    
    print(resp)
            
          
          

    
 



#delete the PIT
clientES.close_point_in_time(id=pit_id)


    

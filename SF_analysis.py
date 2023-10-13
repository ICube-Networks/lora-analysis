from datetime import datetime
from elasticsearch import Elasticsearch
import requests, json, os, tarfile, pathlib
import myconfig

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
print(pit_id)


# list of clients with SF=12
datemin="0"
i=0
while True:
    
    resp = clientES.search(
        size=QUERY_NB_RESULT,
        query={
            "bool": {
                "filter": [
                    {"match": {"rxInfo.crcStatus": "CRC_OK"}},
                    {"term": {"txInfo.loRaModulationInfo.spreadingFactor": "12"}},
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
    print("Got %d Hits:" % resp['hits']['total']['value'])
    length = len(resp['hits']['hits'])

   
    #extracts the mqtt-time of the last element
    datemin = resp['hits']['hits'][length-1]['_source']['mqtt_time']
    print(datemin, "", i)
    i=i+length
    
    #stops if we have less than QUERY_SIZE elements
    if (length < QUERY_NB_RESULT):
        break
   

    
    #for hit in resp['hits']['hits']:
    #    print(hit['_source']['mqtt_time'])
    #    if (hit['_source']['mqtt_time'] > datemin):
    #        datemin = hit['_source']['mqtt_time']




#delete the PIT
clientES.close_point_in_time(id=pit_id)


    

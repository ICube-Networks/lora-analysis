# import the config folder
import sys
sys.path.insert(1, '../config')
sys.path.insert(1, '../analysis')

# elastic search for the queries
from elasticsearch import Elasticsearch

# configuration parameters
import myconfig

# dissector of LoRaWAN frames
import loradissector

# my tool functions in common for the analysis
import tools

# numerical libraries
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# format
import requests, json, os, tarfile, pathlib
from datetime import datetime
import matplotlib.dates as mdates

# Import seaborn
import seaborn as sns


QUERY_NB_RESULT = 10000



############################################################
#           CONNECTION TO ES SERVER
############################################################



#elastic connection
DEBUG_ES = False
clientES = Elasticsearch(
    "https://localhost:9200",
    verify_certs=False,
    ssl_show_warn=False,
)
print(clientES)




############################################################
#          PIT CONNECTION
############################################################

result = clientES.options(
            basic_auth=(myconfig.user, myconfig.password)
        ).open_point_in_time(
            index=myconfig.index_name,
            keep_alive="1m"
        )
pit_id = result['id']
print("PID id: ", pit_id)



# Scroll all the documents of the elastic search index, using the PIT to scroll until the end
datemin="0"
while True:
    response = clientES.options(
        basic_auth=(myconfig.user, myconfig.password),
    ).search(
        #index=myconfig.index_name,
        size=QUERY_NB_RESULT,
        #request_timeout=300,
        sort=[
                {"mqtt_time": {"order": "asc"}},
                {"_score": {"order": "desc"}},
        ],
        pit={
            "id": pit_id,
            "keep_alive": "1m",
        },
        search_after=[
                datemin,
                0
        ],
    )
    #print("Got %d Hits:" % response['hits']['total']['value'])

    # one update per doc
    for num, doc in enumerate(response['hits']['hits']):
          
        #print("---********--")

        #decode the LoRaWAN frame
        #print("payload=", doc['_source']['phyPayload'])
        extra_infos = loradissector.process_phypayload(doc['_source']['phyPayload'])
        rich_info= {}
        rich_info['version'] = 0.6
        rich_info['extra'] = extra_infos
        print(json.dumps(rich_info, sort_keys=True, indent=4))



        #response = elastic_client.update(
        #    index=myconfig.index_name,
        #    doc_type=""_doc"",
        #    id=doc_id['_id'],
        #    body=source_to_update
        #)


    #stops if we have less than QUERY_SIZE elements, it was the last response
    length= len(response['hits']['hits'])
    #extracts the mqtt-time of the last element to then scroll later
    datemin = response['hits']['hits'][length-1]['_source']['mqtt_time']
    if (length < QUERY_NB_RESULT):
        break

#delete the PIT
clientES.close_point_in_time(id=pit_id)

#no error
exit(0)




print("------------")



for i in range(4):
    print("------------")
    print(docs[i])
    print("****")
    loradissector.process_phypayload(docs[i]['_source']['phyPayload'])
print("------------")



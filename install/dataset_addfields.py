# import the config folder
import sys
sys.path.insert(1, '../config')
sys.path.insert(1, '../analysis')

# elastic search for the queries
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

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

#logs
import logging
LOGGER = logging.getLogger('dataset_decodeFrames')
logging.basicConfig(stream=sys.stdout, level=logging.INFO)



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
#    http_auth=(myconfig.user, myconfig.password)
)
print(clientES)

clientES_bulk = Elasticsearch(
    "https://localhost:9200",
    verify_certs=False,
    ssl_show_warn=False,
    http_auth=(myconfig.user, myconfig.password)
)
print(clientES_bulk)


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

    # reinit the next bulk update query
    bulk_update = []

    # one update per doc
    for num, doc in enumerate(response['hits']['hits']):
                
        # has this record already extra info with the right info?
        try:
            assert(doc['_source']['extra_infos']['version'] == loradissector.VERSION)
            
        except (KeyError, AssertionError) as e:
        
            #we MUST have phyPayload
            if not doc.__contains__('_source') or not doc['_source'].__contains__('phyPayload'):
                LOGGER.error("** Decoding the Loraframe: The doc has no phyPayload")
                LOGGER.error(json.dumps(doc, sort_keys=True, indent=4))
                print(doc.__contains__('_source'))
                print(doc['_source'].__contains__('phyPayload'))
                exit(2)
                            
            #construct the nex update for this id (decoding the LoRa frame)
            req_update = {}
            req_update['_index']    = myconfig.index_name
            req_update['_id']       = doc['_id']
            req_update['doc'] = {}          # all the info has to be put in the 'doc' key
            req_update['doc']['extra_infos'] = loradissector.process_phypayload(doc['_source']['phyPayload'])
            LOGGER.debug(json.dumps(req_update['doc']['extra_infos'], sort_keys=True, indent=4))
            
            # insert this update to the current sequence
            bulk_update.append(req_update)
            LOGGER.debug(bulk_update)
            
    for okay, result in streaming_bulk(client=clientES_bulk, actions=bulk_update):
        action, result = result.popitem()
    
        if not okay:
            LOGGER.error("Update failed: ", result["_id"])

   
    #stops if we have less than QUERY_SIZE elements, it was the last response
    length = len(response['hits']['hits'])
    #extracts the mqtt-time of the last element to then scroll later
    datemin = response['hits']['hits'][length-1]['_source']['mqtt_time']
    if (length < QUERY_NB_RESULT):
        break

#delete the PIT
clientES.close_point_in_time(id=pit_id)




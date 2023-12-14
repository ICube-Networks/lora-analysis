# import the config folder
import sys
sys.path.insert(1, '../config')
sys.path.insert(1, '../analysis')

# elastic search for the queries
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from elasticsearch.helpers import parallel_bulk
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

logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)


QUERY_NB_RESULT = 1000
EXTRA_INFO_VERSION = "1.0"




############################################################
#           CONNECTION TO ES SERVER with PIT
############################################################



#elastic connection
DEBUG_ES = False
clientES = Elasticsearch(
    "https://localhost:9200",
    verify_certs=False,
    ssl_show_warn=False,
    basic_auth=(myconfig.user, myconfig.password)
)
print(clientES)





############################################################
#   SEARCH for records without extra_infos to update them
############################################################





# Scroll all the documents of the elastic search index, using the PIT to scroll until the end
datemin="0"
while True:
    #search records without the right extra info version
    response = clientES.options(
        basic_auth=(myconfig.user, myconfig.password),
    ).search(
        index=myconfig.index_name, #no index for PIT connections
        size=QUERY_NB_RESULT,
        query={
            "bool": {
                "must_not": {
                    "term" :{
                        "extra_infos.version": EXTRA_INFO_VERSION
                    }
                }
            }
        },
        #sort them chronologically (just because it's convenient for debuging)
        sort=[
                {"mqtt_time": {"order": "asc"}},
                {"_score": {"order": "desc"}},
        ]
    )
    #extracts the mqtt-time of the last element to then scroll later
    length = len(response['hits']['hits'])
    #print("length:", length)
    if (length == 0):
        break
    
    
    # reinit the next bulk update query
    bulk_update = []

    # one update per doc
    for num, doc in enumerate(response['hits']['hits']):
                
        # has this record already extra info with the right info?
        try:
            assert(doc['_source']['extra_infos']['version'] == loradissector.VERSION)
            
        except (KeyError, AssertionError) as e:
            #LOGGER.info("** Decoding the Loraframe: The doc has no phyPayload")
            #LOGGER.info(json.dumps(doc, sort_keys=True, indent=4))

            #we MUST have phyPayload
            if not doc.__contains__('_source') or not doc['_source'].__contains__('phyPayload'):
                LOGGER.error("** Decoding the Loraframe: The doc has no phyPayload")
                LOGGER.error(json.dumps(doc, sort_keys=True, indent=4))
                print(doc.__contains__('_source'))
                print(doc['_source'].__contains__('phyPayload'))
                exit(2)
              
            #construct the nex update for this id (decoding the LoRa frame)
            req_update = doc['_source']
            req_update['_index']         = myconfig.index_name
            req_update['_id']            = doc['_id']
            req_update['extra_infos']   = loradissector.process_phypayload(doc['_source']['phyPayload'])         # use the previous doc
            #LOGGER.debug(json.dumps(req_update, sort_keys=True, indent=4))
              
            # insert this update to the current sequence
            bulk_update.append(req_update)
            LOGGER.debug(bulk_update)
            
    #push the update
    #for okay, result in streaming_bulk(client=clientES_bulk, actions=bulk_update):
    for okay, result in parallel_bulk(client=clientES, actions=bulk_update, chunk_size=10000, thread_count=4):
        action, result = result.popitem()
        
        #print("action: ", action)
        #print("result: ", result)

        if not okay:
            LOGGER.error("Update failed: ", result["_id"])
            
        
    #stops if we have less than QUERY_SIZE elements, it was the last response
    if (length < QUERY_NB_RESULT):
        break





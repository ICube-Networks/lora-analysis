#!/usr/bin/env python3

""" Enrich the dataset with extrainfos fields .

This scripts parses the dataset to identify records (aka docs) that do not have extrainfo fields to update them.
More precisely, it decodes the LoRaWAN frames to construct the extra_info.

"""

__authors__ = ("Fabrice Theoleyre")
__contact__ = ("fabrice.theoleyre@cnrs.fr")
__copyright__ = "CNRS"
__date__ = "2023"
__version__= "1.0"






# import the config folder
import sys
sys.path.insert(1, '../config')
sys.path.insert(1, '../tools')

# elastic search for the queries
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from elasticsearch.helpers import parallel_bulk
# configuration parameters
import myconfig

# dissector of LoRaWAN frames
import lorawan_dissector

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

#logs
import logging
LOGGER = logging.getLogger('dataset_decodeFrames')
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)


#parameters
EXTRA_INFO_VERSION = "1.0"







############################################################
#   SEARCH for records without extra_infos to update them
############################################################



# executable
if __name__ == "__main__":
    """Executes the script to plot the histogram of the number of packets per SF
 
    """
     

    ############################################################
    #           CONNECTION TO ES SERVER
    ############################################################



    #elastic connection
    DEBUG_ES = False
    clientES = tools.elasticsearch_open_connection()

    # Scroll all the documents of the elastic search index
    datemin="0"
    LOGGER.info("Start scrolling the records")
    while True:
        #search records without the right extra info version
        response = clientES.options(
            basic_auth=(myconfig.user, myconfig.password),
        ).search(
            index=myconfig.index_name,
            size=tools.queries.QUERY_NB_RESULT,
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

        # num of records
        length = len(response['hits']['hits'])
        #print("length:", length)
        if (length == 0):
            LOGGER.info("No remaining entry without the right extra_infos field (version=" + EXTRA_INFO_VERSION + ")")
            break
        
        #extracts the mqtt-time of the last element to then scroll later
        last_record = datetime.strptime(response['hits']['hits'][length-1]['_source']['mqtt_time'], tools.time.DATE_FORMAT_ELASTICSEARCH)
        LOGGER.info("       > " + str(last_record))

        # reinit the next bulk update query
        bulk_update = []

        # one update per doc
        for num, doc in enumerate(response['hits']['hits']):
                    
            # has this record already extra info with the right info?
            try:
                assert(doc['_source']['extra_infos']['version'] == lorawan_dissector.VERSION)
                
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
                req_update['extra_infos']   = lorawan_dissector.process_phypayload(doc['_source']['phyPayload'])         # use the previous doc
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
        if (length < tools.queries.QUERY_NB_RESULT):
            LOGGER.info("No remaining entry without the right extra_infos field (version=" + EXTRA_INFO_VERSION + ")")
            LOGGER.info("Last bulk contained " + str(length) + " entries")
            break


clientES.transport.close()
exit(0)

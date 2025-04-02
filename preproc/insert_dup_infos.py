#!/usr/bin/env python3

""" Enrich the dataset with dup_infos fields .

This scripts parses the dataset to identify duplicates
Same Phypayload with less than 2 hours of diff are considered as duplicates
They may have been received through different gateways.

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

# numerical libraries
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# configuration parameters
import myconfig

# my tool functions in common for the analysis
import tools

# format
import requests, json, os, tarfile, pathlib
from datetime import datetime, timedelta
import flask

#logs
import logging
LOGGER = logging.getLogger('dataset_flag_duplicates')
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)



# parameters
DUP_INFO_VERSION = "1.0"
OFFSET_MINUTES_MAX = 60    # max offset to search for duplicates (length of the time window), number of minutes



############################################################
#           CONNECTION TO ES SERVER
############################################################


def fixMicroseconds(timestamp):
    """
    The microseconds field must be zero padded if required
    Format: 2020-10-05T19:08:35.251262Z
    
    """
    parts = timestamp.split('.')
    last_parts = parts[-1].split('Z')

    # replace the last elemen with padded zeros, and append the 'Z'
    return '.'.join(
        parts[:-1] + ['{:06d}'.format(int(last_parts[0]))]
    )+'Z'
   
   
   
def create_updated_entries(response):
    """
    Search if the same phyPayload exists to mark other packets as duplicates
 
    """
    # initialize the next bulk update query
    bulk_update = []
    
    #print(response)
    
    
    found = False   # used only for index=1, else, it is handled in the tests
    previous_id = 0
    for index in range(len(response)):
    
        if index >= 1:
            # Same info (NB: the recorded are sorted by <phyPayload, mqtt_time>
            # Thus duplicates are contiguous
            if (response[index-1]['_source']['phyPayload'] == response[index]['_source']['phyPayload']) and  (response[index-1]['_source']['txInfo']['loRaModulationInfo']['spreadingFactor'] == response[index]['_source']['txInfo']['loRaModulationInfo']['spreadingFactor']) and (response[index-1]['_source']['txInfo']['loRaModulationInfo']['bandwidth'] == response[index]['_source']['txInfo']['loRaModulationInfo']['bandwidth']) and (response[index-1]['_source']['txInfo']['loRaModulationInfo']['codeRate'] == response[index]['_source']['txInfo']['loRaModulationInfo']['codeRate']) and (response[index-1]['_source']['txInfo']['frequency'] == response[index]['_source']['txInfo']['frequency']):
            
                # compute the time difference between this record and the previous one
                try:
                    diff_time = datetime.strptime(fixMicroseconds(response[index]['_source']['mqtt_time']), tools.time.DATE_FORMAT_ELASTICSEARCH) - datetime.strptime(fixMicroseconds(response[index-1]['_source']['mqtt_time']), tools.time.DATE_FORMAT_ELASTICSEARCH)
                    
                except Exception as e :
                    LOGGER.critical("An error occured when parsing the response -> "+ str(e))
                    LOGGER.critical("response:")
                    LOGGER.critical(json.dumps(response, sort_keys=True, indent=4))
                    LOGGER.critical("index: " + str(index) + " id="+ response[index]['_id'])
                    exit(7)

                # this is a duplicate only if time diff acceptable + same PHY info
                if (diff_time <= timedelta(minutes = OFFSET_MINUTES_MAX)):
                    found = True
                else:
                    found = False
            # PHY info differ -> not a duplicate
            else:
                found = False

        # info on duplicates
        dup_infos = {}
        dup_infos['version']        = DUP_INFO_VERSION
        dup_infos['is_duplicate']   = found
        if (index == 0 or found is False) :
            dup_infos['copy_of']                = response[index]['_id']
        else:
            dup_infos['copy_of']                = previous_id
        previous_id = dup_infos['copy_of']
        
        
        #construct the nex update for this id
        req_update = {}
        req_update = response[index]['_source']
        req_update['_index']       = myconfig.index_name
        req_update['_id']          = response[index]['_id']
        req_update['dup_infos']    = dup_infos
        LOGGER.debug(json.dumps(req_update, sort_keys=True, indent=4))
        LOGGER.debug(req_update['_id'])
        
        # insert this update to the current sequence
        bulk_update.append(req_update)
    
    return(bulk_update)




def get_first_phyPayload():
    """
    Returns the smallest (alphabetically) phyPayload in the dataset that has no du_info field
    We do not make a distinction between records with and without dupinfo field
 
    """
    
    clientES = tools.elasticsearch_open_connection()
 
    response = clientES.search(
        pit={
            "id": pit_id,
            "keep_alive": "10m",
        },
        query={
            "bool": {
                "must_not" : [{
                    "range": {
                        "dup_infos.version": {
                            "gte": DUP_INFO_VERSION
                        }
                    }
                }]
            }
        },
        size=1,
        #sort them chronologically (just because it's convenient for debuging)
        sort=["phyPayload.keyword"]
    )
    
    clientES.transport.close()

    # no response!
    if response['hits']['total']['value'] == 0:
        return(None, None)
    
    return(response['hits']['hits'][0]['_source']['phyPayload'], response['hits']['hits'][0]['_source']['mqtt_time'])




############################################################
#   SEARCH for records without dup_infos to update them
############################################################


# executable
if __name__ == "__main__":
    """
    Executes the script to pinsert duplicate info in the elastic search dataset
 
    """

    #connections
    clientES = tools.elasticsearch_open_connection()
    pit_id = tools.elasticsearch_create_pit(clientES)
    
    
    # retrieve the earliest entry not handled
    phyPayload_min, mqtt_time_min = get_first_phyPayload()
    if phyPayload_min is None:
        LOGGER.info("The dataset does not contain any phyPayload without a dup_info field (version="+ DUP_INFO_VERSION +")")
        exit(0)
    
    #LOGGER.info("Start scrolling the records from the payload " + phyPayload_min + " with mqtt_time " + mqtt_time_min)
    
    
    # Scroll now all the documents of the elastic search index until no remainnig doc to handle
    while True:
        LOGGER.info("\t> phyPayload_min=" + phyPayload_min)

        # Search and Sort the entries chronologically (MUST include the records already handled
        # Else impossible to detect duplicates between those handled and those not handled)
        response = clientES.search(
            index=myconfig.index_name,
            size=tools.queries.QUERY_NB_RESULT,
            query={
            "bool": {
                "must" : [{
                    "range": {
                        "phyPayload.keyword": {
                            "gte": phyPayload_min
                        }
                    }
                }]
            }
            },
            pretty=True,
            human=True,
            #sort them by payload and chronologically
            sort=["phyPayload.keyword", "mqtt_time"],
        )
        
        #no remaining response
        length = len(response['hits']['hits'])
        if (length == 0):
            break
            
         
        # add the is_duplicate field to each entry of this response
        bulk_update = create_updated_entries(response['hits']['hits'])
        
        # what is the last record?
        phyPayload_last = response['hits']['hits'][length-1]['_source']['phyPayload']
        
        #push the update
        if len(bulk_update) > 0:
            LOGGER.info("\tPush the update to the server ("+ str(len(bulk_update))+" records)")
            tools.elasticsearch_push_updates(bulk_update)
            LOGGER.info("\t... pushed")
        else:
            LOGGER.info("\tNo update in this window (" + phyPayload_min + "/" + phyPayload_last + ")")
          
        phyPayload_min = phyPayload_last
          
          
        #stops if we have less than QUERY_SIZE elements, it was the last response
        if (length < tools.queries.QUERY_NB_RESULT):
            break
            


        

clientES.close_point_in_time(id=pit_id)
clientES.transport.close()
exit(0)
  
    
    
    
       
      
     



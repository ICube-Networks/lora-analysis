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


   
def create_updated_entries(response):
    """
    Search if the same phyPayload exists to mark other packets as duplicates
 
    """
    # initialize the next bulk update query
    bulk_update = []
    
    #print(response)
    
    # critical error if a phyPayload wraps several queries and mqtt time diff not sufficient
    if len(response) == tools.queries.QUERY_NB_RESULT and response[0]['_source']['phyPayload'] == response[len(response)-1]['_source']['phyPayload']:
        
        diff_time = datetime.strptime(tools.time.fixMicroseconds(response[len(response)]['_source']['mqtt_time']), tools.time.DATE_FORMAT_ELASTICSEARCH) - datetime.strptime(tools.time.fixMicroseconds(response[0]['_source']['mqtt_time']), tools.time.DATE_FORMAT_ELASTICSEARCH)
        
        
        if (diff_time <= timedelta(minutes = OFFSET_MINUTES_MAX)):
            LOGGER.critical("The time difference for phyPayload "+   + " is equal to " + str(diff_time))
            LOGGER.critical("We may miss some duplicates -> let's top here")
            exit(4)

    
    found = False   # used only for index=1, else, it is handled in the tests
    previous_id = 0
    for index in range(len(response)):
    
        if index >= 1:
            # Same info (NB: the recorded are sorted by <phyPayload, mqtt_time>
            # Thus duplicates are contiguous
            if (response[index-1]['_source']['phyPayload'] == response[index]['_source']['phyPayload']) and  (response[index-1]['_source']['txInfo']['loRaModulationInfo']['spreadingFactor'] == response[index]['_source']['txInfo']['loRaModulationInfo']['spreadingFactor']) and (response[index-1]['_source']['txInfo']['loRaModulationInfo']['bandwidth'] == response[index]['_source']['txInfo']['loRaModulationInfo']['bandwidth']) and (response[index-1]['_source']['txInfo']['loRaModulationInfo']['codeRate'] == response[index]['_source']['txInfo']['loRaModulationInfo']['codeRate']) and (response[index-1]['_source']['txInfo']['frequency'] == response[index]['_source']['txInfo']['frequency']):
            
                # compute the time difference between this record and the previous one
                try:
                    diff_time = datetime.strptime(tools.time.fixMicroseconds(response[index]['_source']['mqtt_time']), tools.time.DATE_FORMAT_ELASTICSEARCH) - datetime.strptime(tools.time.fixMicroseconds(response[index-1]['_source']['mqtt_time']), tools.time.DATE_FORMAT_ELASTICSEARCH)
                    
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




def get_smallest_phyPayload_mqtt_time():
    """
    Returns the smallest (alphabetically) phyPayload in the dataset that has no du_info field
  
    """
    
    clientES = tools.elasticsearch_open_connection()
 
    response = clientES.search(
        index=myconfig.index_name,
        size=1,
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
        fields=[
            "phyPayload",
            "mqtt_time",
        ],
        #sort payload and chronologically (smallest non processed time of the smallest payload)
        sort=["phyPayload.keyword", "mqtt_time"]
    )
    
    clientES.transport.close()

    # no response!
    if response['hits']['total']['value'] == 0:
        return(None, None)
    
    
    min_value = {}
    min_value['phyPayload'] = response['hits']['hits'][0]['_source']['phyPayload']
    min_value['mqtt_time'] = response['hits']['hits'][0]['_source']['mqtt_time']
    return(min_value)


def get_smallest_mqtt_time_phyPayload():
    """
    Returns the earliest  + smallest (alphabetically) phyPayload in the dataset that has no du_info field
 
    """
    
    clientES = tools.elasticsearch_open_connection()
 
    response = clientES.search(
        index=myconfig.index_name,
        size=1,
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
        fields=[
            "mqtt_time",
            "phyPayload",
        ],
        #sort payload and chronologically (smallest non processed time of the smallest payload)
        sort=["phyPayload.keyword", "mqtt_time"]
    )
    
    clientES.transport.close()

    # no response!
    if response['hits']['total']['value'] == 0:
        return(None, None)
    
    
    min_value = {}
    min_value['phyPayload'] = response['hits']['hits'][0]['_source']['phyPayload']
    min_value['mqtt_time'] = response['hits']['hits'][0]['_source']['mqtt_time']
    return(min_value)





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
    
        
    # Scroll now all the documents of the elastic search index until there is no remainnig doc to handle
    while True:
        # retrieve the earliest entry not handled,
        min_value = get_smallest_mqtt_time_phyPayload()

        # no frame to be processed: all of them have a dup_infos field with the right version number
        if min_value['phyPayload'] is None:
            LOGGER.info("The dataset does not contain any phyPayload without a dup_info field (version="+ DUP_INFO_VERSION +")")
            exit(0)
        
        # to detect all the duplicates, shift the mqtt_time_min in the past!
        min_value['mqtt_time'] = (datetime.strptime(tools.time.fixMicroseconds(min_value['mqtt_time']), tools.time.DATE_FORMAT_ELASTICSEARCH) - timedelta(minutes=OFFSET_MINUTES_MAX)).strftime(tools.time.DATE_FORMAT_ELASTICSEARCH)
        
        #info
        LOGGER.info("\t> phyPayload=" + min_value['phyPayload'] + " mqtt_time_min=" + min_value['mqtt_time'])

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
                                "gte": min_value['phyPayload']
                            }
                        }
                    },
                    {
                        "range": {
                            "mqtt_time": {
                                "gte": min_value['mqtt_time']
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
        
        # if all the results own to the same payload, split the query with its mqtt_time
        if len(response) == tools.queries.QUERY_NB_RESULT and response[0]['_source']['phyPayload'] == response[len(response)-1]['_source']['phyPayload']:
        
            #get the min mqtt_time for THIS payload uniquely
            min_value = get_smallest_phyPayload_mqtt_time()
       
            # to detect all the duplicates, shift the mqtt_time_min in the past!
            min_value['mqtt_time'] = (datetime.strptime(tools.time.fixMicroseconds(min_value['mqtt_time']), tools.time.DATE_FORMAT_ELASTICSEARCH) - timedelta(minutes=OFFSET_MINUTES_MAX)).strftime(tools.time.DATE_FORMAT_ELASTICSEARCH)
 
            #all the fields for THIS payload, ranked by the mqtt_time
            response = clientES.search(
                index=myconfig.index_name,
                size=tools.queries.QUERY_NB_RESULT,
                query={
                    "bool": {
                        "filter" : [{
                            "match": {"phyPayload": min_value['phyPayload']}
                        }] ,
                        "must" : [{
                            "range": {
                                "mqtt_time": {
                                    "gte": min_value['mqtt_time']
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
            
            #the rest of the process remains unchanged
        
        #no remaining response
        if (len(response['hits']['hits']) == 0):
            LOGGER.info("\tNo more packets to process in the dataset")
            exit(5)
 
        # add the is_duplicate field to each entry of this response
        bulk_update = create_updated_entries(response['hits']['hits'])

        #push the update
        if len(bulk_update) > 0:
            LOGGER.info("\tPush the update to the server ("+ str(len(bulk_update))+" records)")
            tools.elasticsearch_push_updates(bulk_update)
            LOGGER.info("\t... pushed")
        else:
            LOGGER.info("\tNo update in this window (" + min_value['phyPayload'] + ")")
                 
      
            


        

clientES.close_point_in_time(id=pit_id)
clientES.transport.close()
exit(0)
  
    
    
    
       
      
     



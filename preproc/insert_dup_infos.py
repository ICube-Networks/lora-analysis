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
logger_dup = logging.getLogger('dupinfo')
#logger_dup.basicConfig(stream=sys.stdout, level=logging.INFO)
logger_dup.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)

# debug of the ES connection
logging.getLogger('elastic_transport.transport').setLevel(logging.INFO)


#profiling
import cProfile
    
# parameters
DUP_INFO_VERSION = "1.0"
OFFSET_MINUTES_MAX = 60    # max offset to search for duplicates (length of the time window), number of minutes

# BATCH_FULL = query with a min and max payload
# BATCH_FULL False = batch to get the list of payloads, but then process individually each payload
BATCH_FULL=False


############################################################
#           CONNECTION TO ES SERVER
############################################################


   
def create_updated_entries(response):
    """
    Search if the same phyPayload exists to mark other packets as duplicates
 
    """
    # initialize the next bulk update query
    bulk_update = []
    
    # critical error if a phyPayload wraps several queries and mqtt time diff not sufficient
    if len(response) == tools.queries.QUERY_NB_RESULT and response[0]['_source']['phyPayload'] == response[-1]['_source']['phyPayload']:

        diff_time = datetime.strptime(tools.time.fixMicroseconds(response[-1]['_source']['mqtt_time']), tools.time.DATE_FORMAT_ELASTICSEARCH) - datetime.strptime(tools.time.fixMicroseconds(response[0]['_source']['mqtt_time']), tools.time.DATE_FORMAT_ELASTICSEARCH)
        
        if (diff_time <= timedelta(minutes = OFFSET_MINUTES_MAX)):
            logger_dup.critical("The time difference for phyPayload "+   + " is equal to " + str(diff_time))
            logger_dup.critical("We may miss some duplicates -> let's top here")
            exit(4)

    
    found = False   # used only for index=1, else, it is handled in the tests
    previous_id = 0
    for index in range(len(response)):
        logger_dup.debug("_id=" + response[index]['_id'])
        
        if index >= 1:
            # Same info (NB: the records are sorted by <phyPayload, mqtt_time>
            # Thus duplicates are contiguous
            if (response[index-1]['_source']['phyPayload'] == response[index]['_source']['phyPayload']) and  (response[index-1]['_source']['txInfo']['loRaModulationInfo']['spreadingFactor'] == response[index]['_source']['txInfo']['loRaModulationInfo']['spreadingFactor']) and (response[index-1]['_source']['txInfo']['loRaModulationInfo']['bandwidth'] == response[index]['_source']['txInfo']['loRaModulationInfo']['bandwidth']) and (response[index-1]['_source']['txInfo']['loRaModulationInfo']['codeRate'] == response[index]['_source']['txInfo']['loRaModulationInfo']['codeRate']) and (response[index-1]['_source']['txInfo']['frequency'] == response[index]['_source']['txInfo']['frequency']):
            
                # compute the time difference between this record and the previous one
                try:
                    diff_time = datetime.strptime(tools.time.fixMicroseconds(response[index]['_source']['mqtt_time']), tools.time.DATE_FORMAT_ELASTICSEARCH) - datetime.strptime(tools.time.fixMicroseconds(response[index-1]['_source']['mqtt_time']), tools.time.DATE_FORMAT_ELASTICSEARCH)
                    
                except Exception as e :
                    logger_dup.critical("An error occured when parsing the response -> "+ str(e))
                    logger_dup.critical("response:")
                    logger_dup.critical(json.dumps(response, sort_keys=True, indent=4))
                    logger_dup.critical("index: " + str(index) + " id="+ response[index]['_id'])
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
        logger_dup.debug(json.dumps(req_update, sort_keys=True, indent=4))
        # insert this update to the current sequence
        bulk_update.append(req_update)
    
    return(bulk_update)



def get_nodupinfo_phyPayload_list():
    """
    Returns the phyPayloads in the dataset that has no dup_info field 
 
    """
    
    clientES = tools.elasticsearch_open_connection()
 
    response = clientES.search(
        index=myconfig.index_name,
        size=0, 
        request_cache=False,
        source=False,
        query={
            "bool": {
                "must_not" : [
                    {"term": {"dup_infos.version": DUP_INFO_VERSION}}
                ]
            }
        },
        aggs={
            "list" : {
                "composite": {
                    "size": tools.queries.QUERY_NB_RESULT,
                    "sources" : [
                        { "phyPayload": { "terms": { "field": "phyPayload.keyword" } } }
                    ]
                },
                "aggs":{
                        "min_mqtt_time": {
                            "min" : { "field" : "mqtt_time" }
                        }
                    }
            }
        },
        sort=["phyPayload.keyword", "mqtt_time"],
    )
    clientES.transport.close()
    
    # result
    phyPayload_list = []
    for elem in response['aggregations']['list']['buckets']:
        result = {}
        result['phyPayload']= elem['key']['phyPayload']
        result['doc_count'] = elem['doc_count']
        result['mqtt_time'] = elem['min_mqtt_time']['value_as_string']
        phyPayload_list.append(result)
        
    #NB: the list may be empty if the query does not return any result
    return(phyPayload_list)
        




#return all the packets with the corresponding payload, and a larger MQTT TIME
def get_packets_with_payloads_mqtt_min(phyPayload_min, payload_max, mqtt_time_min):
    """
    Returns the packets with the corresponding phyPayload and a time >= mqtt_time_min - OFFSET_MINUTES_MAX
  
    :param phyPayload_min and max: the min and max phyPayloads to collect (lexicographically)

    :param mqtt_time_min: the mqtt_time_min to read

    """

    # to detect all the duplicates, shift the mqtt_time_min in the past!
    mqtt_time_min = (datetime.strptime(tools.time.fixMicroseconds(mqtt_time_min), tools.time.DATE_FORMAT_ELASTICSEARCH) - timedelta(minutes=OFFSET_MINUTES_MAX)).strftime(tools.time.DATE_FORMAT_ELASTICSEARCH)

    #all the fields for THIS payload, ranked by the mqtt_time
    response = clientES.search(
        index=myconfig.index_name,
        request_cache=False,
        size=tools.queries.QUERY_NB_RESULT,
        query={
            "bool": {
                "must" : [
                    {"range": {
                        "mqtt_time": {
                            "gte": mqtt_time_min
                        }
                    }},
                    {"range": {
                        "phyPayload.keyword":{
                           "gte": phyPayload_min,
                           "lte": payload_max
                        }
                    }}
                ]
            }
        },
        pretty=True,
        human=True,
        #sort them by payload and chronologically
        sort=["phyPayload.keyword", "mqtt_time"],
    )
    
    return(response)



#return all the packets with the corresponding payload, and a larger MQTT TIME
def get_packets_with_payload_mqtt_min(phyPayload, mqtt_time_min):
    """
    Returns the packets with the corresponding phyPayloadS 
  
    :param phyPayload_min and max: the min and max phyPayloads to collect (lexicographically)

    :param mqtt_time_min: the mqtt_time_min to read

    """

    # to detect all the duplicates, shift the mqtt_time_min in the past!
    mqtt_time_min = (datetime.strptime(tools.time.fixMicroseconds(mqtt_time_min), tools.time.DATE_FORMAT_ELASTICSEARCH) - timedelta(minutes=OFFSET_MINUTES_MAX)).strftime(tools.time.DATE_FORMAT_ELASTICSEARCH)

    #all the fields for THIS payload, ranked by the mqtt_time
    response = clientES.search(
        index=myconfig.index_name,
        size=tools.queries.QUERY_NB_RESULT,
        request_cache=False,
        query={
            "bool": {
                "must" : [
                    {"range": {
                        "mqtt_time": {
                            "gte": mqtt_time_min
                        }
                    }},
                    {"match": {"phyPayload": phyPayload}}
                ]
            }
        },
        pretty=True,
        human=True,
        #sort them chronologically
        sort=["mqtt_time"],
    )
    
    return(response)


############################################################
#   SEARCH for records without dup_infos to update them
############################################################



# executable
if __name__ == "__main__":
    """
    Executes the script to insert duplicate info in the elastic search dataset
 
    """
    
    #connections
    clientES = tools.elasticsearch_open_connection()

    # Scroll now all the documents of the elastic search index until there is no remainnig doc to handle
    # Process per phyPayload
    if BATCH_FULL:
        count_pkts = 0
        payload_min = ""
        mqtt_time_min = ""
    while True:
        
        # retrieve 10K non handled phyPayloads (with their mqtt time and their doc_count)
        phyPayload_list = get_nodupinfo_phyPayload_list()
        logger_dup.info("New list of " + str(len(phyPayload_list)) + " payloads")

        # no frame to be processed: all of them have a dup_infos field with the right version number
        if len(phyPayload_list) == 0:
            logger_dup.info("The dataset does not contain any phyPayload without a dup_info field (version="+ DUP_INFO_VERSION +")")
            exit(0)
                
        # for each payload in the list
        for phyPayload_info in phyPayload_list :
            
            if BATCH_FULL:
                #logger_dup.info("\t> phyPayload_min=" + phyPayload_info['phyPayload'] + " doc_count=" + str(phyPayload_info['doc_count']) + " (total=" + str(count_pkts) + "), mintime=" + mqtt_time_min)

                # earliest mqtt_time
                logger_dup.info("\t> " + phyPayload_info['mqtt_time'] + " <? " + mqtt_time_min + " payload="+ phyPayload_info['phyPayload']  + " nb_docs="+ str(phyPayload_info['doc_count']))
                if mqtt_time_min == "" or datetime.strptime(tools.time.fixMicroseconds(mqtt_time_min), tools.time.DATE_FORMAT_ELASTICSEARCH) >  datetime.strptime(tools.time.fixMicroseconds(phyPayload_info['mqtt_time']), tools.time.DATE_FORMAT_ELASTICSEARCH):
                    mqtt_time_min = phyPayload_info['mqtt_time']
                
                
                #min payload
                if payload_min == "" :
                    payload_min = phyPayload_info['phyPayload']
            
                # number of packets
                count_pkts = count_pkts + phyPayload_info['doc_count']
                if (count_pkts < tools.queries.QUERY_NB_RESULT/2):
                    continue
            
                #payload max
                payload_max = phyPayload_info['phyPayload']
           
                #info
                logger_dup.info("\t> phyPayload_min=" + payload_min + " max=" + payload_max + " doc_count=" + str(count_pkts))
            # no bacth full (process individually each payload, not in batch mode)
            else:
            
                #info
                logger_dup.info("\t> phyPayload_min=" + phyPayload_info['phyPayload'] + " nb_docs=" + str(phyPayload_info['doc_count']))
            
           
            # now processs the batch
            while True:
                if BATCH_FULL:
                    response = get_packets_with_payloads_mqtt_min(payload_min, payload_max, mqtt_time_min)
                else:
                    mqtt_time_min = phyPayload_info['mqtt_time']
                    response = get_packets_with_payload_mqtt_min(phyPayload_info['phyPayload'], mqtt_time_min)
                
                # add the is_duplicate field to each entry of this response
                bulk_update = create_updated_entries(response['hits']['hits'])

                #push the update
                if len(bulk_update) > 0 :
                    if BATCH_FULL:
                          logger_dup.info("\t\tPush the update to the server ("+ str(len(bulk_update))+" records) (phyPayload_min="+ payload_min + " phyPayload_max="+ payload_max + " mqtt_min=" + mqtt_time_min + ")")
                    else:
                        logger_dup.info("\t\tPush the update to the server ("+ str(len(bulk_update))+" records) (phyPayload="+ phyPayload_info['phyPayload'] + " mqtt_min=" + mqtt_time_min + ")")
                    tools.elasticsearch_push_updates(bulk_update)
                    logger_dup.info("\t\t... pushed ")
                else:
                    logger_dup.info("\t\tNo update in this window (" + phyPayload_info['phyPayload'] + ")")
                    
                # garbage collector
                del bulk_update[:]

                #no remaining response -> return in the main loop
                if (len(response['hits']['hits']) < tools.queries.QUERY_NB_RESULT):
                    logger_dup.info("\t\tNo more packets to process for phyPayload_min="+phyPayload_info['phyPayload'])
                    
                    if BATCH_FULL:
                        count_pkts = 0
                        payload_min = ""
                        mqtt_time_min = ""
                    break

                # next min payload & mqtt time (depending on the case)
                if BATCH_FULL:
                    payload_min = response['hits']['hits'][-1]['_source']['phyPayload']
                    #logger_dup.info(payload_min + " =?= " + response['hits']['hits'][-1]['_source']['mqtt_time'])
                else:
                    mqtt_time_min = response['hits']['hits'][-1]['_source']['mqtt_time']
                    
        
    clientES.transport.close()
    exit(0)
  
    
    
    
       
      
     



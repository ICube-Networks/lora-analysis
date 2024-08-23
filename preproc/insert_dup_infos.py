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
QUERY_NB_RESULT = 10000
DUP_INFO_VERSION = "1.0"
#NB_LORA_GATEWAYS = 15     # max number of LoRa gateways (and thus, max nb of duplicates)
OFFSET_MINUTES_MAX = 0.5    # max offset to search for duplicates (length of the time window), number of minutes



############################################################
#           CONNECTION TO ES SERVER
############################################################



def create_updated_entries(response):
    """
    Add the dup_infos to the records of the response
 
    """
    # reinit the next bulk update query
    bulk_update = []
    
    #several entries for a single PHY?
    list_phyPayload = []
    for record in response:
        
        #print(json.dumps(record, sort_keys=True, indent=4))
        if record['_source']['phyPayload'] not in list_phyPayload:
            list_phyPayload.append(record['_source']['phyPayload'])
            is_duplicate = False
            #print("ADD: " + record['_source']['phyPayload'] + " / " + record['_source']['mqtt_time'])
            
        else:
            #print("DUP: " + record['_source']['phyPayload'] + " / " + record['_source']['mqtt_time'])
            is_duplicate = True

        # has this record already dup_infos with the right info?
        try:
            assert(record['_source']['dup_infos']['version'] == DUP_INFO_VERSION)
            #logger.DEBUG("--> ok (is_duplicate) (id = " + record['_id'] + ")")
    
        except (KeyError, AssertionError) as e:

            # info on duplicates
            dup_infos = {}
            dup_infos['version'] = DUP_INFO_VERSION
            dup_infos['is_duplicate'] = is_duplicate

            #construct the nex update for this id (decoding the LoRa frame)
            req_update = record['_source']
            req_update['_index']         = myconfig.index_name
            req_update['_id']            = record['_id']
            req_update['dup_infos']    = dup_infos
            LOGGER.debug(json.dumps(req_update, sort_keys=True, indent=4))
              
            # insert this update to the current sequence
            bulk_update.append(req_update)
    return(bulk_update)


def get_first_time_window():
    clientES = tools.elasticsearch_open_connection()
 
    response = clientES.search(
        pit={
            "id": pit_id,
            "keep_alive": "10m",
        },
        #index=myconfig.index_name,
        size=1,
        query={
            "bool": {
                "must_not": {
                    "term" :{
                        "dup_infos.version": DUP_INFO_VERSION
                    }
                }
            }
        },
        #sort them chronologically (just because it's convenient for debuging)
        sort=["mqtt_time"]
    )
    last_record = datetime.strptime(response['hits']['hits'][0]['_source']['mqtt_time'], tools.time.DATE_FORMAT_ELASTICSEARCH)
    next_min = last_record - timedelta(minutes=OFFSET_MINUTES_MAX)
    mqtt_time_min = datetime.strftime(next_min, tools.time.DATE_FORMAT_ELASTICSEARCH)
    
    clientES.transport.close()    
    return(mqtt_time_min)




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
    mqtt_time_min = get_first_time_window()
    LOGGER.info("Start scrolling the records from the date " + mqtt_time_min)
    
    # Scroll now all the documents of the elastic search index until no remainnig doc to handle
    while True:
        LOGGER.info("       > " + mqtt_time_min)

        # Search and Sort the entries chronologically (MUST include the records already handled
        # Else impossible to detect duplicates between those handled those not handled)
        response = clientES.search(
            pit={
                "id": pit_id,
                "keep_alive": "10m",
            },
            #index=myconfig.index_name,
            size=QUERY_NB_RESULT,
            query=tools.queries.QUERY_ALL,
            #sort them chronologically (just because it's convenient for debuging)
            sort=["mqtt_time"],
            search_after=[mqtt_time_min],
        )
        
        #no remaining response
        length = len(response['hits']['hits'])
        if (length == 0):
            break
        
        # remember the date of the last entry in the response
        # the min is not the smallest mqtt_time if we detect duplicates:
        # => a frame in the next window may be a duplicate of a frame in the current window if windows do not opverlap
        # OFFSET_MINUTES_MAX = the max time separating one frame and its duplicate!
        last_record = datetime.strptime(response['hits']['hits'][length-1]['_source']['mqtt_time'], tools.time.DATE_FORMAT_ELASTICSEARCH)
        next_min = last_record - timedelta(minutes=OFFSET_MINUTES_MAX)
        LOGGER.debug(mqtt_time_min + " --> " + datetime.strftime(next_min, tools.time.DATE_FORMAT_ELASTICSEARCH))

        #bug if the next time is before the previous one (the time window cannot contain all the packets in the same query)
        if next_min <= datetime.strptime(mqtt_time_min, tools.time.DATE_FORMAT_ELASTICSEARCH):
            LOGGER.error("The time window ("+str(OFFSET_MINUTES_MAX)+" minutes) is too large.")
            LOGGER.error("An ES query cannot contain all the packets generated during tis time window: "+datetime.strftime(next_min, tools.time.DATE_FORMAT_ELASTICSEARCH)+" / "+mqtt_time_min)
            exit(4)
        else:
            mqtt_time_min = datetime.strftime(next_min, tools.time.DATE_FORMAT_ELASTICSEARCH)
                
        # add the is_duplicate field to each entry of this response
        bulk_update = create_updated_entries(response['hits']['hits'])
        
        #push the update
        if len(bulk_update) > 0:
            tools.elasticsearch_push_updates(bulk_update)
        else:
            LOGGER.info("No update in this time window (" + mqtt_time_min + "/" + last_record + ")")
        
        #stops if we have less than QUERY_SIZE elements, it was the last response
        if (length < QUERY_NB_RESULT):
            break


clientES.close_point_in_time(id=pit_id)
clientES.transport.close()
exit(0)
  
    
    
    
       
      
     



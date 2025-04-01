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
    
    # empty dataframe
    list_processed = pd.DataFrame(columns=('phyPayload', 'spreadingFactor', 'bandwidth', 'codeRate', 'frequency', 'mqtt_time', 'id'))
    
    #a duplicate must have the same payload + PHY info
    for record in response:
    
        found = False
        record_keys = {
                 'phyPayload'       : record['_source']['phyPayload'] ,
                 'spreadingFactor'  : record['_source']['txInfo']['loRaModulationInfo']['spreadingFactor'],
                 'bandwidth'        : record['_source']['txInfo']['loRaModulationInfo']['bandwidth'],
                 'codeRate'         : record['_source']['txInfo']['loRaModulationInfo']['codeRate'],
                 'frequency'        : record['_source']['txInfo']['frequency'],
                 'mqtt_time'        : datetime.strptime(record['_source']['mqtt_time'], tools.time.DATE_FORMAT_ELASTICSEARCH),
                 'id'               : record['_id'],
            }


        # list the records already processed with the same info (phyPayload + PHY info)
        possible_dups = list_processed.query('phyPayload=="'+record_keys['phyPayload']+'" & spreadingFactor=='+str(record_keys['spreadingFactor'])+' & bandwidth=='+str(record_keys['bandwidth'])+' & codeRate=="'+str(record_keys['codeRate'])+'" & frequency=='+str(record_keys['frequency']))

        # and search for an acceptable time difference
        for index, row in  possible_dups.iterrows():
            #print(row)

            diff_time = record_keys['mqtt_time'] - row['mqtt_time']
            if (diff_time <= timedelta(minutes = OFFSET_MINUTES_MAX)):
                found = True
                is_duplicate = True
                original_id = row['id']
                
                break

        # Nothing found -> it's not a duplicate
        if found is False:
            
            # add to the dataframe
            if list_processed.empty:
                list_processed =  pd.DataFrame.from_records([record_keys])
            else :
                list_processed = pd.concat([list_processed, pd.DataFrame.from_records([record_keys])])

            #list_processed.append(record_keys)
            is_duplicate = False
            original_id = record['_id']


        # has this record already dup_infos with the right info?
        try:
            assert(record['_source']['dup_infos']['version'] == DUP_INFO_VERSION)       # dup information version
            assert(record['_source']['dup_infos']['is_duplicate'] == is_duplicate)      # the duplicate was correctly classified
            
            #logger.DEBUG("--> ok (is_duplicate) (id = " + record['_id'] + ")")

        except (KeyError, AssertionError) as e:

            # info on duplicates
            dup_infos = {}
            dup_infos['version'] = DUP_INFO_VERSION
            dup_infos['is_duplicate'] = is_duplicate
            dup_infos['orig'] = original_id

            #construct the nex update for this id (decoding the LoRa frame)
            req_update = record['_source']
            req_update['_index']       = myconfig.index_name
            req_update['_id']          = record['_id']
            req_update['dup_infos']    = dup_infos
            LOGGER.debug(json.dumps(req_update, sort_keys=True, indent=4))
              
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
                "must_not": {
                    "term" :{
                        "dup_infos.version": DUP_INFO_VERSION
                    }
                }
            }
        },
        size=1,
        #sort them chronologically (just because it's convenient for debuging)
        sort=["phyPayload.keyword"]
    )
    
    clientES.transport.close()
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
    LOGGER.info("Start scrolling the records from the payload " + phyPayload_min + " with mqtt_time " + mqtt_time_min)
    
    
    # Scroll now all the documents of the elastic search index until no remainnig doc to handle
    while True:
        LOGGER.info("\t> phyPayload_min=" + phyPayload_min)

        # Search and Sort the entries chronologically (MUST include the records already handled
        # Else impossible to detect duplicates between those handled those not handled)
        response = clientES.search(
            pit={
                "id": pit_id,
                "keep_alive": "10m",
            },
            #index=myconfig.index_name,
            size=tools.queries.QUERY_NB_RESULT,
            query=tools.queries.QUERY_ALL,
            #sort them by payload and chronologically
            sort=["phyPayload.keyword", "mqtt_time"],
            search_after=[phyPayload_min, mqtt_time_min],       # NB: the mqtt min is never updated. We juse use the phypayload
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
            LOGGER.info("\tPush the update to the server")
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
  
    
    
    
       
      
     



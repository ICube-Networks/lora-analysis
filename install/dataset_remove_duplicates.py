""" Enrich the dataset with extrainfos fields .

This scripts parses the dataset to identify duplicates
Same Phypayload with less than 2 hours of diff are considered as duplicates
They may have been received through different gateways.

"""


__authors__ = ("Fabrice Theoleyre")
__contact__ = ("fabrice.theolerye@cnrs.fr")
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

# my tool functions in common for the analysis
import tools

# format
import requests, json, os, tarfile, pathlib
from datetime import datetime
import flask

#logs
import logging
LOGGER = logging.getLogger('dataset_remove_duplicates')
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)



# parameters
QUERY_NB_RESULT = 1000
DUP_INFO_VERSION = "1.0"
NB_LORA_GATEWAYS = 15       #max number of LoRa gateways (and thus, max nb of duplicates)






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
    clientES = Elasticsearch(
        "https://localhost:9200",
        verify_certs=False,
        ssl_show_warn=False,
        basic_auth=(myconfig.user, myconfig.password)
    )
    print(clientES)
    clientES.info()


    result = clientES.open_point_in_time(
        index=myconfig.index_name,
        keep_alive="10m"
    )
    pit_id = result['id']
    
    # Scroll all the documents of the elastic search index
    phyPayload_min=""
    while True:
        #search records without the right extra info version
        response = clientES.options(
            basic_auth=(myconfig.user, myconfig.password),
        ).search(
            pit={
                "id": pit_id,
                "keep_alive": "10m",
            },
            #index=myconfig.index_name,
            size=QUERY_NB_RESULT,
            query={
                "bool": {
                    "must_not": {
                        "term" :{
                            "duplicate_info.version": DUP_INFO_VERSION
                        }
                    }
                }
            },
            collapse={
                "field": "phyPayload.keyword",
                "inner_hits": {
                    "name": "dup_packets",
                    "size": 5,
                    "sort": [ { "phyPayload.keyword": "asc" } ]
                },
                "max_concurrent_group_searches": 5
            },
            #fields=[
            #    "mqtt_time",
            ##    "phyPayload",
            #    "_id",
            #],
            #sort them chronologically (just because it's convenient for debuging)
            sort=["phyPayload.keyword"],
            search_after=[phyPayload_min],
        )
        
        #no response
        length = len(response['hits']['hits'])
        if (length == 0):
            break
        
        #remember the keyword for the last element
        phyPayload_min = response['hits']['hits'][length-1]['_source']['phyPayload']
        print(phyPayload_min)

    
                
        #stops if we have less than QUERY_SIZE elements, it was the last response
        if (length < QUERY_NB_RESULT):
            break


clientES.close_point_in_time(id=pit_id)
exit(0)
  
    
    
    
       
      
        
        
        
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
                
            




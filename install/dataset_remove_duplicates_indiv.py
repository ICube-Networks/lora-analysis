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
from datetime import datetime, timedelta
import flask

#logs
import logging
LOGGER = logging.getLogger('dataset_remove_duplicates')
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)



# parameters
QUERY_NB_RESULT = 10000
DUP_INFO_VERSION = "1.0"
#NB_LORA_GATEWAYS = 15       # max number of LoRa gateways (and thus, max nb of duplicates)
OFFSET_MINUTES_MAX = 120    # max offset to search for duplicates (length of the time window), number of minutes
DATE_FORMAT_ELASTICSEARCH = "%Y-%m-%dT%H:%M:%S.%fZ"     # format of the date




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
    
    # Scroll all the documents of the elastic search index, starting from 1970
    mqtt_time_min = 0
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
            fields=[
                "mqtt_time",
                "phyPayload",
                "_id",
            ],
            #sort them chronologically (just because it's convenient for debuging)
            sort=["mqtt_time"],
            search_after=[mqtt_time_min],
        )
        
        #no response
        length = len(response['hits']['hits'])
        if (length == 0):
            break
        
        #remember the date of the last
        mqtt_time_min = response['hits']['hits'][length-1]['_source']['mqtt_time']
       
       
        # test that each packet has no previous record with the same payload
        for record in response['hits']['hits']:
            #print(json.dumps(record['_source'], sort_keys=True, indent=4))
        
            mqtt_time = datetime.strptime(record['_source']['mqtt_time'], DATE_FORMAT_ELASTICSEARCH)
            mqtt_time_before = mqtt_time - timedelta(minutes=OFFSET_MINUTES_MAX)
           
        
            #search records without the right extra info version
            response_bis = clientES.options(
                basic_auth=(myconfig.user, myconfig.password),
            ).search(
                pit={
                    "id": pit_id,
                    "keep_alive": "10m",
                },
                size=10,
                query={
                    "bool": {
                        "filter": [
                            {
                              "range":{
                                "mqtt_time":{
                                     "gte": datetime.strftime(mqtt_time_before, DATE_FORMAT_ELASTICSEARCH),
                                     "lte": datetime.strftime(mqtt_time, DATE_FORMAT_ELASTICSEARCH)
                        
                                }
                              }
                            }
                          ],
                          "must": {
                                "term" :{
                                    "phyPayload": record['_source']['phyPayload']
                                }
                            }
                    }
                },
            )
            
            if len(response_bis['hits']['hits']) > 0:
                print(json.dumps(response_bis.body, sort_keys=True, indent=4))
        
            
            #print(
            #    datetime.strftime(mqtt_time_before, DATE_FORMAT_ELASTICSEARCH)
            #    + " / " +
            #    datetime.strftime(mqtt_time, DATE_FORMAT_ELASTICSEARCH)
            #)

        
        print(mqtt_time_min)
        #exit(0)
       
        
        #stops if we have less than QUERY_SIZE elements, it was the last response
        if (length < QUERY_NB_RESULT):
            break


clientES.close_point_in_time(id=pit_id)
exit(0)
  
    
    
    
       
      
        
        




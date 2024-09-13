#!/usr/bin/env python3

""" Delete a period of time in the eslastic search dataset .

This scripts deletes the records between two dates .

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

# my tool functions in common for the analysis
import tools


# format
#import requests, json, os, tarfile, pathlib
#from datetime import datetime
#import matplotlib.dates as mdates

#logs
import logging
LOGGER = logging.getLogger('dataset_decodeFrames')
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)








############################################################
#   SEARCH for records in this period
############################################################

DATE_MIN = "2022-03-01"
DATE_MAX = "2022-06-30"


# executable
if __name__ == "__main__":
    """Executes the script to plot the histogram of the number of packets per SF
 
    """
     
     
     
     

    ############################################################
    #           CONNECTION TO ES SERVER
    ############################################################



    #elastic connection
    DEBUG_ES = True
    clientES = tools.elasticsearch_open_connection()

    # Scroll all the documents of the elastic search index

    LOGGER.info("Start deleting the records")

        #search records without the right extra info version
    response = clientES.options(
        basic_auth=(myconfig.user, myconfig.password),
    ).delete_by_query(
        index=myconfig.index_name,
        query={
            "bool": {
                "filter": [
                    {
                        "range":{
                            "mqtt_time":{
                                "gte": DATE_MIN,
                                "lte": DATE_MAX
                            }
                        }
                    }
                ]
            }
        }
    )

    # num of records
     
   
    
    print(response)
    
    #print("length:", length)
    #if (length == 0):
    #    LOGGER.info("No remaining entry without the right extra_infos field (version=" + EXTRA_INFO_VERSION + ")")
    #    break
   


    clientES.transport.close()
    exit(0)

""" inter packet time distribution analysis .

This scripts extracts from an elasticsearch instance statistics concerning
the list of devAddr, and the corresponding packets (timeseries). Then, it analyzes
the inter packet time distribution

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

# configuration parameters
import myconfig

# my tool functions in common for the analysis
import tools

# numerical libraries
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# format
import requests, json, os, tarfile, pathlib
import matplotlib.dates as mdates
from datetime import datetime, timedelta


# Import seaborn
import seaborn as sns
   
#logs
import logging
logger_interpkt = logging.getLogger('intetrpkt_distribution')
logger_interpkt.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)



# parameters
DEVADDR_COUNT_MAX = 10000000        # max number of devices to support
AGG_OFFSET = 1000      # offset for the pagination in the aggregatied query
DATE_FORMAT_ELASTICSEARCH = "%Y-%m-%dT%H:%M:%S.%fZ"     # format of the date
QUERY_NB_RESULT = 10000         #  number of results for our elastic search queries



def es_query_get_devAddr(clientES):
    """ Elastic query to get the list of unique devAddrs
     
    :param clientES is an active connection to an elastic search server
    
    :returns: a list of all devAddrs
    
    :rtype: list of string
    """


    #Until we have still devAddr to get
    pagination_count = 0
    list_devAddr = []
    while True:
    
        response = clientES.options(
            basic_auth=(myconfig.user, myconfig.password)
        ).search(
            index=myconfig.index_name,
            size=0,
            request_timeout=3000,
            pretty=True,
            human=True,
            query=tools.queries.QUERY_DATA_NODUP,
            aggs={
                'devAddr': {
                    "terms": {
                        "field": 'extra_infos.phyPayload.macPayload.fhdr.devAddr.keyword',
                        "size": DEVADDR_COUNT_MAX,
                    },
                    "aggs": {
                    "bucket_sort": {
                        "bucket_sort": {
                            "sort": [{
                                "_key": {
                                    "order": "asc"
                                }
                            }],
                            # "from" and "size" use above terms bucket size. It implements the pagination
                            "from": pagination_count * AGG_OFFSET,
                            "size": AGG_OFFSET
                        }
                    }
                }
                }
            }
        )
        
        # for the next page
        pagination_count = pagination_count + 1
        
        # no more record
        if (len(response["aggregations"]["devAddr"]["buckets"]) == 0):
            logger_interpkt.info("no more page in the aggregate query")
            break;
        
        
        # dump the json response for debug (VERY verbose !!)
        logger_interpkt.debug("GET_LIST:" + json.dumps(response.body, sort_keys=True, indent=4))
        
        # add the corresponding devAddr in the list
        for record in response["aggregations"]["devAddr"]["buckets"]:
            list_devAddr.append(record["key"])
  
   
    # end of extraction
    logger_interpkt.info("Found " + str(len(list_devAddr)) + " different devAddrs")
   
    #result
    return(list_devAddr)
 
   

def eq_query_get_interpkt(clientES, devAddr):
    """ Elastic query to get the list of inter packet time for a given devAddr
        
    :param clientES is an active connection to an elastic search server
    
    :param the devAddr to search in the DB.
        
    :returns: a list of all inter packet time
    
    :rtype: list of durations
    """

 
    # get the list of packets for this devAddr
    # -> data packets only (with a devAddr)
    # -> without duplicates
    # -> ordered chronologically (by mqtt_time)
    list_interpkt_time = []
    mqtt_time_min = 0
    while True:
    
        response = clientES.options(
            basic_auth=(myconfig.user, myconfig.password)
        ).search(
            index=myconfig.index_name,
            size=QUERY_NB_RESULT,
            request_timeout=3000,
            pretty=True,
            human=True,
            query={
                "bool": {
                    "filter" : [
                        {"match": {"dup_infos.is_duplicate": False}},
                        {"match": {"extra_infos.phyPayload.mhdr.mType": "2"}},
                        {"match": {"extra_infos.phyPayload.macPayload.fhdr.devAddr.keyword": devAddr}},
                    ],
                }
            },
            fields=[
                "mqtt_time",
            ],
            sort=[
                {"mqtt_time" : "asc"}
            ],
            search_after=[mqtt_time_min],
            source = False
        )
            
        #no remaining response
        length = len(response['hits']['hits'])
        if (length == 0):
            break

        # dump the json response for debug (VERY verbose !!)
        logger_interpkt.debug("GET_LIST:" + json.dumps(response.body, sort_keys=True, indent=4))
      
        # computes the inter packet time
        for i in range(0, len(response["hits"]["hits"])  - 1):
            
            list_interpkt_time.append(
                datetime.strptime(response["hits"]["hits"][i+1]["fields"]["mqtt_time"][0], DATE_FORMAT_ELASTICSEARCH)\
                -\
                datetime.strptime(response["hits"]["hits"][i]["fields"]["mqtt_time"][0], DATE_FORMAT_ELASTICSEARCH)
            )
        
        #stops if we have less than QUERY_SIZE elements, it was the last response
        if (length < QUERY_NB_RESULT):
            break
            
        # next page for the query
        mqtt_time_min = response['hits']['hits'][length-1]['fields']['mqtt_time'][0]
       
    logger_interpkt.debug(devAddr + " -> ", list_interpkt_time)
    logger_interpkt.info(devAddr + " -> " + str(len(list_interpkt_time)))

    
        
        
# executable
if __name__ == "__main__":
    """Executes the script to analyze the distribution of inter packet times
 
    """
    
    clientES = tools.elasticsearch_open_connection()

    #get the list of devaddrs in the elastic search DB
    list_devAddr = es_query_get_devAddr(clientES)

    #get the inter packet times for a given devAddr
    for devAddr in list_devAddr :
        eq_query_get_interpkt(clientES, devAddr)
     
     
    clientES.transport.close()


#save the pandas dataframe into parcket / pickle / feather / XX csv XX

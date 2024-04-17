""" Tools for elastic search manipulations.

This module loads a few specific tools to help for elastic search queries.

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

# configuration parameters
import myconfig

# json
import json
import flask 

# numerical libraries
import pandas as pd
from matplotlib.dates import MO, TU, WE, TH, FR, SA, SU

# elastic search for the queries
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from elasticsearch.helpers import parallel_bulk


#logs
import logging
logger_tool = logging.getLogger('tools')
logger_tool.setLevel(logging.INFO)



            
############################################################
#           ES -> dataframe   (two dimensions array)
############################################################


# walk recursively in an aggregated reply
def elasticsearch_walk_aggrep(es_reply, agg_names, depth, results_df, tuple, field_values, key_as_string):
    """Walk in an aggregate elastic search response.
    
    This function parses a json elastic search response from an aggregate query to store everything in a dataframe. It is a recursive implementation of elasticsearch_agg_into_dataframe(). So, there is probaby no need no call it directly.
    
    :param string es_reply: a json reply from the elastic search server.
    
    :param list of strings agg_names: list of field names to parse the elastic search query.
    
    :param depth: the depth where we are from the ground zero (recursive function)
    
    :param results_df: pandas dataFrame to enrich with a novel record.
    
    :param tuple: the partial tuple to insert in the dataFrame. Will be inserted only when it is complete (max depth -- last level in the elastic response).
    
    :param field_values: list of field values to retrieve when we reach the maximum depth (last level in the elastic response).
    
    :param  key_as_string: force the key to be considered a string (useful to retrieve a key which is a date, and which should not be interpreted).
    
    :returns: pandas dataFrame containing the histogram
    :rtype: DataFrame
    """


    if key_as_string:
        key = "key_as_string"
    else:
        key = "key"
    
    #last recursive call, save the value (doc_count) in the tuple, and push the tuple into the dataframe
    if(depth == len(agg_names)):
        logger_tool.debug("--last depth----")
        logger_tool.debug(es_reply)
        tuple['count'] = [es_reply['doc_count']]
        
        # the parameter is not an empty string: store as well the corresponding value
        for field_value in field_values:
            
            logger_tool.debug("Field: ", field_value)

        
            tuple[field_value] = [es_reply[field_value]['value']]
            if 'value_as_string' in es_reply[field_value]:
                tuple[field_value+'as_string'] = [es_reply[field_value]['value_as_string']]
        
        
        
        results_df = pd.concat([results_df, pd.DataFrame.from_dict(tuple)], ignore_index=True)
        return(results_df)

    #debug
    logger_tool.debug("-----------------")
    logger_tool.debug("depth:" + str(depth))
    logger_tool.debug(" agg_names=" + agg_names[depth])
    logger_tool.debug(es_reply)
    logger_tool.debug("-----------------")

    #keep on walking recursively in the aggregated reply
    for elem in es_reply[agg_names[depth]]["buckets"]:
        logger_tool.debug("----- elem -------")
        logger_tool.debug(elem)
    
        # store the corresponding value for this tuple
        tuple[agg_names[depth]] = [elem['key']]
        results_df = elasticsearch_walk_aggrep(es_reply=elem, agg_names=agg_names, depth=depth+1, results_df=results_df, tuple=tuple, field_values=field_values, key_as_string=key_as_string)
        
    return(results_df)
   


# transform an elastic search reply to an agg query into a pandas dataframe
def elasticsearch_agg_into_dataframe(es_reply, agg_names, field_values="", key_as_string=True):
    """Transofrm an aggregate elastic search response into a pandas dataFrame.
    
    This function parses a json elastic search response from an aggregate query to store everything in a dataframe. The function expects a json reply with a list of aggregate name (for which a bucket field exists) to finally extract the final count number of elastic search. Then, all the values of the fields are stored with the count number in the pandas dataFrame.
    
    :param string es_reply: a json reply from the elastic search server.
    
    :param list of strings agg_names: list of field names to parse the elastic search query.
        
    :param field_values: list of field values to retrieve when we reach the maximum depth (last level in the elastic response).
    
    :param  key_as_string: force the key to be considered a string (useful to retrieve a key which is a date, and which should not be interpreted).
    
    :returns: pandas dataFrame containing the histogram
    :rtype: DataFrame
    """
        
    logger_tool.debug("Start analysis")
    
       
    #panda dataframe
    results_df = pd.DataFrame()
    tuple = {}
    
    #walk in the response of elastic search with the aggregated fields
    results_df = elasticsearch_walk_aggrep(es_reply=es_reply["aggregations"], agg_names=agg_names, depth=0, results_df=results_df, tuple=tuple, field_values=field_values, key_as_string=key_as_string)
    
    logger_tool.debug("Start analysis")
    logger_tool.debug(results_df)
        
    return(results_df)




############################################################
#           Generic Queries (body of ES)
############################################################


class queries:
    """
    Class that regroups common elastic search queries.
     
    """
    
    # max number of results for one query
    QUERY_NB_RESULT = 10000
   
  
    # fields extra_info exist for the frame
    QUERY_EXTRAINFO_EXIST =  {
            "bool": {
              "must": [
                    {
                      "exists": {
                      "field": "extra_infos"
                     }
                  }
                ]
            }
        }
    """
    Query to match all the documents that have an extra_infos field.
    """
    
    # fields extra_info exist for the frame
    QUERY_EXTRAINFO_EXIST_NODUP =  {
            "bool": {
              "filter" : [
                    {"match": {"dup_infos.is_duplicate": False}},

              ],
              "must": [
                    {
                      "exists": {
                      "field": "extra_infos"
                     }
                  }
                ]
            }
        }
    """
    Query to match all the documents that have an extra_infos field.
    """
    

    # fields extra_info exist for the frame
    QUERY_NOEXTRAINFO_EXIST =  {
            "bool": {
              "must_not": [
                    {
                      "exists": {
                      "field": "extra_infos"
                     }
                  }
                ]
            }
        }
    
    
    """
    Query to match all the documents that have NO extra_infos field.
    """
    
    # the data frames only
    QUERY_NODATA =  {
            "bool": {
              "must_not": [
                    {
                        "term": {
                            "extra_infos.phyPayload.mhdr.mType": "2"
                        }
                     }
                ]
            }
        }
    """
    Query to match all the documents that are NOT data packets (extra_infos mtype=2).
    """

    # the data frames only
    QUERY_DATA =  {
            "bool": {
              "must": [
                    {
                    "term": {"extra_infos.phyPayload.mhdr.mType": "2"}
                    }
                ]
            }
        }
    
    """
    Query to match all the documents that are data packets (extra_infos mtype=2).
    """
    # the data frames only
    QUERY_DATA_NODUP =  {
            "bool": {
              "filter" : [
                    {"match": {"dup_infos.is_duplicate": False}},

              ],
              "must": [
                    {
                        "term": {
                            "extra_infos.phyPayload.mhdr.mType": "2"
                        }
                     }
                ]
            }
        }
    
    """
    Query to match all the documents that are data packets (extra_infos mtype=2), removing duplicates.
    """


    QUERY_ALL = {
            "bool": {
                "filter": [
                    {"match": {"rxInfo.crcStatus": "CRC_OK"}},
                    {
                        "range":{
                            "mqtt_time":{
                                 "gte": "2020-09-01",
                                 #"lte": "2020-12-30",
                                 "format": "year_month_day",
                            }
                        }
                    }
                ]
            }
        }
    """
    Query to match any document corresponding to a correctly received LoRa frame.
    """
    
    QUERY_ALL_NODUP = {
            "bool": {
                "filter": [
                    {"match": {"rxInfo.crcStatus": "CRC_OK"}},
                    {"match": {"dup_infos.is_duplicate": False}},
                    {
                        "range":{
                            "mqtt_time":{
                                 "gte": "2020-09-01",
                                 "format": "year_month_day",
                            }
                        }
                    }
                ]
            }
        }
    """
    Query to match any document corresponding to a correctly received LoRa frame.
    """

############################################################
#           ELASTIC SEARCH
############################################################


def elasticsearch_open_connection():

    """
    Open a connection to the elastic search server
    
    """
    
    #elastic connection
    DEBUG_ES = False
    clientES = Elasticsearch(
        "https://localhost:9200",
        ssl_assert_fingerprint=(myconfig.cert_fingerprint), #certificate of the server (its fingerprint)
        basic_auth=(myconfig.user, myconfig.password),      #credentials
        request_timeout=3000,                              #10s for the requests
    )
    logger_tool.debug(clientES.info())
    
    return(clientES)



def elasticsearch_create_pit(clientES):
    """
    Create a PIT for an existing elastic search connection
    
    """
    
    result = clientES.open_point_in_time(
        index=myconfig.index_name,
        keep_alive="10m"
    )

    return(result['id'])



def elasticsearch_push_updates(bulk_update):
    """
    Pushes a list of updates to the elastic search server
 
    """
    
    clientES = elasticsearch_open_connection()
   

    logger_tool.debug(json.dumps(bulk_update, sort_keys=True, indent=4))

   
    #for okay, result in streaming_bulk(client=clientES_bulk, actions=bulk_update):
    for okay, result in parallel_bulk(client=clientES, actions=bulk_update, chunk_size=1000, thread_count=4):
        action, result = result.popitem()
        
        logger_tool.debug("action: ", action)
        logger_tool.debug("result: ", result)

        if not okay:
            logger_tool.error("Update failed: ", result["_id"])

    clientES.transport.close()




############################################################
#           Day of the week (string) -> int
############################################################



class dayofweek:
    """
    Class that regroups names of the days of week (starting with monday).
     
    """
    short = ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')
    """
    shortnames for the days of the week (starting with monday)
    """
    
    long = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
    """
    long names for the days of the week (starting with monday)
    """


# return the string associated to a weekday id (int)
def shortdayofweek_to_int(day):
    """Transform a short day of week into an in int.
    
    A day of week (short string with 3 letters) is converted in a number (monday = 0)
    
    :param string day: day of week with 3 letters.
    
    :returns: the integer corresponding to this day
    :rtype: int
    """

    for i in range(0, len(dayofweek.short)):
        if dayofweek.short[i] == day:
            return(i)
      
    raise Exception('Unknown week of day')

# return the int associated to a weekday (string)
def int_to_shortdayofweek(day):
    """Transform a int into a short day of week.
    
    A number (monday = 0) is converted into a short day of week (short string with 3 letters)
    
    :param int day: integer (0..6).
    
    :returns: the corresponding day of the week (3 letters only)
    :rtype: string
    """
    
    if (day <0 or day>7):
        raise Exception('Unknown week of day')
    return(dayofweek.short[i])
        
# return the string associated to a weekday id (int)
def longdayofweek_to_int(day):
    """Transform a long day of week into an in int.
    
    A day of week (complete name) is converted in a number (monday = 0)
    
    :param string day: day of week with its long name
    
    :returns: the integer corresponding to this day
    :rtype: int
    """
    
    for i in range(0, len(dayofweek.long)):
        if dayofweek.long[i] == day:
            return(i)
      
    raise Exception('Unknown week of day')

# return the int associated to a weekday (string)
def int_to_longdayofweek(day):
    """Transform a int into a short day of week.
    
    A number (monday = 0) is converted into a long day of week (full name)
    
    :param int day: integer (0..6).
    
    :returns: the corresponding day of the week (long name)
    :rtype: string
    """
    
    if (day <0 or day>7):
        raise Exception('Unknown week of day')
    return(dayofweek.long[i])
     
     
     

# numerical libraries
import pandas as pd
from matplotlib.dates import MO, TU, WE, TH, FR, SA, SU
import sys

#logs
import logging
logger_tool = logging.getLogger('tools')
logger_tool.setLevel(logging.WARN)



            
############################################################
#           ES -> dataframe   (two dimensions array)
############################################################


# walk recursively in an aggregated reply
def elasticsearch_walk_aggrep(es_reply, agg_names, depth, results_df, tuple, field_values, key_as_string):
    if key_as_string:
        key = "key_as_string"
    else:
        key = "key"
    
    #last recursive call, save the value (doc_count) in the tuple, and push the tuple into the dataframe
    if(depth == len(agg_names)):
        logger_tool.info("--last depth----")
        logger_tool.info(es_reply)
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
    logger_tool.info("-----------------")
    logger_tool.info("depth:" + str(depth))
    logger_tool.info(" agg_names=" + agg_names[depth])
    logger_tool.info(es_reply)
    logger_tool.info("-----------------")

    #keep on walking recursively in the aggregated reply
    for elem in es_reply[agg_names[depth]]["buckets"]:
        logger_tool.info("----- elem -------")
        logger_tool.info(elem)
    
        # store the corresponding value for this tuple
        tuple[agg_names[depth]] = [elem['key']]
        results_df = elasticsearch_walk_aggrep(es_reply=elem, agg_names=agg_names, depth=depth+1, results_df=results_df, tuple=tuple, field_values=field_values, key_as_string=key_as_string)
        
    return(results_df)
   


# transform an elastic search reply to an agg query into a pandas dataframe
def elasticsearch_agg_into_dataframe(es_reply, agg_names, field_values="", key_as_string=True):
    logger_tool.info("Start analysis")
    
       
    #panda dataframe
    results_df = pd.DataFrame()
    tuple = {}
    
    #walk in the response of elastic search with the aggregated fields
    results_df = elasticsearch_walk_aggrep(es_reply=es_reply["aggregations"], agg_names=agg_names, depth=0, results_df=results_df, tuple=tuple, field_values=field_values, key_as_string=key_as_string)
    
    logger_tool.info("Start analysis")
    logger_tool.info(results_df)
        
    return(results_df)


############################################################
#           Generic Queries (body of ES)
############################################################


class queries:
      
      
      
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

    # the data frames only
    QUERY_DATA =  {
            "bool": {
              "must": [
                    {
                        "term": {
                            "extra_infos.phyPayload.mhdr.mType": "2"
                        }
                     }
                ]
            }
        }

############################################################
#           Day of the week (string) -> int
############################################################



class dayofweek:
     short = ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')
     long = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
 
            
# return the string associated to a weekday id (int)
def shortdayofweek_to_int(day):
    for i in range(0, len(dayofweek.short)):
        if dayofweek.short[i] == day:
            return(i)
      
    raise Exception('Unknown week of day')

# return the int associated to a weekday (string)
def int_to_shortdayofweek(day):
    if (day <0 or day>7):
        raise Exception('Unknown week of day')
    return(dayofweek.short[i])
        
# return the string associated to a weekday id (int)
def longdayofweek_to_int(day):
    for i in range(0, len(dayofweek.long)):
        if dayofweek.long[i] == day:
            return(i)
      
    raise Exception('Unknown week of day')

# return the int associated to a weekday (string)
def int_to_longdayofweek(day):
    if (day <0 or day>7):
        raise Exception('Unknown week of day')
    return(dayofweek.long[i])
     

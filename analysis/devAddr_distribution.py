""" devEUI & devAddr distribution analysis .

This scripts extracts from an elasticsearch instance statistics concerning
the devEUI/devAddr distribution

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

# numerical libraries
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# format
import requests, json, os, tarfile, pathlib
import matplotlib.dates as mdates

# Import seaborn
import seaborn as sns
   
#logs
import logging
logger_devaddr = logging.getLogger('devaddr_distribution')
logger_devaddr.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)




def es_query_count_for_field(params):
    """ Elastic search query for a double aggregate query.
    
    This function sends a query to an elastic search server to retrive the doc counts for two field values (fieldname1 and fieldname2), and returns the corresponding pandas DataFrame
    
    
    :param dictionary params: a list of two field names (with the keys fieldname1 and fieldname2) to construct the elastic search query
    
    :returns: a pandas DataFrame which contains the count numbers for each fields in the params variable (hierarchical aggregate)
    :rtype: DataFrame
    """

    clientES = tools.elasticsearch_open_connection()

    #get the number of valid records per day of the week
    resp = clientES.options(
        basic_auth=(myconfig.user, myconfig.password)
    ).search(
        index=myconfig.index_name,
        size=0,
        pretty=True,
        human=True,
        query=tools.queries.QUERY_EXTRAINFO_EXIST_NODUP, 
        aggs={
            params['fieldname1']: {
                "terms": {
                    "field": params['fieldname1'],
                    "size": 1000,
                },
                "aggs": {
                    params['fieldname2']: {
                        "terms": {
                            "field": params['fieldname2'],
                            "size": 1000,
                        }
                    }
                }
            }
        }
    )
    #print(resp)
    
    # transform the aggregation results into a pandas' dataframe
    results_df = tools.elasticsearch_agg_into_dataframe(es_reply=resp, agg_names=(params['fieldname1'],params['fieldname2']), key_as_string=False)
    
    # close the elastic connection
    clientES.transport.close()
    
    if results_df.empty:
        logger_devaddr.critical("Empty pandaframe")
        exit(2)
    
    #result
    return(results_df)
 
    
 

    
# trafic per day of week
def plot_pkt_per_devaddr():
    """Plot the graph for the devaddr distribution.
    
    Generates two seaborn plots:
    
    # distribtuion of the number of packets per devAddr per fow per LoRa gateway
    # distribtuion of the number of packets per devEUI per fow per LoRa gateway

    :param Elasticsearch clientES: an active connection to an elastic search server

    """

    params_list = []
    
    #parameters of the query for devAddr
    params_list.append({
        'fieldname1' : 'extra_infos.phyPayload.macPayload.fhdr.devAddr.keyword',
        'fieldname2' : 'rxInfo.gatewayID.keyword',
        'xlabel' : 'Number of packets per devAddr',
        'figname' : 'figures/devaddr_distrib_pkts_per_devAddr.pdf'
        })
    #parameters of the query for devEUI
    params_list.append({
        'fieldname1' : 'extra_infos.phyPayload.macPayload.devEUI.keyword',
        'fieldname2' : 'rxInfo.gatewayID.keyword',
        'xlabel' : 'Number of packets per devEUI',
        'figname' : 'figures/devaddr_distrib_pkts_per_devEUI.pdf'
        })


    for params in params_list:
        #es query
        results_df = es_query_count_for_field(params=params)
        print(results_df)
        

        logger_devaddr.info("Processing " + params['fieldname2'])
        
        # Create a seaborn visualization
        sns.set()
        sns.set_theme()
        g = sns.ecdfplot(
            data=results_df,
            x="count",
            hue=str,
            palette="tab10",   #only if hue specified
        )
        #g.set_xlim(0, 1000)
        #g.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        g.set(xlabel=params['xlabel'],)
        #plt.legend(bbox_to_anchor=(1, 1), loc=2)
        
        #fsave and flush
        fig = g.figure.savefig(params['figname'])
        g.figure.clf()



def es_query_devaddr_duration(operator, field_scope, field_value):
    """Elastic search query for a double aggregate query.
    
    This function sends a query to an elastic search server to retrieve the doc counts
    for two field values (fieldname1 and fieldname2), and returns the corresponding
    pandas DataFrame
    
    :param string operator: the aggregation operator (max or min) for the query which will be used on the field "field_value".
    
    :param dictionary field_scope: name of the elastic field to define classes (e.g., extra_infos.phyPayload.macPayload.fhdr.devAddr.keyword to group the packets per devAddr)
    
    :param dictionary field_value: name of the field where the operator is applied (e.g. mqtt_time to get the max or min time for a given class)
    
    :returns: a pandas DataFrame which contains the duration of all
    :rtype: DataFrame
    """

    
    # open the connection to the elastic search server
    clientES = tools.elasticsearch_open_connection()

    #PIT creation
  #  pit_id = tools.elasticsearch_create_pit(clientES)
  #  logger_devaddr.debug("PIT id: " + str(pit_id))

    min_next = ""
    while True:
        
        logger_devaddr.info("Min key for the next ES query: " + min_next)

        #get the number of valid records per day of the week
        try:
            resp = clientES.options(
                basic_auth=(myconfig.user, myconfig.password)
            ).search(
                index=myconfig.index_name,
                size=0,
                pretty=True,
                human=True,
                query=tools.queries.QUERY_DATA_NODUP,
                aggs={
                    field_scope: {
                        "composite": {
                        "size" : tools.queries.QUERY_NB_RESULT,
                        "sources": [{
                            field_scope: {
                                "terms": {"field": field_scope}
                            }
                        }],
                        "after": { field_scope: min_next }
                      },
                      "aggs":{
                        operator+field_value: {operator: {"field": field_value}},
                        "max"+field_value: {"max": {"field": field_value}}
                      }
                    }
                  },
                  sort=[
                    {field_scope : "asc"},
                  ],
                  search_after=[
                    min_next
                  ],
            )

        # handle the exception
        except Exception as error:
            print("An exception occurred:", error) # An exception occurred: division by zero
            exit(5)
               
        #next minimum value
        min_next = resp['aggregations'][field_scope]['after_key'][field_scope]
                  
        # transform the aggregation results into a pandas' dataframe
        results_df_tmp = tools.elasticsearch_agg_into_dataframe(es_reply=resp, agg_names=(field_scope, ), field_values=(operator+field_value, "max"+field_value), key_as_string=False)
 
 
        # append the new dataframe to the previous one (or copy if it doesn't yet exist)
        if 'results_df' in locals():
            results_df = pd.concat([results_df, results_df_tmp], ignore_index=True)
        else:
            results_df = results_df_tmp


        #print(resp['aggregations'][field_scope])
        logger_devaddr.info("Num records read: " + str(len(resp['aggregations'][field_scope]['buckets'])))

        if (len(resp['aggregations'][field_scope]['buckets']) < tools.queries.QUERY_NB_RESULT):
            break
                
    clientES.transport.close()

    
    
    if results_df.empty:
        logger_devaddr.critical("Empty pandaframe")
        exit(2)
    
    #result
    return(results_df)
 
    




# executable
if __name__ == "__main__":
    """Executes the script to plot the distributions of devaddrs (duration, number of pkts per devAddr and devEUI)
 
    """
    
    
    #plot the number of packets per devAddr / devEUI
    plot_pkt_per_devaddr()


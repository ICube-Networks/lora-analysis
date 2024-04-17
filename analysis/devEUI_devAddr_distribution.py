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


# Import seaborn
import seaborn as sns
   
#logs
import logging
logger_flow = logging.getLogger('flow_distribution')
logger_flow.setLevel(logging.INFO)
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
        logger_flow.critical("Empty pandaframe")
        exit(2)
    
    #result
    return(results_df)
 
    
 

    
# trafic per day of week
def plot_pkt_per_flow():
    """Plot the graph for the flow distribution.
    
    Generates two seaborn plots:
    
    # distribtuion of the number of devAddr per fow per LoRa gateway
    # distribtuion of the number of devEUI per fow per LoRa gateway

    :param Elasticsearch clientES: an active connection to an elastic search server

    """

    params_list = []
    
    #parameters of the query for devAddr
    params_list.append({
        'fieldname1' : 'extra_infos.phyPayload.macPayload.fhdr.devAddr.keyword',
        'fieldname2' : 'rxInfo.gatewayID.keyword',
        'xlabel' : 'Number of packets per devAddr',
        'figname' : 'figures/devAddr_traffic_distribution.pdf'
        })
    #parameters of the query for devEUI
    params_list.append({
        'fieldname1' : 'extra_infos.phyPayload.macPayload.devEUI.keyword',
        'fieldname2' : 'rxInfo.gatewayID.keyword',
        'xlabel' : 'Number of packets per devEUI',
        'figname' : 'figures/devEUI_traffic_distrib.pdf'
        })


    for params in params_list:
        #es query
        results_df = es_query_count_for_field(params=params)
        print(results_df)
        
        
        str = params['fieldname2']
        print(str)
        
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



# executable
if __name__ == "__main__":
    """Executes the script to plot the distribution of the number of packets per devAddr and devEUI per LoRa gateway
 
    """
    
    #plot the graphs
    plot_pkt_per_flow()


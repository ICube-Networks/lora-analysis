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
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
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
                    "size": params['fieldname1_size'],
                },
                "aggs": {
                    params['fieldname2']: {
                        "terms": {
                            "field": params['fieldname2'],
                            "size": params['fieldname2_size'],
                        }
                    }
                }
            }
        }
    )
    
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
        'fieldname1_size' : 10000000,
        'fieldname2' : 'rxInfo.gatewayId.keyword',
        'fieldname2_size' : 20,
        'xlabel' : 'Number of packets per devAddr',
        'figname' : 'figures/devaddr_distrib_pkts_per_devAddr.pdf'
        })
    #parameters of the query for devEUI
    params_list.append({
        'fieldname1' : 'extra_infos.phyPayload.macPayload.devEUI.keyword',
        'fieldname1_size' : 10000000,
        'fieldname2' : 'rxInfo.gatewayId.keyword',
        'fieldname2_size' : 20,
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






# executable
if __name__ == "__main__":
    """Executes the script to plot the distributions of devaddrs (duration, number of pkts per devAddr and devEUI)
 
    """
    
    
    #plot the number of packets per devAddr / devEUI
    plot_pkt_per_devaddr()


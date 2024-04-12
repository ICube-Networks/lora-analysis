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
logger_quality = logging.getLogger('quality')
logger_quality.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)



def  es_query_packets():
    """Elastic search query for an histogram.
    
    This function sends a query to an elastic search server  to retrieve an histogram (per week) of the number of packets per SF
    
    :param Elasticsearch clientES: a connection to an elastic search server
    
    :returns: a pandas DataFrame which contains the counts for each window of the histogram
    :rtype: DataFrame
    """

    clientES = tools.elasticsearch_open_connection()


    #get the number of valid records per SF per channel
    resp = clientES.options(
        basic_auth=(myconfig.user, myconfig.password),
    ).search(
        index=myconfig.index_name,
        size=10000,
        request_timeout=300,
        query=tools.queries.QUERY_DATA,
        source=False,
        fields=[
            "rxInfo.rssi",
            "rxInfo.loRaSNR",
            "rxInfo.crcStatus",
            "txInfo.loRaModulationInfo.spreadingFactor",
 #           "txInfo.loRaModulationInfo.codeRate",
            "dup_infos.is_duplicate",
        ]
    )
    
    
      
    #extract the results
    results_df = pd.json_normalize(resp["hits"]['hits'])
  
    # delete useless columns
    results_df = results_df.drop(['_score', '_index'], axis=1)
  
    #rename the fields
    results_df = results_df.rename(columns={
        "fields.rxInfo.rssi": "rssi",
        "fields.rxInfo.loRaSNR": "loRaSNR",
        "fields.rxInfo.crcStatus": "crcStatus",
        "fields.txInfo.loRaModulationInfo.spreadingFactor": "spreadingFactor",
 #       "fields.txInfo.loRaModulationInfo.codeRate": "codeRate",
        "fields.dup_infos.is_duplicate": "is_duplicate",
    }, errors="raise")

    #flatten the values (by default, each value is an array with one element
    results_df = results_df.explode('rssi')
    results_df = results_df.explode('loRaSNR')
    results_df = results_df.explode('crcStatus')
    results_df = results_df.explode('spreadingFactor')
 #   results_df = results_df.explode('codeRate')
    results_df = results_df.explode('is_duplicate')
    
    # convert the codeRate string into a float value
    #results_df['codeRateFloat'] = results_df['codeRate'].str.replace(r'[\'\"]', '').apply(eval)
     
    # close the elastic connection
    clientES.transport.close()

    if results_df.empty:
        logger_flow.critical("Empty pandaframe")
        exit(2)


    #result
    return(results_df)





def plot_SF_SNR_RSSI(results_df):
    """Plot the SF distribution.
    
    This function plots the pairplot (correlation) of the different fields.
    
    :param pandas dataFrame containing the information
    """

 
    # Create a seaborn visualization
    sns.set()
    sns.set_theme(style='whitegrid')
    sns.set(font_scale=1)

    g = sns.pairplot(
        data=results_df,
        diag_kind="kde",
        corner=True,
    )
    g.map_lower(sns.kdeplot, levels=4, color=".2", warn_singular=False)     # no warning if we have no variance for one variable
    
  

    # save the figure
    fig = g.figure.savefig("figures/SF_RSSI_SNR.pdf")
    g.figure.clf()

 

 
 

    


# executable
if __name__ == "__main__":
    """Executes the script to analyze the link qualities
 
    """
    
    #elastic search query, transformed in a panda dataFrame
    results_df = es_query_packets()
    logger_quality.info(results_df)
    
    #plot it
    plot_SF_SNR_RSSI(results_df)
    




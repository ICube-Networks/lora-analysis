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
        query=tools.queries.QUERY_DATA_NODUP,
        source=False,
        fields=[
            "rxInfo.rssi",
            "rxInfo.loRaSNR",
            "rxInfo.crcStatus",
            "txInfo.spreadingFactor",
            "txInfo.codeRate",
            "dup_infos.is_duplicate",
        ]
    )
    print(resp["hits"]['hits'][0])
    
    #extract the results
    results_df = pd.json_normalize(resp["hits"]['hits'])
  
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

    print(results_df)

    # Create a seaborn visualization
    sns.set()
    sns.set_theme(style='whitegrid')
    sns.set(font_scale=1)

    g = sns.pairplot(
        data=results_df,
        diag_kind="kde",
    )

    # common
    #axes = g.axes.flat[0]
    #g.set(xlabel='Date', ylabel='Number of packets per day')
    #g.set(ylim=(0, None))

 

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
    




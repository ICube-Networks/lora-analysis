""" Link quality indicators correlation .

This scripts selects randomly X packets and plot 
the correlation between different link quality indicators and features


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
logger_quality_corr = logging.getLogger('quality')
logger_quality_corr.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)


#variables
NUMPACKETS_MAX = 10000


def  es_query_packets():
    """Elastic search query for an histogram.
    
    This function sends a query to an elastic search server  to retrieve a bunch of packets, randomly picked in the dataset
    
    :param Elasticsearch clientES: a connection to an elastic search server
    
    :returns: a pandas DataFrame which contains at most NUMPACKETS_MAX packets
    :rtype: DataFrame
    """

    clientES = tools.elasticsearch_open_connection()
    
    #get the number of valid records per SF per channel
    minkey = 0               # we start with the value 0
    counter_numpackets = 0   # we count the numnber of packets we already processed
    logger_quality_corr.info("Min hash key for the next ES query: " + str(minkey))
    while True:

        resp = clientES.options(
            basic_auth=(myconfig.user, myconfig.password),
        ).search(
            index=myconfig.index_name,
            size=min(NUMPACKETS_MAX, tools.queries.QUERY_NB_RESULT),
            query=tools.queries.QUERY_DATA,
            source=False,
            fields=[
                "rxInfo.rssi",
                "rxInfo.loRaSNR",
                "rxInfo.crcStatus",
                "rxInfo.channel",
                "txInfo.loRaModulationInfo.spreadingFactor",
                "mqtt_time"
            ],
            search_after=[
                minkey
            ],
            sort=[{
                "_script": {
                    "type": "number",
                    #index with a random double value
                    #"script": "Random r = new Random(); double value; value = r.nextDouble(); return(value);",
                    #index with a hash of the PHY + mqtt_time (to have the same key each time)
                    "script": "return(Math.abs(doc['phyPayload.keyword'].hashCode() + doc['mqtt_time'].hashCode()));",
                    "order": "asc",
                },
            }],
        )
        length = len(resp['hits']['hits'])
        minkey = resp['hits']['hits'][length-1]['sort'][0]
            
        #extract the results
        df_tempo = pd.json_normalize(resp['hits']['hits'])
      
        # delete useless columns
        df_tempo = df_tempo.drop(['_score', '_index', 'sort'], axis=1)
      
        #rename the fields
        df_tempo = df_tempo.rename(columns={
            "fields.rxInfo.rssi": "rssi",
            "fields.rxInfo.loRaSNR": "loRaSNR",
            "fields.rxInfo.crcStatus": "crcStatus",
            "fields.rxInfo.channel": "channel",
            "fields.txInfo.loRaModulationInfo.spreadingFactor": "spreadingFactor",
        }, errors="raise")

        # flatten the values (by default, each value is an array with one element
        df_tempo = df_tempo.explode('rssi')
        df_tempo = df_tempo.explode('loRaSNR')
        df_tempo = df_tempo.explode('crcStatus')
        df_tempo = df_tempo.explode('channel')
        df_tempo = df_tempo.explode('spreadingFactor')
                        
        # Transform boolean fields
        #df_tempo['is_duplicate']  = df_tempo['is_duplicate'].fillna(True)
  
        # append the new dataframe to the previous one (or copy if it doesn't yet exist)
        if 'results_df' in locals():
            results_df = pd.concat([results_df, df_tempo], ignore_index=True)
        else:
             results_df = df_tempo
     
        #stop the iteration now, we have enough results (or no more results)
        counter_numpackets += len(resp['hits']['hits'])
        logger_quality_corr.info("\t " + str(counter_numpackets) + " packets (next min key for the query: " + str(minkey))
    
        if (len(resp['hits']['hits']) < tools.queries.QUERY_NB_RESULT) or (counter_numpackets >= NUMPACKETS_MAX):
            break

     
     
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
#        corner=True,
    )
    g.map_lower(sns.kdeplot, levels=4, color=".2", warn_singular=False)     # no warning if we have no variance for one variable
    
  

    # save the figure
    fig = g.figure.savefig("figures/linkqual_pairplots.pdf")
    g.figure.clf()

 

    


# executable
if __name__ == "__main__":
    """Executes the script to analyze the link qualities
 
    """
    
    #elastic search query, transformed in a panda dataFrame
    results_df = es_query_packets()
    # remove outliers
    results_df = results_df[results_df['rssi'] < 0]
    logger_quality_corr.info(results_df)
    
    #plot it
    plot_SF_SNR_RSSI(results_df)
    

    #link quality for each flow
    



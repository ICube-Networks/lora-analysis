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



NUMPACKETS_MAX = 10


def  es_query_packets():
    """Elastic search query for an histogram.
    
    This function sends a query to an elastic search server  to retrieve an histogram (per week) of the number of packets per SF
    
    :param Elasticsearch clientES: a connection to an elastic search server
    
    :returns: a pandas DataFrame which contains the counts for each window of the histogram
    :rtype: DataFrame
    """

    clientES = tools.elasticsearch_open_connection()

    #PIT creation
    pit_id = tools.elasticsearch_create_pit(clientES)
    print("PID id: ", pit_id)


    #get the number of valid records per SF per channel
    datemin = "0"           # we start with the date 0
    counter_numpackets = 0  # we count the numnber of packets we considered
    while True:
        resp = clientES.options(
            basic_auth=(myconfig.user, myconfig.password),
        ).search(
            #index=myconfig.index_name,
            size=tools.queries.QUERY_NB_RESULT,
            request_timeout=300,
            query=tools.queries.QUERY_DATA,
            source=False,
            fields=[
                "rxInfo.rssi",
                "rxInfo.loRaSNR",
                "rxInfo.crcStatus",
                "rxInfo.channel",
                "txInfo.loRaModulationInfo.spreadingFactor",
                "dup_infos.is_duplicate",
                "mqtt_time",
                "random",
            ],
            runtime_mappings={
                "random": {
                    "type": "keyword",
                    "script": { "source": "String str = doc['mqtt_time'].value.getYear()+doc['mqtt_time'].value.getMonth().toString()+doc['mqtt_time'].value.getDayOfMonth().toString()+doc['mqtt_time'].value.getHour().toString()+doc['mqtt_time'].value.getMinute().toString()+doc['mqtt_time'].value.getSecond().toString()+doc['phyPayload.keyword'].value; emit(Integer.toString(str.hashCode()));"
                    }
                },
            },
            pit={
                "id": pit_id,
                "keep_alive": "1m",
            },
            #search_after=[
            #        random_min,
            #        0
            #],
            #sort=[
            #    {"random": {"order": "asc"}},
            #    {"_score": {"order": "desc"}},
            #],
        )
          
        #extract the results
        df_tempo = pd.json_normalize(resp['hits']['hits'])
      
        # delete useless columns
        df_tempo = df_tempo.drop(['_score', '_index'], axis=1)
      
        #rename the fields
        df_tempo = df_tempo.rename(columns={
            "fields.rxInfo.rssi": "rssi",
            "fields.rxInfo.loRaSNR": "loRaSNR",
            "fields.rxInfo.crcStatus": "crcStatus",
            "fields.rxInfo.channel": "channel",
            "fields.txInfo.loRaModulationInfo.spreadingFactor": "spreadingFactor",
            "fields.dup_infos.is_duplicate": "is_duplicate",
        }, errors="raise")

        #flatten the values (by default, each value is an array with one element
        df_tempo = df_tempo.explode('rssi')
        df_tempo = df_tempo.explode('loRaSNR')
        df_tempo = df_tempo.explode('crcStatus')
        df_tempo = df_tempo.explode('channel')
        df_tempo = df_tempo.explode('spreadingFactor')
        df_tempo = df_tempo.explode('is_duplicate')
        
        # cut useless columns
        
             
        #append the new dataframe to the previous one (or copy if it doesn't yet exist)
        if 'results_df' in locals():
            results_df = pd.concat([results_df, df_tempo])
            print("concat")
            
        else:
            results_df = df_tempo
            print("create")
     
     
        #stop the iteration now, we have enough results
        counter_numpackets += tools.queries.QUERY_NB_RESULT
        if (len(resp['hits']['hits']) < tools.queries.QUERY_NB_RESULT) or (counter_numpackets > NUMPACKETS_MAX):
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
    




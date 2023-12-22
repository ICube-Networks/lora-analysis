#!/usr/bin/env python3


"""Temporal analysis of the traffic.

Plots the distribution of traffic per day of week and per hour.

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
from datetime import datetime
import matplotlib.dates as mdates

# Import seaborn
import seaborn as sns
    
  
SIZE_RESP_ELASTIC = 1000000
"""
Constant signifying the number of records to count in the query per class

An aggregate query
"""
  

def es_query_traffic_per_dayofweek(clientES):
    """Elastic search query for a distribution of packets per day of week.
    
    This function sends a query to an elastic search server to retrive the distribution of traffic per day of week (i.e., Monday, etc.)
    
    :param Elasticsearch clientES: a connection to an elastic search server
    
    :returns: a pandas DataFrame which contains the result of the ES query
    :rtype: DataFrame
    """

  
    #get the number of valid records per day of the week
    resp = clientES.options(
        basic_auth=(myconfig.user, myconfig.password)
    ).search(
        index=myconfig.index_name,
        size=0,
        request_timeout=300,
        pretty=True,
        human=True,
        query=tools.queries.QUERY_ALL,
        runtime_mappings={
            "day_of_week": {
                "type": "keyword",
                "script": { "source": "emit(doc['mqtt_time'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ROOT));" }
            },
            "date-day": {
                "type": "keyword",
                "script": {
                "source":"emit(doc['mqtt_time'].value.getYear()+doc['mqtt_time'].value.getMonth().toString()+doc['mqtt_time'].value.getDayOfMonth().toString());" }
            }
    },
    aggs={
        "day_of_week": {
            "terms": {
                "field": "day_of_week",
                "size": 7
            },
            "aggs":{
                "date-day": {
                    "terms": {
                        "field": "date-day",
                        "size": SIZE_RESP_ELASTIC
                    }
                }
            }
        }
    },
    )
    #print(resp)
    
    # transform the aggregation results into a pandas' dataframe
    results_df = tools.elasticsearch_agg_into_dataframe(es_reply=resp, agg_names=("day_of_week", "date-day"), key_as_string=False,)
 
    #mapping to sort with the day of week
    results_df["day_of_week"] = pd.Categorical(results_df["day_of_week"], tools.dayofweek.long[:len(tools.dayofweek.long)])
    results_df = results_df.sort_values("day_of_week")


    return(results_df)
    
    
    
  
# trafic per day of week
def plot_traffic_per_dayofweek(clientES):
    """Plot a distribution of traffic per day of week.
    
    This function plots the distribution of traffic per day of week (i.e., Monday, etc.)
    
    :param Elasticsearch clientES: a connection to an elastic search server
    

    """

    #result
    results_df = es_query_traffic_per_dayofweek(clientES)
    print(results_df)

    # Create a seaborn visualization
    sns.set()
    sns.set_theme()
    g = sns.relplot(
        data=results_df,
        kind="line",
        x="day_of_week", y="count",
        #palette="tab10",
    )
    g.set(xlabel='Day of the week', ylabel='Number of packets per day')
    g.set(ylim=(0, None))
    g.set_xticklabels(tools.dayofweek.short[:len(tools.dayofweek.short)])
    fig = g.figure.savefig("figures/traffic_per_dayofweek.pdf")
    


# trafic per hour
def es_query_traffic_per_hour(clientES):
    """Elastic search query for a distribution of packets per hour.
    
    This function sends a query to an elastic search server to retrieve the distribution of traffic per hour (0..23)
    
    :param Elasticsearch clientES: a connection to an elastic search server
    
    :returns: a pandas DataFrame which contains the result of the ES query
    :rtype: DataFrame
    """


   #get the number of valid records per hour of the day
    resp = clientES.options(
        basic_auth=(myconfig.user, myconfig.password)
    ).search(
        index=myconfig.index_name,
        size=0,
        request_timeout=300,
        pretty=True,
        human=True,
        query=tools.queries.QUERY_ALL,
        runtime_mappings={
            "hour": {
                "type": "long",
                "script": { "source": "emit(doc['mqtt_time'].value.getHour());" }
            },
            "date-day": {
                "type": "keyword",
                "script": { "source": "emit(doc['mqtt_time'].value.getYear()+doc['mqtt_time'].value.getMonth().toString()+doc['mqtt_time'].value.getDayOfMonth().toString());" }
            }
        },
        aggs={
            "hour": {
                "histogram": {
                    "field": "hour",
                    "interval": "1",
                },
                "aggs":{
                    "date-day": {
                        "terms": {
                            "field": "date-day",
                            "size": 1000000,
                        }
                    }
                }
            }
        }
    )
   # print(resp)
    
    # transform the aggregation results into a pandas' dataframe
    results_df = tools.elasticsearch_agg_into_dataframe(es_reply= resp, agg_names=("hour", "date-day"), key_as_string=False,)
    print(results_df)


    return(results_df)


# trafic per hour
def plot_traffic_per_hour(clientES):
    """Plot a distribution of traffic per hour.
    
    This function plots the distribution of traffic (nb packets) per hour
    
    :param Elasticsearch clientES: a connection to an elastic search server
    
    """

    results_df = es_query_traffic_per_hour(clientES=clientES)
    print(results_df)

    # Create a seaborn visualization
    sns.set()
    sns.set_theme()
    g = sns.relplot(
        data=results_df,
        kind="line",
        x="hour", y="count",
        palette="tab10",
        #hue="event", style="event",
    )
    g.set(xlabel='Hour of the day', ylabel='Number of packets per hour')
    g.set(ylim=(0, None))
    fig = g.figure.savefig("figures/traffic_per_hour.pdf")

 



# executable
if __name__ == "__main__":
    """Executes the script to plot the distribution of the number of packets per day of the week and per hour
 
    """


    #elastic connection
    DEBUG_ES = False
    clientES = Elasticsearch(
        "https://localhost:9200",
        verify_certs=False,
        ssl_show_warn=False,
    )
    print(clientES)


    #plot the graphs
    plot_traffic_per_dayofweek(clientES)
    plot_traffic_per_hour(clientES)

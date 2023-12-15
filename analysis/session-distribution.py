# import the config folder
import sys
sys.path.insert(1, '../config')

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
   
#logs
import logging
logger_session = logging.getLogger('session_distribution')
logger_session.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)



def eq_query_session_duration(clientES, operator, field_scope, field_value):
    #get the number of valid records per day of the week
    resp = clientES.options(
        basic_auth=(myconfig.user, myconfig.password)
    ).search(
        index=myconfig.index_name,
        size=0,
        request_timeout=3000,
        pretty=True,
        human=True,
        query=tools.queries.QUERY_EXTRAINFO_EXIST,
        aggs={
            field_scope: {
              "terms": {
                "field": field_scope,
                "size": 10000,
                "order": {operator+field_value: "desc"}
              },
              "aggs":{
                operator+field_value: {operator: {"field": field_value}},
                "max"+field_value: {"max": {"field": field_value}}
              }
            }
          }
    )
    
    # transform the aggregation results into a pandas' dataframe
    results_df = tools.elasticsearch_agg_into_dataframe(es_reply=resp, agg_names=(field_scope, ), field_values=(operator+field_value, "max"+field_value), key_as_string=False)
    
    
    if results_df.empty:
        logger_session.critical("Empty pandaframe")
        exit(2)
    
    #result
    return(results_df)
 
    
# distribution of the sessions
def plot_sessions(clientES):

    #es query
    results_df = eq_query_session_duration(clientES=clientES, operator="min", field_scope="extra_infos.phyPayload.macPayload.fhdr.devAddr.keyword", field_value="mqtt_time")
    #results_df["minmqtt_timeas_string"] = mdates.date2num(results_df["minmqtt_timeas_string"])
    results_df["session_duration"] = mdates.date2num(results_df["maxmqtt_timeas_string"]) - mdates.date2num(results_df["minmqtt_timeas_string"])
    logger_session.info(results_df)
  
    # Create a seaborn visualization
    sns.set()
    sns.set_theme()
    g = sns.ecdfplot(
        data=results_df,
        x="session_duration (in days)",
    #    hue=str,
    #    palette="tab10",   #only if hue specified
    )
    axes = g.axes
    g.set(xlabel='Duration of the session',)
    
    # formating dates -> not required since this is not a date but a difference (=duration)
    #locator = mdates.AutoDateLocator()
    #formatter = mdates.ConciseDateFormatter(locator)
    #axes.xaxis.set_major_locator(locator)
    #axes.xaxis.set_major_formatter(formatter)
    
    fig = g.figure.savefig('figures/session_distribution.pdf')
    g.figure.clf()







#elastic connection
DEBUG_ES = False
clientES = Elasticsearch(
    "https://localhost:9200",
    verify_certs=False,
    ssl_show_warn=False,
)
print(clientES)




#plot the graphs
plot_sessions(clientES)


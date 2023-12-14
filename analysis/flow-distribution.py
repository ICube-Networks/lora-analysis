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
LOGGER = logging.getLogger('dataset_decodeFrames')
logging.basicConfig(stream=sys.stdout, level=logging.INFO)



def es_query_count_for_field(clientES, fieldname):
    #get the number of valid records per day of the week
    resp = clientES.options(
        basic_auth=(myconfig.user, myconfig.password)
    ).search(
        index=myconfig.index_name,
        size=0,
        request_timeout=300,
        pretty=True,
        human=True,
        aggs={
            fieldname: {
                "terms": {
                    "field": fieldname,
                    "size": 10000000,
                }
            }
        }
    )
    #print(resp)
    
    # transform the aggregation results into a pandas' dataframe
    results_df = tools.elasticsearch_agg_into_dataframe(es_reply=resp, agg_names=(fieldname,), key_as_string=False, debug=False)
    
    
    if results_df.empty:
        LOGGER.critical("Empty pandaframe")
        exit(2)
    
    #result
    return(results_df)
 
    
# trafic per day of week
def plot_pkt_per_flow(clientES):

    params = []
    
    #devAddr
    params.append({
        'fieldname' : 'extra_infos.phyPayload.macPayload.fhdr.devAddr.keyword',
        'xlabel' : 'Number of packets per devAddr',
        'figname' : 'figures/traffic_distribution_per_devAddr.pdf'
        })
    #devEUI
    params.append({
        'fieldname' : 'extra_infos.phyPayload.macPayload.devEUI.keyword',
        'xlabel' : 'Number of packets per devEUI',
        'figname' : 'figures/traffic_distribution_per_devEUI.pdf'
        })


    for param in params:
        #es query
        results_df = es_query_count_for_field(clientES=clientES, fieldname=param['fieldname'])
        print(results_df)
        
        # Create a seaborn visualization
        sns.set()
        sns.set_theme()
        g = sns.ecdfplot(
            data=results_df,
            x="count",
        #    palette="tab10",   #only if hue specified
        )
        g.set(xlabel=param['xlabel'],)
        fig = g.figure.savefig(param['figname'])
        #flush for the next one
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
plot_pkt_per_flow(clientES)


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
logger_flow = logging.getLogger('flow_distribution')
logger_flow.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)



def es_query_count_for_field(clientES, params):
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
    
    
    if results_df.empty:
        logger_flow.critical("Empty pandaframe")
        exit(2)
    
    #result
    return(results_df)
 
    
# trafic per day of week
def plot_pkt_per_flow(clientES):

    params_list = []
    
    #devAddr
    params_list.append({
        'fieldname1' : 'extra_infos.phyPayload.macPayload.fhdr.devAddr.keyword',
        'fieldname2' : 'rxInfo.gatewayID.keyword',
        'xlabel' : 'Number of packets per devAddr',
        'figname' : 'figures/traffic_distribution_per_devAddr.pdf'
        })
    #devEUI
    params_list.append({
        'fieldname1' : 'extra_infos.phyPayload.macPayload.devEUI.keyword',
        'fieldname2' : 'rxInfo.gatewayID.keyword',
        'xlabel' : 'Number of packets per devEUI',
        'figname' : 'figures/traffic_distribution_per_devEUI.pdf'
        })


    for params in params_list:
        #es query
        results_df = es_query_count_for_field(clientES=clientES, params=params)
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


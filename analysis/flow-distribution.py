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
    
    
# trafic per day of week
def plot_pkt_per_flow(clientES):

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
            "pkts_per_MAC": {
                "terms": {
                    "field": "rxInfo.gatewayID.keyword",
                    "size": 1000000,
                }
            }
            }
    )
    print(resp)
    
    # transform the aggregation results into a pandas' dataframe
    results_df = tools.elasticsearch_agg_into_dataframe(es_reply=resp, agg_names=("pkts_per_MAC",), key_as_string=False, debug=False)

    #result
    print(results_df)

    # Create a seaborn visualization
    sns.set()
    sns.set_theme()
    g = sns.ecdfplot(
    #g = sns.displot(
        data=results_df,
    #    kind="kde",
        x="count",
    #    palette="tab10",   #only if hue specified
    )
    g.set(xlabel='Number of packets per MAC address',)
   # g.set(ylim=(0, None))
   # g.set_xticklabels(tools.dayofweek.short[:len(tools.dayofweek.short)])
    fig = g.figure.savefig("figures/traffic_distribution_per_device.pdf")
    







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


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


# trafic per day of week
def plot_traffic_per_dayofweek(clientES):

    #get the number of valid records per day of the week
    resp = clientES.options(
        basic_auth=(myconfig.user, myconfig.password)
    ).search(
        index=myconfig.index_name,
        size=0,
        request_timeout=300,
        pretty=True,
        human=True,
        query={
            "bool": {
                "filter": [
                    {"match": {"rxInfo.crcStatus": "CRC_OK"}},
                    {
                        "range":{
                            "mqtt_time":{
                                 "gte": "2020-09-01",
                                 #"lte": "2020-10-30",
                                 "format": "year_month_day",
                            }
                        }
                    }
                ],
            },
        },
        runtime_mappings={
            "day_of_week": {
                "type": "keyword",
                "script": { "source": "emit(doc['mqtt_time'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ROOT));" }
               }
        },
        aggs={
            "day_of_week": {"terms": {"field": "day_of_week"}}
        },
    )

    # transform the aggregation results into a pandas' dataframe
    results_df = pd.Series()
    for col_df in resp["aggregations"]["day_of_week"]["buckets"]:
        results_df = results_df._append(pd.Series([col_df["doc_count"]], index=[tools.day_of_week_int(col_df["key"])] ))
    results_df = results_df.sort_index()
        
    if True:
        print("------------")
        print(results_df.index.to_numpy)
        print("type: ", type(results_df.index.to_numpy))
        print("------------")
        print(results_df.to_numpy)
        print("type: ", type(results_df.to_numpy))
        print("------------")
   
    #plot
    fig, ax = plt.subplots()
    ax.plot(results_df.index, results_df.values)
    ax.set(xlabel='day of the week', ylabel='Number of packets', title='Distribution of the traffic per day of the week')
    ax.set_ylim(bottom=0)
    ax.grid()
    fig.savefig("figures/traffic_per_day.pdf")
    #plt.show()





# trafic per hour
def plot_traffic_per_hour(clientES):
    #get the number of valid records per hour of the day
    resp = clientES.options(
        basic_auth=(myconfig.user, myconfig.password)
    ).search(
        index=myconfig.index_name,
        size=0,
        request_timeout=300,
        pretty=True,
        human=True,
        query={
            "bool": {
                "filter": [
                    {"match": {"rxInfo.crcStatus": "CRC_OK"}},
                    {
                        "range":{
                            "mqtt_time":{
                                 "gte": "2020-09-01",
                                 #"lte": "2020-10-30",
                                 "format": "year_month_day",
                            }
                        }
                    }
                ],
            },
        },
        runtime_mappings={
            "hour": {
                "type": "long",
                "script": { "source": "emit(doc['mqtt_time'].value.getHour());" }
               }
        },
        aggs={
            "hour_distrib": {
                "histogram": {
                    "field": "hour",
                    "interval": "1"
                }
            }
        },
    )
    print(resp)
    
    # transform the aggregation results into a pandas' dataframe
    results_df = pd.Series()
    for col_df in resp["aggregations"]["hour_distrib"]["buckets"]:
        results_df = results_df._append(pd.Series([col_df["doc_count"]], index=[col_df["key"]] ))
    results_df = results_df.sort_index()
 
    if True:
        print("------------")
        print(results_df.index.to_numpy)
        print("type: ", type(results_df.index.to_numpy))
        print("------------")
        print(results_df.to_numpy)
        print("type: ", type(results_df.to_numpy))
        print("------------")
   

    #plot
    fig, ax = plt.subplots()
    ax.plot(results_df.index, results_df.values)
    ax.set(xlabel='day of the week', ylabel='Number of packets', title='Distribution of the traffic per hour')
    ax.set_ylim(bottom=0)
    ax.grid()
    fig.savefig("figures/traffic_per_hour.pdf")
    #plt.show()






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




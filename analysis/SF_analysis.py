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

############################################################
#           CONNECTION TO ES SERVER
############################################################



#elastic connection
DEBUG_ES = False
clientES = Elasticsearch(
    "https://localhost:9200",
    verify_certs=False,
    ssl_show_warn=False,
)
print(clientES)




############################################################
#           SF per week
############################################################


#get the number of valid records per SF per channel
resp = clientES.options(
    basic_auth=(myconfig.user, myconfig.password),
).search(
    index=myconfig.index_name,
    size=0,
    request_timeout=300,
    query={
        "bool": {
            "filter": [
                {"match": {"rxInfo.crcStatus": "CRC_OK"}},
                {
                    "range":{
                        "mqtt_time":{
                            "gte": "2020-09-01",
                            "format": "year_month_day",
                        }
                    }
                }
            ],
        },
    },
    aggs={
        "SF": {
            "terms" : { "field" : "txInfo.loRaModulationInfo.spreadingFactor" },
            "aggregations": {
                "date": {
                    "date_histogram" : {
                        "field" : "rxInfo.time",
                        "calendar_interval": "week",
                        "format": "yyy-MM-dd",
                        "time_zone": "Europe/Paris"
                    }
                },
            },
        },
     }
)

        
# print
if True:
    #print(resp)
    print("------------")


results_df = tools.elasticsearch_agg_into_dataframe(es_reply=resp, agg_names=("SF", "date"), key_as_string=True, debug=False, )
dtime = datetime(2020, 9, 1, 20)
results_df = results_df[results_df['date'] > dtime.timestamp()]
results_df = results_df[results_df['count'] > 0]
results_df['date'] = pd.to_datetime(results_df['date'], unit='ms')

print(results_df)




# transform the aggregation results into a pandas' dataframe
#results_df = tools.elasticsearch_reply_into_dataframe(es_reply= resp, row_name="SF", col_name="date", debug=False, )





# Create a seaborn visualization
sns.set()
sns.set_theme()
g = sns.relplot(
    data=results_df,
    kind="line",
    x="date", y="count",
    hue="SF", style="SF",
    palette="tab10",
)

# common
axes = g.axes.flat[0]
g.set(xlabel='Date', ylabel='Number of packets per day')
g.set(ylim=(0, None))

# formating dates
locator = mdates.AutoDateLocator()
formatter = mdates.ConciseDateFormatter(locator)
axes.xaxis.set_major_locator(locator)
axes.xaxis.set_major_formatter(formatter)
#axes.tick_params(axis='x', rotation=90)
axes.margins(x=0)

# save the figure
fig = g.figure.savefig("figures/SF_distribution_week.pdf")

 

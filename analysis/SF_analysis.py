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
if DEBUG_ES:
    print(resp["aggregations"]["SF"])
    print("------------")


# transform the aggregation results into a pandas' dataframe
results_df = tools.elasticsearch_reply_into_dataframe(es_reply= resp, row_name="SF", col_name="date", debug=False, )
print(results_df)


#plot
fig, ax = plt.subplots()
for SF in results_df.index:
    x = results_df.columns
    y = results_df.loc[SF]
    ax.plot(mdates.date2num(x), y, label=SF)    # transform the date in seconds into a "real" date

#labels
ax.set(xlabel='Date', ylabel='Number of packets')
ax.legend()

#xtics = a date
locator = mdates.AutoDateLocator()
formatter = mdates.ConciseDateFormatter(locator)
ax.xaxis.set_major_locator(locator)
ax.xaxis.set_major_formatter(formatter)
ax.set_xlim(np.datetime64('2020-09-01'), np.datetime64('2022-02-01'))


#final result stored in a file
fig.savefig("figures/SF_distribution_week.pdf")




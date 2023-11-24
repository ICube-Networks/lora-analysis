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


#elastic connection
DEBUG_ES = False
clientES = Elasticsearch(
    "https://localhost:9200",
    verify_certs=False,
    ssl_show_warn=False,
)
print(clientES)




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
                             "lte": "2020-10-30",
                             "format": "year_month_day",
                        }
                    }
                }
            ],
        },
    },
    aggs={
        "day_of_week": {"terms": {"field": "day_of_week"}}
    },
)

#print(resp)


# transform the aggregation results into a pandas' dataframe
results_df = pd.Series()
for col_df in resp["aggregations"]["day_of_week"]["buckets"]:
    results_df = results_df._append(pd.Series([col_df["doc_count"]], index=[col_df["key"]] ))

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
ax.legend()
ax.set(xlabel='day of the week', ylabel='Number of packets', title='Distribution of the traffic in the week')
ax.grid()
fig.savefig("figures/traffic_per_day_of_week.pdf")
#plt.show()

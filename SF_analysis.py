# elastic search for the queries
from elasticsearch import Elasticsearch

# format
import requests, json, os, tarfile, pathlib
from datetime import datetime

# configuration parameters
import myconfig

# numerical libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# transform an elastic search reply to an agg query into a pandas dataframe
def elasticsearch_reply_into_dataframe(es_reply, row_name, col_name, debug):
    results_df = pd.DataFrame()
    for row_df in es_reply["aggregations"][row_name]["buckets"]:
        nb_total = row_df["doc_count"]
        nb_total_bis = 0
        
        #the row does not exist -> add an empty column
        if row_df["key"] not in results_df.index:
            if debug:
                print("the row ", row_df["key"], " does not exist")
            results_df.loc[row_df["key"], :] = [0] * len(results_df.columns)
        
        for col_df in row_df[col_name]["buckets"]:
            #create the column if it doesn't exit
            if col_df["key"] not in results_df.columns:
                if debug:
                    print("the col ", col_df["key"] , " does not exist")
                results_df.insert(0, col_df["key"] ,  [0] * len(results_df) , True)

            #store the current value in the corresponding cell of the dataframe
            if debug:
                print("   >", col_df["key"], "=", col_df["doc_count"])
            nb_total_bis += col_df["doc_count"]
            results_df.loc[[row_df["key"]],[col_df["key"]]] = col_df["doc_count"]
            
        
        
        if debug:
            print("       ", nb_total, " =?= ", nb_total_bis)

    if debug:
        print("------------")

    return(results_df)



#elastic connection
DEBUG_ES = False
clientES = Elasticsearch(
    "https://localhost:9200",
    verify_certs=False,
    ssl_show_warn=False,
    basic_auth=(myconfig.user, myconfig.password)
)
    

# get the number of valid records per SF per channel
numRecords = pd.Series()
resp = clientES.search(
    size=0,
    query={
        "bool": {
            "filter": [
                {"match": {"rxInfo.crcStatus": "CRC_OK"}},
            ],
        },
    },
    aggs={
        "SF": {
            "terms" : { "field" : "txInfo.loRaModulationInfo.spreadingFactor" },
            "aggregations": {
                 "date": { "date_histogram" : { "field" : "rxInfo.time", "calendar_interval": "day", "time_zone": "Europe/Paris" }},
                # "channels": { "terms" : { "field" : "rxInfo.channel" }},
            },
        },
     }
)
        
# print
if DEBUG_ES:
    print(resp["aggregations"]["SF"])
    print("------------")


# transform the aggregation results into a pandas' dataframe
results_df = elasticsearch_reply_into_dataframe(es_reply= resp, row_name="SF", col_name="date", debug=False, )
print(results_df)


#plot
fig, ax = plt.subplots()
for SF in results_df.index:
    x = results_df.columns
    y = results_df.loc[SF]
    ax.plot(x, y, label=SF)

ax.legend()
ax.set(xlabel='time (s)', ylabel='Nb packets',
           title='Number of packets in the dataset')
ax.grid()

fig.savefig("test.pdf")
#plt.show()

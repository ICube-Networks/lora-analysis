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


#elastic connection
DEBUG_ES = False
clientES = Elasticsearch(
    "https://localhost:9200",
    verify_certs=False,
    ssl_show_warn=False,
)
print(clientES)


# remove any record before 2020 (1970 etc.)
numRecords = pd.Series()
resp = clientES.options(
    basic_auth=(myconfig.user, myconfig.password)
).search(
    index=myconfig.index_name,
    request_timeout=30,
    query={
        "bool": {
            "filter": [
                {
                    "range":{
                        "rxInfo.time":{
                            "lte": "2020-09-01",
                            "format": "yyyy-MM-dd",
                        }
                    }
                }
            ],
        },
    }
)
print(resp)
 

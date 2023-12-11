# import the config folder
import sys
sys.path.insert(1, '../config')
sys.path.insert(1, '../analysis')

# elastic search for the queries
from elasticsearch import Elasticsearch

# configuration parameters
import myconfig

# dissector of LoRaWAN frames
import loradissector

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
response = clientES.options(
    basic_auth=(myconfig.user, myconfig.password),
).search(
    index=myconfig.index_name,
    size=1000,
    request_timeout=300,
)

# one update per doc
for num, doc in enumerate(response['hits']['hits']):
    
    
    #debug
    print("------------")
    print("num=",num," doc_id=",doc['_id'], " phyPayload=", doc['_source']['phyPayload'])
    print(doc)


    #decode the LoRaWAN frame
    extra_infos = loradissector.process_phypayload(doc['_source']['phyPayload'])
    print(json.dumps(extra_infos, sort_keys=True, indent=4))



    #response = elastic_client.update(
    #    index=myconfig.index_name,
    #    doc_type=""_doc"",
    #    id=doc_id['_id'],
    #    body=source_to_update
    #)





#no error
exit(0)




print("------------")



for i in range(4):
    print("------------")
    print(docs[i])
    print("****")
    loradissector.process_phypayload(docs[i]['_source']['phyPayload'])
print("------------")



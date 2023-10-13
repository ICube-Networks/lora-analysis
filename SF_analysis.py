from datetime import datetime
from elasticsearch import Elasticsearch
import requests, json, os, tarfile, pathlib
import myconfig


print(myconfig.password)

exit(0)

#elastic connection
clientES = Elasticsearch(
    "https://localhost:9200",
    verify_certs=False,
    ssl_show_warn=False,
    basic_auth=(myconfig.user, PAmyconfig.passwordSWORD)
)


clientES.index(index=myconfig.index_name, id=i, document=data['_source'])
      
      

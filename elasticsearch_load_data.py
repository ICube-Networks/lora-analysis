from datetime import datetime
from elasticsearch import Elasticsearch
import requests, json, os, tarfile, pathlib
import myconfig


# Parameters
PATTERN="*"
DIR="data"


#elastic connection
clientES = Elasticsearch(
    "https://localhost:9200",
    verify_certs=False,
    ssl_show_warn=False,
    basic_auth=(myconfig.user, myconfig.password)
)


# check if the Elasticsearch index exists
index_exists = clientES.indices.exists(index=myconfig.index_name)
if index_exists:
    print ("INDEX_NAME:", myconfig.index_name, "already exists.")
else:
    print ("INDEX_NAME:", myconfig.index_name, "will be created.")
    

#load each .gz file
i=0
for filename in os.listdir(DIR):
    if filename.endswith(".json.tar.gz"):
        #extract the tar.gz and get the uncompressed filename
        archive = tarfile.open(DIR+"/"+filename, 'r')
        #archive.extractall(path=DIR)
        filename_json = (pathlib.Path(filename).with_suffix('')).with_suffix('')
        
        #inject the data of the json in the corresponding index
        pfile = open(os.path.join(DIR,filename_json))
        print(DIR,filename_json)
        while True:
            data = json.loads(pfile.readline())
            if not data:
                break
            
            #the line is correct, let's add the record
            clientES.index(index=myconfig.index_name, id=i, document=data['_source'])
            i = i + 1
            
            if (i % 1000 == 0):
                print(i)
        os.remove(os.path.join(DIR, filename_json))

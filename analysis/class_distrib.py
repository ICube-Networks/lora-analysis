""" distribution of the traffic per class.

"""

__authors__ = ("Fabrice Theoleyre")
__contact__ = ("fabrice.theoleyre@cnrs.fr")
__copyright__ = "CNRS"
__date__ = "2025"
__version__= "1.0"





# import the config folder
import sys
sys.path.insert(1, '../config')
sys.path.insert(1, '../tools')
import glob     #to search filenames with wildcards

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
import math

# format
import requests, json, os, tarfile, pathlib
import matplotlib.dates as mdates
from datetime import datetime, timedelta



# Import seaborn
import seaborn as sns
   
#logs
import logging
logger = logging.getLogger('class')
logger.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)

# debug of the ES connection
#logging.getLogger('elastic_transport.transport').setLevel(logging.INFO)








# --------------------------------------------------------
#       QUERIES
# --------------------------------------------------------





 
 
 


def eq_query_count_docs(term, value):
    """ Elastic search query for a double aggregate query.
    
    This function sends a query to an elastic search server to retrive the doc counts for two field values (fieldname1 and fieldname2), and returns the corresponding pandas DataFrame
    
    
    :param dictionary params: a list of two field names (with the keys fieldname1 and fieldname2) to construct the elastic search query
    
    :returns: a pandas DataFrame which contains the count numbers for each fields in the params variable (hierarchical aggregate)
    :rtype: DataFrame
    """

    clientES = tools.elasticsearch_open_connection()

    #get the number of valid records per day of the week
    resp = clientES.options(
        basic_auth=(myconfig.user, myconfig.password)
    ).count(
        index=myconfig.index_name,
        pretty=True,
        human=True,
        query={
            "bool": {
                "must_not": [
                    {
                        "term": { term: value}
                    }
                ]
            }
        }
    )
    
    return(resp['count'])
    
  
 
 
 
 





# --------------------------------------------------------
#       MAIN
# --------------------------------------------------------

   
    
# executable
if __name__ == "__main__":
    """Executes the script to plot distribs for the traffic
 
    """
    param = {}
    fieldnames = { "fieldname1" : 'extra_infos.phyPayload.macPayload.fhdr.fCtrl.classB', "fieldname2": 'extra_infos.phyPayload.macPayload.fhdr.fCtrl.ack'}
    results_df = tools.elasticsearch_query_count_docs_with_twofields(fieldnames)
    print(results_df)
        

    plot_class_ack_distrib(results_df, fieldnames, "figures/allpackets_class_ack_distrib.pdf")
    
    

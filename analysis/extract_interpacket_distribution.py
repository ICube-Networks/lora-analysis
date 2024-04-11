""" inter packet time distribution extraction.

This scripts extracts from an elasticsearch instance statistics concerning
the list of devAddr, and the corresponding packets (timeseries). A different
script analyzes the distributions.

More precisely, the application first reads the data in data/dataset.parquet,
and load the associated distributions from the disk (parquet format). Then
elastic search is used to read the data for the devAddr not present in the
disk. Thus, the application may not use elastic search at all (except to
extract the list of all the devAddrs)

"""

__authors__ = ("Fabrice Theoleyre")
__contact__ = ("fabrice.theoleyre@cnrs.fr")
__copyright__ = "CNRS"
__date__ = "2023"
__version__= "1.0"





# import the config folder
import sys
sys.path.insert(1, '../config')
sys.path.insert(1, '../tools')

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

   
#logs
import logging
logger_interpkt = logging.getLogger('interpkt_distribution')
logger_interpkt.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)

# system
import signal
import time


# parameters
DEVADDR_COUNT_MAX = 10000000        # max number of devices to support

AGG_OFFSET = 1000                   # offset for the pagination in the aggregatied query
DATE_FORMAT_ELASTICSEARCH = "%Y-%m-%dT%H:%M:%S.%fZ"     # format of the date
QUERY_NB_RESULT = 10000                #  number of results for our elastic search queries
CTRL_C_PRESSED = False              # has ctrl-c been pressed?
FILENAME_DF = 'data/dataset.parquet'# the name of the file to read/write the data frames
FILENAME_DISTRIB = 'data/distrib_'  # the prefix of the filenames for the distribution

      
# --------------------------------------------------------
#       ELASTIC SEARCH QUERIES
# --------------------------------------------------------


def es_query_get_devAddr(clientES):
    """ Elastic query to get the list of unique devAddrs
     
    :param clientES is an active connection to an elastic search server
    
    :returns: a list of all devAddrs
    
    :rtype: list of string
    """


    #Until we have still devAddr to get
    pagination_count = 0
    list_devAddr = []
    while True:
    
        response = clientES.search(
            index=myconfig.index_name,
            size=0,
            #request_timeout=3000,
            pretty=True,
            human=True,
            query=tools.queries.QUERY_DATA_NODUP,
            aggs={
                'devAddr': {
                    "terms": {
                        "field": 'extra_infos.phyPayload.macPayload.fhdr.devAddr.keyword',
                        "size": DEVADDR_COUNT_MAX,
                    },
                    "aggs": {
                    "bucket_sort": {
                        "bucket_sort": {
                            "sort": [{
                                "_key": {
                                    "order": "asc"
                                }
                            }],
                            # "from" and "size" use above terms bucket size. It implements the pagination
                            "from": pagination_count * AGG_OFFSET,
                            "size": AGG_OFFSET
                        }
                    }
                }
                }
            }
        )
        
        # for the next page
        pagination_count = pagination_count + 1
        
        # no more record
        if (len(response["aggregations"]["devAddr"]["buckets"]) == 0):
            logger_interpkt.info("Elastic search: last page for the aggregate query")
            break;
        
        
        # dump the json response for debug (VERY verbose !!)
        logger_interpkt.debug("GET_LIST:" + json.dumps(response.body, sort_keys=True, indent=4))
        
        # add the corresponding devAddr in the list
        for record in response["aggregations"]["devAddr"]["buckets"]:
            list_devAddr.append(record["key"])
  
   
    # end of extraction
    logger_interpkt.info("\t> Found " + str(len(list_devAddr)) + " different devAddrs")
   
    #result
    return(list_devAddr)
 
   

def eq_query_get_interpkt(clientES, devAddr):
    """ Elastic query to get the list of inter packet time for a given devAddr
        
    :param clientES is an active connection to an elastic search server
    
    :param the devAddr to search in the DB.
        
    :returns: a list of all inter packet time
    
    :rtype: list of durations
    """

 
    # get the list of packets for this devAddr
    # -> data packets only (with a devAddr)
    # -> without duplicates
    # -> ordered chronologically (by mqtt_time)
    mqtt_time_min = 0
    while True:
    
        response = clientES.search(
            index=myconfig.index_name,
            size=QUERY_NB_RESULT,
            pretty=True,
            human=True,
            query={
                "bool": {
                    "filter" : [
                        {"match": {"dup_infos.is_duplicate": False}},
                        {"match": {"extra_infos.phyPayload.mhdr.mType": "2"}},
                        {"match": {"extra_infos.phyPayload.macPayload.fhdr.devAddr.keyword": devAddr}},
                    ],
                }
            },
            fields=[
                "mqtt_time",
                "extra_infos.phyPayload.macPayload.fhdr.fCnt",
            ],
            sort=[
                {"mqtt_time" : "asc"}
            ],
            search_after=[mqtt_time_min],
            source = False
        )
            
        #no remaining response
        length = len(response["hits"]["hits"])
        if (length == 0):
            break

        # dump the json response for debug (VERY verbose !!)
        logger_interpkt.debug("GET_LIST:" + json.dumps(response.body, sort_keys=True, indent=4))
      
        pd_distrib = pd.DataFrame({'interpkt_time': [], 'fCnt': []})
        pd_distrib['fCnt'] = int
      
        # computes the inter packet time
        for i in range(0, len(response["hits"]["hits"])  - 1):
            diff = datetime.strptime(response["hits"]["hits"][i+1]["fields"]["mqtt_time"][0], DATE_FORMAT_ELASTICSEARCH) \
                - \
                datetime.strptime(response["hits"]["hits"][i]["fields"]["mqtt_time"][0], DATE_FORMAT_ELASTICSEARCH)


            pd_record_distrib = {
                'interpkt_time': diff.total_seconds(),
                'fCnt': response["hits"]["hits"][i]["fields"]["extra_infos.phyPayload.macPayload.fhdr.fCnt"][0]
            }
            pd_distrib = pd.concat([pd_distrib, pd.DataFrame(data=pd_record_distrib, index=[0])], ignore_index=True)
 
        #stops if we have less than QUERY_SIZE elements, it was the last response
        if (length < QUERY_NB_RESULT):
            break
            
        # next page for the query
        mqtt_time_min = response["hits"]["hits"][length-1]["fields"]["mqtt_time"][0]
       
    logger_interpkt.debug(devAddr + " (list size) -> " + str(pd_distrib.size))

    #save the raw distribution (dataframe in a file)
    save_distrib_to_disk(pd_distrib, devAddr)


    #saves the results in a dataframe
    if pd_distrib.size > 0:
        record = {
            'devAddr': [devAddr,],
            'median_interpkt_time': pd_distrib['interpkt_time'].median(),
            'nb_pkts': [pd_distrib.size + 1,],         # number of packets = length of the list + one
        }
    else:
        record = {
            'devAddr': [devAddr,],
            'median_interpkt_time': [pd.NaT,],
            'nb_pkts': [pd_distrib.size + 1,] ,        # number of packets = length of the list + one
       }
    
    
    return(pd.DataFrame(data=record))


       
      
# --------------------------------------------------------
#       DISK PRESISTENT STORAGE
# --------------------------------------------------------


  
  
  
def load_from_disk(verbose=False):
    """ Load from disk the dataframe (with parquet)
        
    :return pd_stats: the pandas dataframe
    3 columns:
    devAddr: string
    median_interpkt_time: delta_time
    nb_pkt: integer
    
    """
    pd_stats = None

    if  os.path.exists(FILENAME_DF):
        if verbose:
            logger_interpkt.info(" Loading parquet data from " + FILENAME_DF + ":")
            logger_interpkt.info(" > Reading values ....")
            logger_interpkt.info("\tdevAddr\t\tnb_pkts\tDisk\tmedian_interpkt_time")
        pd_stats = pd.read_parquet(FILENAME_DF)
        
        # force some types in the pandaframe
        pd_stats['nb_pkts'] = pd_stats['nb_pkts'].astype('int')
    else:
        logger_interpkt.info(FILENAME_DF + " doesn't exist.")

 
    return(pd_stats)
    

  
def save_to_disk(pd_stats):
    """ Save to disk the dataframe (with parquet)
        
    :param pd_stats: the pandas dataframe
    4 columns:
    devAddr: string
    median_interpkt_time: float (seconds)
    nb_pkt: integer
    """

    
    # savings separately the dataframe without the individual distributions p(arquet format)
    #pd_stats[['devAddr', 'nb_pkts', 'median_interpkt_time']].to_parquet(FILENAME_DF)
    pd_stats.to_parquet(FILENAME_DF)
   
      
 


def load_distrib_from_disk(devAddr, verbose=False):
    """ load each individual distribution from the disk into a dataframe (with parquet)
        
    :param devAddr: the devAddr to read
    
    :returns: a dataframe with the raw distribution (inter packet time + fCnt)
    
    :rtype: pandas dataframe
    
    """
     
    filename = FILENAME_DISTRIB + devAddr + '.parquet'
     
    if os.path.exists(filename) :
       pd_distrib = pd.read_parquet(filename).squeeze()
         
       if verbose:
           logger_interpkt.info("Addr=" + devAddr + " Distrib_length=" + str(pd_distrib.size))
    else:
        logger_interpkt.error(filename + " doesn't exist")
        sys.exit(4)
    
    return(pd_distrib)
    
    
     
def save_distrib_to_disk(pd_distrib, devAddr):
    """ Save to disk a dataframe (with parquet)
        
    :param pd_distrib: the pandas dataframe
    2 columns:
    interpkt_time: float, the inter packet time (in s) with the previous packet
    fCnt: integer, the frame counter of LoRa
    """
     
    filename_distrib = FILENAME_DISTRIB + devAddr + '.parquet'
    
    # if only one packet -> nan, else store the timeseries in individual files
    if pd_distrib.size > 0 :
        pd_distrib.to_parquet(filename_distrib)
 
    else:
        pd.DataFrame([np.nan]).to_parquet(filename_distrib)
  


  
  
  
  
  
# --------------------------------------------------------
#       APPLICATION (INTERRUPTABLE)
# --------------------------------------------------------

 
class Application:
    """ Class to define the application to run (for the analysis)
    We can stop the application with a ctrl-c  since the number of flows is very high
    
    """

    def __init__( self, pd_stats ):
        """ Creation of the app object
        
        """
        signal.signal(signal.SIGINT, lambda signal, frame: self._signal_handler() )
        self.terminated = False
        
        #stats
        self.pd_stats = pd_stats
 

    def _signal_handler( self ):
        """ Ctrl-c has been pressed
        
        """
        self.terminated = True
        
        
  
    def MainLoop( self ):
        """ Ask for all the packets for each devAddr to store info in a pandas dataframe
                    
        """


        clientES = tools.elasticsearch_open_connection()

        #get the list of devaddrs in the elastic search DB
        list_devAddr = es_query_get_devAddr(clientES)
        
        # empty pandas dataframe -> let's create it
        if self.pd_stats is  None:
            self.pd_stats = pd.DataFrame({'devAddr': [], 'median_interpkt_time': [], 'nb_pkts': []})
            

        # remove the devAddr already handled
        else:
            for i in range(0, len(self.pd_stats)) :
                list_devAddr.remove(self.pd_stats.iloc[i]['devAddr'])


        #get the inter packet times for a given devAddr
        logger_interpkt.info("> Reading values ....")
        logger_interpkt.info("\tdevAddr\t\tnb_pkts\t\tmedian_interpkt_time")
        for devAddr in list_devAddr :

            # get the new record for this devAddr
            pd_record = eq_query_get_interpkt(clientES, devAddr)
            logger_interpkt.info("\t" + pd_record['devAddr'][0] + "\t" + str(pd_record['nb_pkts'][0]) + "\t\t" + str(pd_record['median_interpkt_time'][0])   )
            logger_interpkt.debug("memory: "+ str(sys.getsizeof(self.pd_stats) / (1024 * 1024)) + " MB")

            # concantenated to the global pandaframe
            self.pd_stats = pd.concat([self.pd_stats, pd_record], ignore_index=True)
 
            #exit condition
            if self.terminated:
                print("------- Application interrupted ----- ")
                break

        #clean up
        clientES.transport.close()
      
      
      
      
      
# --------------------------------------------------------
#       MAIN
# --------------------------------------------------------

    
# executable
if __name__ == "__main__":
    """Executes the script to extract the distribution of inter packet times from elastic search
 
    """
    
      
    # ---- disk -----
    # load data that is on the disk (already read previously)
    pd_disk = load_from_disk(verbose=True)


    
    # -- elastic search ----
    # extract from elastic search what was not read on the disk
    # encapsulated in a class to be able to stop the computation with a ctrl-c
    app = Application(pd_disk)
    app.MainLoop()
 
                     
    # ---- disk -----
    # save the pandas frame to the disk
    save_to_disk(app.pd_stats)





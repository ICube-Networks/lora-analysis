""" inter packet time distribution analysis .

This scripts extracts from an elasticsearch instance statistics concerning
the list of devAddr, and the corresponding packets (timeseries). Then, it analyzes
the inter packet time distribution

"""

__authors__ = ("Fabrice Theoleyre")
__contact__ = ("fabrice.theolerye@cnrs.fr")
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


# Data science and co
import seaborn as sns
import pandas as pd
   
#logs
import logging
logger_interpkt = logging.getLogger('interpkt_distribution')
logger_interpkt.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)

# system
import signal
import time

# storage
import pyarrow.feather as feather


# parameters
DEVADDR_COUNT_MAX = 10000000        # max number of devices to support
AGG_OFFSET = 1000                   # offset for the pagination in the aggregatied query
DATE_FORMAT_ELASTICSEARCH = "%Y-%m-%dT%H:%M:%S.%fZ"     # format of the date
QUERY_NB_RESULT = 10000                #  number of results for our elastic search queries
CTRL_C_PRESSED = False              # has ctrl-c been pressed?
FILENAME_DF = 'data/dataset.parquet'   # the name of the file to read/write the data frames
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
            logger_interpkt.info("no more page in the aggregate query")
            break;
        
        
        # dump the json response for debug (VERY verbose !!)
        logger_interpkt.debug("GET_LIST:" + json.dumps(response.body, sort_keys=True, indent=4))
        
        # add the corresponding devAddr in the list
        for record in response["aggregations"]["devAddr"]["buckets"]:
            list_devAddr.append(record["key"])
  
   
    # end of extraction
    logger_interpkt.info("Found " + str(len(list_devAddr)) + " different devAddrs")
   
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
    list_interpkt_time = []
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
            ],
            sort=[
                {"mqtt_time" : "asc"}
            ],
            search_after=[mqtt_time_min],
            source = False
        )
            
        #no remaining response
        length = len(response['hits']['hits'])
        if (length == 0):
            break

        # dump the json response for debug (VERY verbose !!)
        logger_interpkt.debug("GET_LIST:" + json.dumps(response.body, sort_keys=True, indent=4))
      
        # computes the inter packet time
        for i in range(0, len(response["hits"]["hits"])  - 1):
            
            list_interpkt_time.append(
                datetime.strptime(response["hits"]["hits"][i+1]["fields"]["mqtt_time"][0], DATE_FORMAT_ELASTICSEARCH) \
                - \
                datetime.strptime(response["hits"]["hits"][i]["fields"]["mqtt_time"][0], DATE_FORMAT_ELASTICSEARCH)
            )
        
        #stops if we have less than QUERY_SIZE elements, it was the last response
        if (length < QUERY_NB_RESULT):
            break
            
        # next page for the query
        mqtt_time_min = response['hits']['hits'][length-1]['fields']['mqtt_time'][0]
       
    #logger_interpkt.info(devAddr + " -> ", list_interpkt_time)
    logger_interpkt.debug(devAddr + " (list size) -> " + str(len(list_interpkt_time)))

    # convert the list into a series
    pd_distrib = pd.Series(list_interpkt_time)
    
    #saves the results
    record = {}
    record['devAddr'] = devAddr
    record['median_interpkt_time'] = pd_distrib.median()
    record['nb_pkts'] = len(list_interpkt_time)

    return(record, pd_distrib)


      
# --------------------------------------------------------
#       PLOTS
# --------------------------------------------------------



def plot_distribution_grid(pd_frame, index, count, nb_cols):
    """ Plot a list of distributions (timeseries) from a pandas dataframe with an ECDF
        
    :param distribution: a pandas dataframe containing a distribution (pandas series) for each row
    
    """
    sns.set()
    sns.set_theme()
    sns.set(font_scale=0.5)

    #a grid of plots
    fig, axs = plt.subplots(ncols=nb_cols, nrows=math.ceil(count/nb_cols))

    #live view of the distributions for each packet
    for g_id in range(index, index+count):
    
        print(str(g_id % nb_cols) + " / " + str(math.floor(g_id / nb_cols)))
        
        g = sns.ecdfplot(
            pd_frame.iloc[g_id]['distribution'].array.seconds,
            ax=axs[math.floor(g_id / nb_cols), g_id % nb_cols]
            
        )
        g.set(xlabel='Inter pkt time (s) / '+str(pd_frame.iloc[g_id]['distribution'].size) + ' pkts', ylabel='Proportion')
        #g.set(xlim=(0, 2000))
        
    plt.tight_layout()
    fig = g.figure.savefig("figures/test.pdf")
    g.figure.clf()
         
         

    
    
    

def plot_distribution_unique(pd_frame):
    """ Plot a distribution (pd dataframe) with an ECDF
        
    :param distribution: a pandas dataframe with the data to plot
    
    """
    sns.set()
    sns.set_theme()
    sns.set(font_scale=1.0)

      
    g = sns.ecdfplot(
        pd_frame / pd.Timedelta(seconds=1)
    )
    g.set(xlabel='Inter pkt time (s)', ylabel='Proportion')
    #g.set(xlim=(0, 2000))
    
    fig = g.figure.savefig("figures/test2.pdf")
    g.figure.clf()
         
      
      
      
# --------------------------------------------------------
#       DISK PRESISTENT STORAGE
# --------------------------------------------------------


def save_to_disk(pd_stats):
    """ Save to disk the dataframe (with parquet)
        
    :param pd_stats: the pandas dataframe
    4 columns:
    devAddr: string
    median_interpkt_time: delta_time
    nb_pkt: integer
    distributions: series
    """

    #save the pandas dataframe into parquet / pickle / feather / XX csv XX
    print("size " + str(len(pd_stats)))
    
    print("-----------")
    #save each distribution in a separated file
    for index in range(0, len(pd_stats)):
        #print(pd_stats.loc[index])
        #print("-----------")
        #print(pd_stats.loc[index].notnull()['distribution'])
        #print("-----------")

        # if the key distribution is not NaN, it means we need to save it (not already on the disk)
        if pd_stats.loc[index].notnull()['distribution'] :
            pd_stats.loc[index]['distribution'].to_frame().to_parquet(FILENAME_DISTRIB + pd_stats.loc[index]['devAddr'] + '.parquet')
            logger_interpkt.info("saving distribution for " + pd_stats.loc[index]['devAddr'])
            print("------------")
        

    pd_stats.loc[:, pd_stats.columns != "distribution"].to_parquet(FILENAME_DF)
     
  
  
  
  
def load_from_disk():
    """ Load from disk the dataframe (with parquet)
        
    :return pd_stats: the pandas dataframe
    3 columns:
    devAddr: string
    median_interpkt_time: delta_time
    nb_pkt: integer
    
    """
    pd_stats = None

    if  os.path.exists(FILENAME_DF):
        logger_interpkt.info(" load parquet data from " + FILENAME_DF)
        pd_stats = pd.read_parquet(FILENAME_DF)
    else:
        logger_interpkt.info(FILENAME_DF + " doesn't exist.")

    return(pd_stats)
     
     
     
     
     
# --------------------------------------------------------
#       APPLICATION (INTERRUPTABLE)
# --------------------------------------------------------

 
class Application:
    """ Class to define the application to run (for the analysis)
    We can stop the application with a ctrl-c  since the number of flows is very high
    
    """

    def __init__( self ):
        signal.signal(signal.SIGINT, lambda signal, frame: self._signal_handler() )
        self.terminated = False
        
        #stats
        self.pd_stats = pd.DataFrame({'devAddr': [], 'median_interpkt_time': [], 'nb_pkts': [], 'distribution': []})
        self.pd_stats['distribution'] = pd.Series(dtype='object')


    def _signal_handler( self ):
        self.terminated = True
        
        
    def handler(signum, frame):
        """ Ctrl-c has been pressed
            
        :param signum: signal id
        
        :param frame
        
        """
        
        print("")
        print("Ctrl-c was pressed. Will stop the program and generate the plots")
        print("")
        CTRL_C_PRESSED = True


    def MainLoop( self ):
        """ Ask for all the packets for each devAddr to store info in a pandas dataframe
                    
        """


        clientES = tools.elasticsearch_open_connection()

        #get the list of devaddrs in the elastic search DB
        list_devAddr = es_query_get_devAddr(clientES)
        
        # remove the devAddr already handled
        for i in range(0, len(self.pd_stats)) :
              list_devAddr.remove(self.pd_stats.iloc[i]['devAddr'])


        #get the inter packet times for a given devAddr
        logger_interpkt.debug("devAddr   median_interpkt_time       nb_pkts      distribution")
        for devAddr in list_devAddr :

            record, distribution = eq_query_get_interpkt(clientES, devAddr)
            record['distribution'] = distribution
            self.pd_stats.loc[len(self.pd_stats.index)] = record
            
            #debug
            logger_interpkt.info(record['devAddr'] + "  " + str(record['median_interpkt_time']) + "     " + str(record['nb_pkts']))
            logger_interpkt.debug("memory: "+ str(sys.getsizeof(self.pd_stats) / (1024 * 1024)) + " MB")
 
            #exit condition
            if self.terminated:
                print("Break it")
                break

        #clean up
        clientES.transport.close()

        #return(self.pd_stats)

      
      
      
# --------------------------------------------------------
#       MAIN
# --------------------------------------------------------

    
# executable
if __name__ == "__main__":
    """Executes the script to analyze the distribution of inter packet times
 
    """
    
    
    # encapsulated in a class to be able to stop the computation with a ctrl-c
    app = Application()
    
    # load data that is on the disk (already read previously)
    pd_disk = load_from_disk()
    if pd_disk is not None:
        app.pd_stats = pd.concat([app.pd_stats, pd_disk])
            
    # read the rest (starting from what has been read on the disk)
    app.MainLoop()
 
    #plots
    if False:
        # plot a grid of distributions (invidividual analysis)
        nb_plots = max(25, app.pd_stats.size)
        plot_distribution_grid(app.pd_stats, index=0, count=nb_plots, nb_cols=5)
        
        # plot the EDCF of the median inter packet time
        plot_distribution_unique(app.pd_stats['median_interpkt_time'])
            
    # save the pandas frame to the disk
    save_to_disk(app.pd_stats)

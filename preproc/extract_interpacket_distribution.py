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
logger_preprocflow = logging.getLogger('interpkt_distribution')
logger_preprocflow.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)

# system
import signal
import time


# parameters
DEVADDR_COUNT_MAX = 10000000        # max number of devices to support

AGG_OFFSET = 1000                   # offset for the pagination in the aggregatied query
DATE_FORMAT_ELASTICSEARCH = "%Y-%m-%dT%H:%M:%S.%fZ"     # format of the date
QUERY_NB_RESULT = 10000             #  number of results for our elastic search queries
CTRL_C_PRESSED = False              # has ctrl-c been pressed?
FILENAME_DF = 'data/dataset.parquet'# the name of the file to read/write the data frames
FILENAME_DISTRIB = 'data/distrib_'  # the prefix of the filenames for the distribution

# conditions for a new flow
DELTA_FCNT_MAX = 10                 # if the counter diff exceeds this value between two consecutive packets -> new flow
DELTA_INTERPKT_TIME_MAX = 3         # if the inter-packet time exceeds DELTA_INTERPKT_TIME_MAX * max_value (in the existing timeseries)
      
# --------------------------------------------------------
#       ELASTIC SEARCH QUERIES
# --------------------------------------------------------


def es_query_get_devAddr():
    """ Elastic query to get the list of unique devAddrs
     
    :param clientES is an active connection to an elastic search server
    
    :returns: a list of all devAddrs
    
    :rtype: list of string
    """

    clientES = tools.elasticsearch_open_connection()

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
                        "devAddr_bucket_sort": {
                            "bucket_sort": {
                                "sort": [
                                    {"_key": {"order": "asc"}}
                                ],
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
            logger_preprocflow.info("Elastic search: last page for the aggregate query")
            break;
        
        
        # dump the json response for debug (VERY verbose !!)
        logger_preprocflow.debug("GET_LIST:" + json.dumps(response.body, sort_keys=True, indent=4))
        
        # add the corresponding devAddr in the list
        for record in response["aggregations"]["devAddr"]["buckets"]:
            list_devAddr.append(record["key"])
  
   
    # end of extraction
    logger_preprocflow.info("\t> Found " + str(len(list_devAddr)) + " different devAddrs")
   
    #result
    return(list_devAddr)
 
   
def pd_create_record(devAddr, flow_id, pd_distrib):



#    logger_preprocflow.debug(devAddr + " (list size) -> " + str(pd_distrib.size))

 

    #save the raw distribution (dataframe in a file)
    #save_distrib_to_disk(pd_distrib, devAddr, flow_id)


    #s new record to add
    if pd_distrib.size > 0:
        record = {
            'devAddr': [devAddr,],
            'flow': flow_id,
            'median_interpkt_time': pd_distrib['interpkt_time'].median(),
            'max_time': pd_distrib['interpkt_time'].max(),
            'min_time': pd_distrib['interpkt_time'].min(),
            'nb_pkts': [pd_distrib.size + 1,],         # number of packets = length of the list + one
        }
    else:
        record = {
            'devAddr': [devAddr,],
            'flow': flow_id,
            'median_interpkt_time': [pd.NaT,],
            'max_time': pd_distrib['interpkt_time'].max(),
            'min_time': pd_distrib['interpkt_time'].min(),
            'nb_pkts': [pd_distrib.size + 1,] ,        # number of packets = length of the list + one
       }

    return(record)



def es_query_get_devAddr_tx(devAddr, mqtt_time_min):
    """ Elastic query to get the list of inter packet time for a given devAddr
        
    :param devAddr: the devADDR to ask for
    
    :param mqtt_time_min: the min mqqt_time in the query
        
    :returns: the elastic search response + the next mqtt_time_min to ask for
    
    :rtype: elastic search json response + string
    """

    clientES = tools.elasticsearch_open_connection()

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
            "_id",
        ],
        sort=[
            {"mqtt_time" : "asc"}
        ],
        search_after=[mqtt_time_min],
        source = False
    )
    
    # dump the json response for debug (VERY verbose !!)
    logger_preprocflow.debug("GET_LIST:" + json.dumps(response.body, sort_keys=True, indent=4))

    # next page for the query
    length = len(response["hits"]["hits"])
    mqtt_time_min = response["hits"]["hits"][length-1]["fields"]["mqtt_time"][0]
 
    return(response, mqtt_time_min)
    
    



def eq_query_get_interpkt(devAddr):
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
    flow_id = 0
    test = 0
    while True:
    
        #tx the elastic search query to the server
        response, mqtt_time_min = es_query_get_devAddr_tx(devAddr, mqtt_time_min)
            
        #no remaining response
        length = len(response["hits"]["hits"])
        if (length == 0):
            break


        #pointer to the last packet of the flow (initially, packet 0)
        pointer_last_pkt_of_the_flow = 0

        # computes the inter packet time
        for i in range(1, len(response["hits"]["hits"])  - 1):
            
            #create the dataframe if it doesn't exist yet
            if 'pd_distrib' not in locals():
                print("dataframe creation")
                pd_distrib = pd.DataFrame({'interpkt_time': [], 'fCnt': []})
                pd_distrib['fCnt'] = int
                test = test + 1


            diff = datetime.strptime(response["hits"]["hits"][i]["fields"]["mqtt_time"][0], DATE_FORMAT_ELASTICSEARCH) \
                - \
                datetime.strptime(response["hits"]["hits"][pointer_last_pkt_of_the_flow]["fields"]["mqtt_time"][0], DATE_FORMAT_ELASTICSEARCH)
            
            #frame counter difference
            fCnt_diff = response["hits"]["hits"][i]["fields"]["extra_infos.phyPayload.macPayload.fhdr.fCnt"][0] - response["hits"]["hits"][pointer_last_pkt_of_the_flow]["fields"]["extra_infos.phyPayload.macPayload.fhdr.fCnt"][0]
            
            
            print("fCnt diff: " + str(fCnt_diff) + ' time ' + str(diff.total_seconds()) + " _id " +
                        str(response["hits"]["hits"][i]["fields"]["_id"][0]),
                        str(response["hits"]["hits"][pointer_last_pkt_of_the_flow]["fields"]["_id"][0])
                        )

            # same flow ?
            if (fCnt_diff < DELTA_FCNT_MAX):
                if (i-1 != pointer_last_pkt_of_the_flow):
                    print("ecrase le gap")
                    
                #create a record for this correct packet
                pd_record_distrib = {
                    'interpkt_time': diff.total_seconds(),
                    'fCnt': fCnt_diff,
                }
                # my reference is this new packet
                pointer_last_pkt_of_the_flow = i
                
                #and insert the new record
                pd_distrib = pd.concat([pd_distrib, pd.DataFrame(data=pd_record_distrib, index=[0])], ignore_index=True)
            
 
            # flush if it corresponds to a new flow
            # counter diff > threshold value
            # inter packet time > max inter packet time * X
            if (fCnt_diff > DELTA_FCNT_MAX) and (diff.total_seconds() > DELTA_INTERPKT_TIME_MAX * (pd_distrib['interpkt_time'].max())) :
                print("stop the previous flow, finished time diff " + str(diff.total_seconds()) + " > " + str(pd_distrib['interpkt_time'].max()) + ' fnct diff ' + str(fCnt_diff))
                
                
                record = pd_create_record(devAddr=devAddr, flow_id=flow_id, pd_distrib=pd_distrib)
                if 'pd_these_flows' not in locals():
                    pd_these_flows = pd.DataFrame(data=record)
                else:
                    pd_these_flows = pd.concat([pd_these_flows, pd.DataFrame(data=record)], ignore_index=True)
                
                flow_id = flow_id + 1       # next flow
                del pd_distrib              # remove the occurence to the dataframe
               
                #back to the pointer where it diverged
                i = pointer_last_pkt_of_the_flow + 1
               
                PROBLEM: continue ensuite à boucler alors qu'il devrait considéerer qu'ils s'agit d'un nouveau flot (la diff de compteur devrait rebaisser, alors qu'elle continue à s'accumuler)
               
 
            if test > 9:
                exit(4)
            
    
        #stops if we have less than QUERY_SIZE elements, it was the last response
        if (length < QUERY_NB_RESULT):
            break
   
   
   
    #flush if still one pending record
    if (len(pd_distrib) > 0):
        record = pd_create_record(devAddr=devAddr, flow_id=flow_id, pd_distrib=pd_distrib)
        if 'pd_these_flows' not in locals():
            pd_these_flows = pd.DataFrame(data=record)
        else:
            pd_these_flows = pd.concat([pd_these_flows, pd.DataFrame(data=record)], ignore_index=True)

    
    return(pd_these_flows)


       
      
# --------------------------------------------------------
#       DISK PRESISTENT STORAGE
# --------------------------------------------------------


  
  
  
def load_from_disk(verbose=False):
    """ Load from disk the dataframe (with parquet)
        
    :return pd_all_flows: the pandas dataframe
    3 columns:
    devAddr: string
    median_interpkt_time: delta_time
    nb_pkt: integer
    
    """
    pd_all_flows = None

    if  os.path.exists(FILENAME_DF):
        if verbose:
            logger_preprocflow.info(" Loading parquet data from " + FILENAME_DF + ":")
            logger_preprocflow.info(" > Reading values ....")
            logger_preprocflow.info("\tdevAddr\t\tnb_pkts\tDisk\tmedian_interpkt_time")
        pd_all_flows = pd.read_parquet(FILENAME_DF)
        
        # force some types in the pandaframe
        pd_all_flows['nb_pkts'] = pd_all_flows['nb_pkts'].astype('int')
    else:
        logger_preprocflow.info(FILENAME_DF + " doesn't exist.")

 
    return(pd_all_flows)
    

  
def save_to_disk(pd_all_flows):
    """ Save to disk the dataframe (with parquet)
        
    :param pd_all_flows: the pandas dataframe
    4 columns:
    devAddr: string
    median_interpkt_time: float (seconds)
    nb_pkt: integer
    """

    
    # savings separately the dataframe without the individual distributions p(arquet format)
    #pd_all_flows[['devAddr', 'nb_pkts', 'median_interpkt_time']].to_parquet(FILENAME_DF)
    pd_all_flows.to_parquet(FILENAME_DF)
   
      
 


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
           logger_preprocflow.info("Addr=" + devAddr + " Distrib_length=" + str(pd_distrib.size))
    else:
        logger_preprocflow.error(filename + " doesn't exist")
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

    def __init__( self, pd_all_flows ):
        """ Creation of the app object
        
        """
        signal.signal(signal.SIGINT, lambda signal, frame: self._signal_handler() )
        self.terminated = False
        
        #stats
        self.pd_all_flows = pd_all_flows
 

    def _signal_handler( self ):
        """ Ctrl-c has been pressed
        
        """
        self.terminated = True
        
        
  
    def MainLoop( self ):
        """ Ask for all the packets for each devAddr to store info in a pandas dataframe
                    
        """


   
        #get the list of devaddrs in the elastic search DB
        list_devAddr_pending = es_query_get_devAddr()
        
        # empty pandas dataframe -> let's create it
        if self.pd_all_flows is  None:
            self.pd_all_flows = pd.DataFrame({'devAddr': [], 'flow_id': [], 'median_interpkt_time': [], 'max_time': [], 'min_time': [], 'nb_pkts': []})
            

        # remove from the pending list the devAddr already handled
        else:
            for i in range(0, len(self.pd_all_flows)) :
                list_devAddr_pending.remove(self.pd_all_flows.iloc[i]['devAddr'])


        #get the inter packet times for a given devAddr
        logger_preprocflow.info("> Reading values ....")
        logger_preprocflow.info("\tdevAddr\t\tnb_pkts\t\tmedian_interpkt_time\tmin\tmax")
        for devAddr in list_devAddr_pending :

            # get the new record(s) for this devAddr (one record per flow)
            pd_records = eq_query_get_interpkt(devAddr)
                   
            # concatenation to the global pandaframe
            self.pd_all_flows = pd.concat([self.pd_all_flows, pd_records], ignore_index=True)

            #logs
            logger_preprocflow.info("\t" + pd_record['devAddr'][0] + "\t" + str(pd_record['nb_pkts'][0]) + "\t\t" + str(pd_record['median_interpkt_time'][0])   )
            logger_preprocflow.debug("memory: "+ str(sys.getsizeof(self.pd_all_flows) / (1024 * 1024)) + " MB")

 
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
    save_to_disk(app.pd_all_flows)





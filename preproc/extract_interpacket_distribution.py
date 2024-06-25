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
QUERY_NB_RESULT = 10000              #  number of results for our elastic search queries
CTRL_C_PRESSED = False              # has ctrl-c been pressed?
FILENAME_DF = myconfig.directory_data+'/dataset.parquet'# the name of the file to read/write the data frames
FILENAME_DISTRIB = myconfig.directory_data+'/distrib_'  # the prefix of the filenames for the distribution

# conditions for a new flow
DELTA_FCNT_ABS_MAX = 30              # absolute diff: if the counter diff exceeds DELTA
DELTA_INTERPKT_ABS_TIME_MAX = 604800000  # if the inter-packet time exceeds 1 day (epoch in ms)
DELTA_INTERPKT_REL_TIME_MAX = 3      # if the inter-packet time exceeds DELTA * max

#


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
            logger_preprocflow.debug("Elastic search: last page for the aggregate query")
            break;
        
        
        # dump the json response for debug (VERY verbose !!)
        #logger_preprocflow.debug("GET_LIST:" + json.dumps(response.body, sort_keys=True, indent=4))
        
        # add the corresponding devAddr in the list
        for record in response["aggregations"]["devAddr"]["buckets"]:
            list_devAddr.append(record["key"])
  
   
    # end of extraction
    logger_preprocflow.info("\t> Found " + str(len(list_devAddr)) + " different devAddrs")
   
    #result
    return(list_devAddr)
 
   
#create a record for this flow (and save its distribution on the disk)
def pd_save_record(devAddr, fCnt_1st, fCnt_last, time_1st, time_last, pd_distrib):

    #logger_preprocflow.debug(devAddr + " (list size) -> " + str(pd_distrib['interpkt_time'].size))

    #save the raw distribution (dataframe in a file)
    save_distrib_to_disk(pd_distrib, devAddr, time_1st)
    
    # new record to add
    if pd_distrib.size > 0:
        record = {
            'devAddr': [devAddr,],
            'fCnt_1st': fCnt_1st,
            'fCnt_last': fCnt_last,
            'time_1st': time_1st,
            'time_last': time_last,
            'median_fCnt_diff': pd_distrib['fCnt_diff'].median(),
            'max_fCnt_diff': pd_distrib['fCnt_diff'].max(),
            'median_interpkt_time': pd_distrib['interpkt_time'].median(),
            'max_time': pd_distrib['interpkt_time'].max(),
            'min_time': pd_distrib['interpkt_time'].min(),
            'nb_pkts': [pd_distrib['interpkt_time'].size + 1,],         # number of packets = length of the list + one
        }
    else:
        record = {
            'devAddr': [devAddr,],
            'fCnt_1st': fCnt_1st,
            'fCnt_last': fCnt_last,
            'time_1st': time_1st,
            'time_last': time_last,
            'median_interpkt_time': [pd.NaT,],
            'max_fCnt_diff': [pd.NaT,],
            'median_fCnt_diff': [pd.NaT,],
            'max_time': [pd.NaT,],
            'min_time': [pd.NaT,],
            'nb_pkts': [pd_distrib['interpkt_time'].size + 1,] ,        # number of packets = length of the list + one
       }
       
    
    del(pd_distrib)

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
 #       runtime_mappings= {
 #           "mqtt_time_epoch_seconds": {
 #               "type": "keyword",
 #               "script": {
 #                   "source": "ZonedDateTime zdt = ZonedDateTime.parse(doc['mqtt_time'].value.toString()); emit(zdt.toEpochMilli().toString())"
 #               }
 #           }
 #       },
        fields=[
            "mqtt_time",
            "extra_infos.phyPayload.macPayload.fhdr.fCnt",
  #          "mqtt_time_epoch_seconds",
            "_id",
            "phyPayload",
        ],
        sort=[
            {"mqtt_time" : "asc"}
        ],
        search_after=[mqtt_time_min],
        source = False
    )
    
    # dump the json response for debug (VERY verbose !!)
    #logger_preprocflow.debug("GET_LIST:" + json.dumps(response.body, sort_keys=True, indent=4))

    # next page for the query
    length = len(response["hits"]["hits"])
    mqtt_time_min = response["hits"]["hits"][length-1]["fields"]["mqtt_time"][0]
 
    return(response, mqtt_time_min)
    
    



def eq_query_get_interpkt(devAddr):
    """ Elastic query to get the list of inter packet time for a given devAddr
        
    :param the devAddr to search in the DB.
        
    :returns: a list of all inter packet time
    
    :rtype: list of durations
    """

 
    # get the list of packets for this devAddr
    # -> data packets only (with a devAddr)
    # -> without duplicates
    # -> ordered chronologically (by mqtt_time)
    mqtt_time_min = 0
     
    #list of flows for this devAddr
    flows_for_thisDevAddr = []
    test = 0
    
    #for all the packets generated with this devAddr
    while True:
    
        #tx the elastic search query to the server
        response, mqtt_time_min = es_query_get_devAddr_tx(devAddr, mqtt_time_min)
        logger_preprocflow.debug("New Elastic Search query (" + str(QUERY_NB_RESULT) + " records at most)")
            
        #no remaining response
        length = len(response["hits"]["hits"])
        if (length == 0):
            break

        # computes the inter packet time
        for i in range(1, len(response["hits"]["hits"])  - 1):
            found = False
                        
            current_packet_data = response["hits"]["hits"][i]
            time_current = current_packet_data["sort"][0]
            fCnt_current = current_packet_data["fields"]["extra_infos.phyPayload.macPayload.fhdr.fCnt"][0]

            #search for an active flow to which the current packet may correspond
            for flow in flows_for_thisDevAddr:
                time_difference = time_current - flow['epochtime_last']
                
                # is it same flow ?
                # 1nd condition: the time between two packets does not exceed a given DELTA value
                #the sorting field is the mqtt_time (in epoch format)
                #diff_time = response["hits"]["hits"][i]["sort"][0] - flow['time_last']
                if (time_difference <=  flow['interpkttime_max']):
                  
                    #frame counter difference
                    fCnt_difference = fCnt_current - flow['fCnt_last']
                   
                    # 2nd condition: the different for the sequence number does not exceed a given DELTA value
                    if  1 <= fCnt_difference < DELTA_FCNT_ABS_MAX:

                        # ok, this packet corresponds to the flow
                        found = True
                                          
                        # update the current fcnt value for this flow (this is my new reference for this flow)
                        flow['fCnt_last'] = max(flow['fCnt_last'], fCnt_current)
                        flow['epochtime_last'] = time_current
                        flow['interpkttime_max'] = max(flow['interpkttime_max'], time_difference * DELTA_INTERPKT_REL_TIME_MAX)
                        flow['time_last'] = datetime.strptime(response["hits"]["hits"][i]["fields"]["mqtt_time"][0], DATE_FORMAT_ELASTICSEARCH)

                        # create a pandas record at the end of the distrib
                        flow['pd_distrib'].loc[len(flow['pd_distrib'].index)] = [
                            time_difference,
                            fCnt_difference,
                            fCnt_current,
                            datetime.strptime(current_packet_data["fields"]["mqtt_time"][0], DATE_FORMAT_ELASTICSEARCH),
                            current_packet_data["fields"]["phyPayload"][0],
                            test,
                        ]
                        test = test +1
                   

                        #if test > 200:
                        #    exit(2)
                        break  # exit loop since packet corresponds to the flow, no need to further iterate.
            

            
            #no flow exists -> create a new one for this devAddr
            if found is False:
                
                record = {
                    'interpkt_time' : [0],
                    'fCnt_diff' : [0],
                    'fCnt' : [fCnt_current],
                    'mqtt_time' : [datetime.strptime(response["hits"]["hits"][i]["fields"]["mqtt_time"][0], DATE_FORMAT_ELASTICSEARCH)],
                    'phyPayload' : [response["hits"]["hits"][i]["fields"]["phyPayload"][0]],
                    'test': [test],
                }
                test = test + 1
                
                # a new flow is created
                flows_for_thisDevAddr.append({
                    'fCnt_1st': fCnt_current,
                    'fCnt_last': fCnt_current,
                    'time_1st': datetime.strptime(response["hits"]["hits"][i]["fields"]["mqtt_time"][0], DATE_FORMAT_ELASTICSEARCH),
                    'time_last': datetime.strptime(response["hits"]["hits"][i]["fields"]["mqtt_time"][0], DATE_FORMAT_ELASTICSEARCH),
                    'epochtime_last': time_current,
                    'interpkttime_max': DELTA_INTERPKT_ABS_TIME_MAX,
                    'pd_distrib': pd.DataFrame(data=record),
                    #pd.DataFrame({'interpkt_time': [], 'fCnt_diff': [], 'fCnt': [],  'mqtt_time': [], 'phyPayload': [], 'test':[]}),
                })
            
                
                #force the type of the column (int) for fCnt
                #flows_for_thisDevAddr[-1]['pd_distrib']['fCnt'] = int
                #flows_for_thisDevAddr[-1]['pd_distrib']['fCnt_diff'] = int

    
        #stops if we have less than QUERY_SIZE elements, it was the last response
        if (length < QUERY_NB_RESULT):
            break

   
    #we must now flush all the distributions in pd_these_flows
    for flow in flows_for_thisDevAddr:
        
        #flush if still one pending record
        if (len(flow['pd_distrib']) > 0):
        
       
            record = pd_save_record(devAddr=devAddr, fCnt_1st=flow['fCnt_1st'], fCnt_last=flow['fCnt_last'],  time_1st=flow['time_1st'], time_last=flow['time_last'], pd_distrib=flow['pd_distrib'])
            
            #print( "size=" + str(flow['pd_distrib']['interpkt_time'].size + 1) + " nbPkts="+ str(record['nb_pkts'])+ " fCnt_1st="+ str(record['fCnt_1st'])+ " devAddr="+ str(record['devAddr']))
 
            
            if 'pd_these_flows' not in locals():
                pd_these_flows = pd.DataFrame(data=record)
            else:
                pd_these_flows = pd.concat([pd_these_flows, pd.DataFrame(data=record)], ignore_index=True)
    
    if 'pd_these_flows' not in locals():
        logger_preprocflow.error("No flow for the devAddr" + str(devAddr))
            
      
        return(None)

    return(pd_these_flows)


       
      
# --------------------------------------------------------
#       DISK PERSISTENT STORAGE
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
           
    logger_preprocflow.info("Loading parquet data from " + FILENAME_DF + ":")
    logger_preprocflow.info("\t> Reading values ....")

    if  os.path.exists(FILENAME_DF):
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
   
      
 


def load_distribs_forDevAddr_from_disk(pd_all_flows, devAddr, verbose=False):
    """ load a list of individual distribution from the disk into a dataframe (with parquet)
        
    :param devAddr: the devAddr to read
    
    :returns: a list of dataframes with the raw distribution (inter packet time + fCnt + etc.)
    
    :rtype: a list of dataframe (be careful, several flows may be included, since several flows can be associated to the same devAddr)
    
    """
     
    
    pd_distrib = []
    #get all the mqtt_time for theis devAddr
    for time_1st in pd_all_flows[pd_all_flows['devAddr']== devAddr]['time_1st']:
    
        filename_distrib = FILENAME_DISTRIB + devAddr + '_' + str(time_1st) + '.parquet'
        pd_distrib.append(pd.read_parquet(filename_distrib))
 
        if verbose:
           logger_preprocflow.info("Addr=" + devAddr + " Distrib_length=" + str(pd_distrib['interpkt_time'].size) + " filename=" + filename)
 
    
    return(pd_distrib)
    
    
def load_distribs_forDevAddr_and_time_1st_from_disk(devAddr, time_1st, verbose=False):
    """ load one distribution from the disk into a dataframe (with parquet)
        
    :param devAddr: the devAddr to read
    
    :param time_1st: the mqtt_time (of the first packet of the flow) to read

    :returns: the raw distribution (inter packet time + fCnt + etc.)
    
    :rtype: dataframe
    
    """
     
    filename_distrib = FILENAME_DISTRIB + devAddr + '_' + str(time_1st) + '.parquet'
    pd_distrib = pd.read_parquet(filename_distrib)
    logger_preprocflow.debug("Addr=" + devAddr + " Distrib_length=" + str(pd_distrib['interpkt_time'].size) + " filename=" + filename_distrib)
    
     
    return(pd_distrib)



     
def save_distrib_to_disk(pd_distrib, devAddr, time_1st):
    """ Save to disk a dataframe (with parquet)
        
    :param pd_distrib: the pandas dataframe to store
    
    : param devAddr: string, the devAddr of ths flow
    
    : param time_1st: integer, the mqtt time of the first packet of the flow
    """
     
    # store the timeseries in individual files
    filename_distrib = FILENAME_DISTRIB + devAddr + '_' + str(time_1st) + '.parquet'
    logger_preprocflow.debug("Addr=" + devAddr + " Distrib_length=" + str(pd_distrib['interpkt_time'].size) + " filename=" + filename_distrib)
    pd_distrib.to_parquet(filename_distrib)
 
 
  

  
  
  
  
  
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
        
        # empty pandas dataframe (none read from the disk) -> let's create it
        if self.pd_all_flows is  None:
            self.pd_all_flows = pd.DataFrame({'devAddr': [], 'fCnt_1st': [], 'fCnt_last': [], 'time_1st': [], 'time_last': [], 'median_interpkt_time': [], 'max_time': [], 'min_time': [], 'nb_pkts': []})
            

        # Else, some have already been extracted from the disk
        # remove from the pending list the devAddr already handled
        # NB: a devAddr may correspond to multiple flows. But when a devAddr is processed, all the corresponding flows are also processed
        else:
            devAddr_proc = self.pd_all_flows['devAddr'].drop_duplicates()
          
            logger_preprocflow.info(str(len(devAddr_proc)) + " devAddr already processed and saved in local:")
            logger_preprocflow.info("\tdevAddr\t\tNb flows")
           
            for devAddr in devAddr_proc:
                list_devAddr_pending.remove(devAddr)
                
                if (logger_preprocflow.getEffectiveLevel() >= logging.DEBUG):
                    pd_records = load_distribs_forDevAddr_from_disk(self.pd_all_flows, devAddr, verbose=False)
                    logger_preprocflow.info("\t" + devAddr + "\t" + str(len(pd_records))  )
                    logger_preprocflow.debug("memory: "+ str(sys.getsizeof(self.pd_all_flows) / (1024 * 1024)) + " MB")


        #get the inter packet times for a given devAddr
        logger_preprocflow.info("> Reading new values in Elastic Search ....")
        logger_preprocflow.info("\tdevAddr\t\tNb flows")
        for devAddr in list_devAddr_pending :

            # get the new record(s) for this devAddr (one record per flow)
            pd_records = eq_query_get_interpkt(devAddr)
            
            # concatenation to the global pandaframe
            if pd_records is not None :
                if self.pd_all_flows.empty is True:
                    self.pd_all_flows = pd_records
                else:
                    self.pd_all_flows = pd.concat([self.pd_all_flows, pd_records], ignore_index=True)

                #logs
                logger_preprocflow.info("\t" + devAddr + "\t" + str(pd_records.shape[0])  )
                logger_preprocflow.debug("memory: "+ str(sys.getsizeof(self.pd_all_flows) / (1024 * 1024)) + " MB")
            else:
                logger_preprocflow.info("\t" + devAddr + "\t0" )


 
            #exit condition
            if self.terminated:
                print("------- Application interrupted ----- ")
                break
      
      
      
      
      
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




def obsolete():
    print(app.pd_all_flows)
    pd_records = load_distribs_forDevAddr_from_disk(app.pd_all_flows, "000173b7", verbose=False)
    boug = 0
    for record in pd_records:
        print("-----------------------------------------------------")
        print(record)
        print("+++ " + str(boug) + " +++++++++")
        boug = boug + 1
    
    
    exit(1)


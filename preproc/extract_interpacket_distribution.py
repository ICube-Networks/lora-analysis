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

# debug of the ES connection
#logging.getLogger('elastic_transport.transport').setLevel(logging.INFO)

# system
import signal
import time


# parameters
DEVADDR_COUNT_MAX = 10000000        # max number of devices to support

AGG_OFFSET = 100000                   # offset for the pagination in the aggregated query
DATE_FORMAT_ELASTICSEARCH = "%Y-%m-%dT%H:%M:%S.%fZ"     # format of the date
CTRL_C_PRESSED = False              # has ctrl-c been pressed?
FILENAME_DF = myconfig.directory_data+'/dataset.parquet'# the name of the file to read/write the data frames
FILENAME_DISTRIB = myconfig.directory_data+'/distrib_'  # the prefix of the filenames for the distribution

# conditions for a new flow
DELTA_FCNT_ABS_MAX = 10000              # absolute diff: if the counter diff exceeds DELTA
DELTA_INTERPKT_ABS_TIME_MAX = 604800000  # 7 days




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
        #print(pagination_count)
        
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
 
   
#create a record for this flow
def extract_flow_record(devAddr, fCnt_1st, fCnt_last, time_1st, time_last, pd_distrib):
   
    # new record to add
    if pd_distrib.size > 0:
        record = {
            'devAddr': [devAddr,],
            'fCnt_1st': fCnt_1st,
            'fCnt_last': fCnt_last,
            'time_1st': time_1st,
            'time_last': time_last,
            'mean_fCnt_diff': pd_distrib['fCnt_diff'].mean(),
            'median_fCnt_diff': pd_distrib['fCnt_diff'].median(),
            'max_fCnt_diff': pd_distrib['fCnt_diff'].max(),
            'nb_duplicates': pd_distrib['nb_duplicates'].sum(),
            'median_interpkt_time_ms': pd_distrib['interpkt_time_ms'].median(),
            'max_interpkt_time_ms': pd_distrib['interpkt_time_ms'].max(),
            'min_interpkt_time_ms': pd_distrib['interpkt_time_ms'].min(),
            'nb_pkts': [pd_distrib['interpkt_time_ms'].size,],         # number of packets = length of the list
        }
    else:
        record = {
            'devAddr': [devAddr,],
            'fCnt_1st': fCnt_1st,
            'fCnt_last': fCnt_last,
            'time_1st': time_1st,
            'time_last': time_last,
            'max_fCnt_diff': [pd.NaT,],
            'mean_fCnt_diff': [pd.NaT,],
            'median_fCnt_diff': [pd.NaT,],
            'nb_duplicates': pd_distrib['nb_duplicates'].sum(),
            'median_interpkt_time_ms': [pd.NaT,],
            'max_interpkt_time_ms': [pd.NaT,],
            'min_interpkt_time_ms': [pd.NaT,],
            'nb_pkts': [pd_distrib['interpkt_time_ms'].size,] ,        # number of packets = length of the list
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
        size=tools.queries.QUERY_NB_RESULT,
        pretty=True,
        human=True,
        query={
            "bool": {
                "filter" : [
                    
                    #{"term": {"dup_infos.is_duplicate": False}},
                    {"term": {"extra_infos.phyPayload.mhdr.mType": "2"}},
                    {"term": {"extra_infos.phyPayload.macPayload.fhdr.devAddr.keyword": devAddr}},
                ],
                "must": [
                    { "exists": { "field": "dup_infos"  }  }
                ]
            }
        },
        fields=[
            "mqtt_time",
            "extra_infos.phyPayload.macPayload.fhdr.fCnt",
            "_id",
            "phyPayload",
            "dup_infos.copy_of",
            "dup_infos.is_duplicate",
            "txInfo.loRaModulationInfo.spreadingFactor"
        ],
        sort=[
            "mqtt_time",
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
     
    #list of flows for this devAddr: empty (we will add a flow when we detect a new one)
    flows_for_thisDevAddr = []
    
    #for all the packets generated with this devAddr
    while True:
    
        #send the elastic search query to the server
        response, mqtt_time_min = es_query_get_devAddr_tx(devAddr, mqtt_time_min)
        logger_preprocflow.debug("New Elastic Search query (" + str(tools.queries.QUERY_NB_RESULT) + " records at most)")
        
        #no remaining response
        length = len(response["hits"])
                  
        if (length == 0):
            logger_preprocflow.error("No packet matches for devAddr " + devAddr)
            break

        # identification of the flows for all the packets
        for i in range(0, len(response["hits"]["hits"])):
            found = False
            current_packet_data = response["hits"]["hits"][i]
         
            # If duplicate, search the flow of the original packet
            # the original packet has already been processed since duplicates are packets received *later*
            if current_packet_data["fields"]["dup_infos.is_duplicate"][0] is True:
                for flow in flows_for_thisDevAddr:
                
                    # search for the row with this _id in the corresponding flow
                    row_index = flow['pd_distrib'][ flow['pd_distrib']['_id'] == current_packet_data["fields"]["dup_infos.copy_of"][0]].index
                    
                    # one row has been found, increment the corresponding nb of duplicates
                    if row_index.size > 0 :
                        #if current_packet_data["fields"]["dup_infos.copy_of"][0] not in flow['pd_distrib']['_id'].values :
                        #    logger_preprocflow.error("No packet match while we have a row index!")
                        
                        flow['pd_distrib'].loc[row_index, 'nb_duplicates'] += 1
                        found = True
                        break
                        
                # bug
                if found is False:
                    logger_preprocflow.error("No packet matches this duplicate " + current_packet_data["fields"]["_id"] + " copy of " + current_packet_data["fields"]["dup_infos.copy_of"])
                # next packet, this duplicated packet is processed
                continue
                
            # -- The rest of this loop corresponds to a non duplicated packet --
            
            #info on the packet to handle
            time_current = current_packet_data["sort"][0]
            fCnt_current = current_packet_data["fields"]["extra_infos.phyPayload.macPayload.fhdr.fCnt"][0]

            #search for an active flow to which the current packet may correspond
            for flow in flows_for_thisDevAddr:
                time_difference_ms = time_current - flow['epochtime_last']      # epoch time = nb of seconds since 1970
                fCnt_difference = fCnt_current - flow['fCnt_last']
                
   
                # is it same flow ?
                # 1st condition: the difference for the sequence number does not exceed a given DELTA value
                # 2nd condition: the time between two packets does not exceed the max inter packet time X the max nb of missed packets
                # 3rd condition: the time difference between two packets does not exceed MAX (= one week)
                if (1 <= fCnt_difference < DELTA_FCNT_ABS_MAX) \
                and ((flow['pd_distrib']['interpkt_time_ms'].max() == 0) or (flow['pd_distrib']['interpkt_time_ms'].max() * 2 >= time_difference_ms / fCnt_difference))\
                and (0 < time_difference_ms < DELTA_INTERPKT_ABS_TIME_MAX) :
                
                    # ok, this packet corresponds to the flow
                    found = True
                                      
                    # update info of this flow
                    flow['epochtime_last'] = time_current
                    flow['time_last'] = datetime.strptime(tools.time.fixMicroseconds(response["hits"]["hits"][i]["fields"]["mqtt_time"][0]), DATE_FORMAT_ELASTICSEARCH)
                    flow['fCnt_last'] = fCnt_current

                    # create a pandas record at the end of the distrib
                    flow['pd_distrib'].loc[len(flow['pd_distrib'].index)] = [
                        time_difference_ms / fCnt_difference,      # normalize the interpacket time by the number of frames I've missed (fnct_diff)
                        fCnt_difference,
                        fCnt_current,
                        current_packet_data["fields"]["txInfo.loRaModulationInfo.spreadingFactor"][0],
                        datetime.strptime(tools.time.fixMicroseconds(current_packet_data["fields"]["mqtt_time"][0]), DATE_FORMAT_ELASTICSEARCH),
                        current_packet_data["fields"]["phyPayload"][0],
                        current_packet_data["fields"]["_id"][0],
                        0,                                        # not a duplicate
                    ]

                    break  # exit loop since packet corresponds to the flow, no need to further iterate.
            

            
            #no flow exists -> create a new one for this devAddr
            if found is False:
                try:
                    record = {
                        'interpkt_time_ms' : [0],
                        'fCnt_diff' : [0],
                        'fCnt' : [fCnt_current],
                        'SF' : [current_packet_data["fields"]["txInfo.loRaModulationInfo.spreadingFactor"][0]],
                        'mqtt_time' : [datetime.strptime(tools.time.fixMicroseconds(response["hits"]["hits"][i]["fields"]["mqtt_time"][0]), DATE_FORMAT_ELASTICSEARCH)],
                        'phyPayload' : [response["hits"]["hits"][i]["fields"]["phyPayload"][0]],
                        '_id' : [current_packet_data["fields"]["_id"][0]],        #new flow, so cannot be a duplicated packet
                        'nb_duplicates' : [0]                                     #same reason
                    }
                    
                    # a new flow is created
                    flows_for_thisDevAddr.append({
                        'fCnt_1st': fCnt_current,
                        'fCnt_last': fCnt_current,
                        'time_1st': datetime.strptime(tools.time.fixMicroseconds(response["hits"]["hits"][i]["fields"]["mqtt_time"][0]), DATE_FORMAT_ELASTICSEARCH),
                        'time_last': datetime.strptime(tools.time.fixMicroseconds(response["hits"]["hits"][i]["fields"]["mqtt_time"][0]), DATE_FORMAT_ELASTICSEARCH),
                        'epochtime_last': time_current,
                        'pd_distrib': pd.DataFrame(data=record)
                    })
                except Exception as e :
                    logger_preprocflow.critical("An error occured when parsing an ES response -> "+ str(e))
                    logger_preprocflow.critical("response:")
                    logger_preprocflow.critical(response)
                    logger_preprocflow.critical("id="+ response[index]['_id'])
                    exit(7)
                    
#110c5a7a
    
        #stops if we have less than QUERY_SIZE elements, it was the last response
        if (length < tools.queries.QUERY_NB_RESULT):
            break

    #all flows must be saved (or more precisely, their distribution)
    for flow in flows_for_thisDevAddr:
 
        #print(flow['pd_distrib'].to_string())
        
        #save the raw distribution (dataframe in a file)
        save_distrib_to_disk(pd_distrib=flow['pd_distrib'], devAddr=devAddr, time_1st=flow['time_1st'])
        

        #extract the summarized record only
        record_summary = extract_flow_record(devAddr=devAddr, fCnt_1st=flow['fCnt_1st'], fCnt_last=flow['fCnt_last'],  time_1st=flow['time_1st'], time_last=flow['time_last'], pd_distrib=flow['pd_distrib'])
          
        # the individual values are not anymore useful
        del(flow['pd_distrib'])

         
        #add this flow to the list of flows for this devAddr
        if 'pd_these_flows' not in locals():
            pd_these_flows = pd.DataFrame(data=record_summary)
        else:
            pd_these_flows = pd.concat([pd_these_flows, pd.DataFrame(data=record_summary)], ignore_index=True)
  

    if 'pd_these_flows' not in locals():
        logger_preprocflow.error("No flow for the devAddr " + str(devAddr))
        return(None)

    return(pd_these_flows)


       
      
# --------------------------------------------------------
#       DISK PERSISTENT STORAGE
# --------------------------------------------------------


  
  
  
def load_from_disk(verbose=False):
    """ Load from disk the dataframe (with parquet)
        
    :return pd_all_flows: the pandas dataframe

    
    """
 
    if verbose:
        logger_preprocflow.info("Loading parquet data from " + FILENAME_DF + " :")
        logger_preprocflow.info("\t> Reading values ....")

    if  os.path.exists(FILENAME_DF):
        pd_all_flows = pd.read_parquet(FILENAME_DF)
        
        # force some types in the pandaframe
        pd_all_flows['nb_pkts'] = pd_all_flows['nb_pkts'].astype('int')
    else:
        logger_preprocflow.info(FILENAME_DF + " doesn't exist.")
        pd_all_flows = pd.DataFrame({'empty' : []})


    return(pd_all_flows)
    

#save to the disk the main dataframe regrouping all the flow stats (individual distributions per flow are handled separately)
def save_to_disk(pd_all_flows):
    """ Save to disk the dataframe (with parquet)
        
    :param pd_all_flows: the pandas dataframe
    X columns:
    devAddr: string
    median_interpkt_time_ms: float (seconds)
    nb_pkt: integer
    etc.
    """

    
    # savings separately the dataframe without the individual distributions p(arquet format)
    #pd_all_flows[['devAddr', 'nb_pkts', 'median_interpkt_time_ms']].to_parquet(FILENAME_DF)
    pd_all_flows.to_parquet(FILENAME_DF)
   
      
 


def load_distribs_forDevAddr_from_disk(pd_all_flows, devAddr, pd_distrib, verbose=False):
    """ load a list of individual distribution from the disk into a dataframe (with parquet)
        
    :param pd_all_flows: a pandas distribution representing all the flows (synthetic data) in the dataset

    :param devAddr: the devAddr to read from the filesystem
    
    :param pd_distrib: a list to complete with the additionnal 
    
    :param verbose: verbose mode (default=False) to debug
     
    :returns: the updated pd_distrib (a list of dataframes with the raw distribution  -- inter packet time + fCnt + etc.)
    
    :rtype: a list of dataframe (be careful, several flows may be included, since several flows can be associated to the same devAddr)
    
    """
     
    #get all the mqtt_time for theis devAddr
    for time_1st in pd_all_flows[pd_all_flows['devAddr']== devAddr]['time_1st']:
    
        filename_distrib = FILENAME_DISTRIB + devAddr + '_' + time_1st.strftime(tools.time.DATE_FORMAT_FILENAME) + '.parquet'
        pd_distrib.append(pd.read_parquet(filename_distrib))
        
        if verbose:
           logger_preprocflow.info("Addr=" + devAddr + " Distrib_length=" + str(pd_distrib['interpkt_time_ms'].size) + " filename=" + filename)
 
    
    return(pd_distrib)
    
    
def load_distribs_forDevAddr_and_time_1st_from_disk(devAddr, time_1st, verbose=False):
    """ load one distribution from the disk into a dataframe (with parquet)
        
    :param devAddr: the devAddr to read
    
    :param time_1st: the mqtt_time (of the first packet of the flow) to read

    :returns: the raw distribution (inter packet time + fCnt + etc.)
    
    :rtype: dataframe
    
    """
     
    filename_distrib = FILENAME_DISTRIB + devAddr + '_' + time_1st.strftime(tools.time.DATE_FORMAT_FILENAME) + '.parquet'
    pd_distrib = pd.read_parquet(filename_distrib)
    logger_preprocflow.debug("Addr=" + devAddr + " Distrib_length=" + str(pd_distrib['interpkt_time_ms'].size) + " filename=" + filename_distrib)
    
     
    return(pd_distrib)



     
def save_distrib_to_disk(pd_distrib, devAddr, time_1st):
    """ Save to disk a dataframe (with parquet)
        
    :param pd_distrib: the pandas dataframe to store
    
    : param devAddr: string, the devAddr of ths flow
    
    : param time_1st: integer, the mqtt time of the first packet of the flow
    """
     
    # store the timeseries in individual files
    filename_distrib = FILENAME_DISTRIB + devAddr + '_' + time_1st.strftime(tools.time.DATE_FORMAT_FILENAME) + '.parquet'
    logger_preprocflow.debug("Addr=" + devAddr + " Distrib_length=" + str(pd_distrib['interpkt_time_ms'].size) + " filename=" + filename_distrib)
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
        signal.signal(signal.SIGINT, lambda signal, frame: self._signal_handler() ) # protection against CTRL-C
        self.terminated = False         # the process doesn't need to be terminated
        
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
        #print(list_devAddr_pending)
        
        # empty pandas dataframe (none read from the disk) -> let's create it
        if self.pd_all_flows.empty is True:
            self.pd_all_flows = pd.DataFrame({'devAddr': [], 'fCnt_1st': [], 'fCnt_last': [], 'time_1st': [], 'time_last': [], 'median_interpkt_time_ms': [], 'max_interpkt_time_ms': [], 'min_interpkt_time_ms': [], 'nb_pkts': []})

        # Else, some have already been extracted from the disk
        # remove from the pending list the devAddr already handled
        # NB: a devAddr may correspond to multiple flows. But when a devAddr is processed, all the corresponding flows are also processed
        else:
            devAddr_proc = self.pd_all_flows['devAddr'].drop_duplicates()
          
            logger_preprocflow.info(str(len(devAddr_proc)) + " devAddr already processed and saved in local:")
            logger_preprocflow.info("\tdevAddr\t\tNb flows\tNb pkts")
           
            for devAddr in devAddr_proc:
                list_devAddr_pending.remove(devAddr)
                
                if (logger_preprocflow.getEffectiveLevel() >= logging.DEBUG):
                    pd_records = load_distribs_forDevAddr_from_disk(self.pd_all_flows, devAddr, [], verbose=False)
                      
                    logger_preprocflow.info("\t" + devAddr + "\t" + str(len(pd_records)) + "\t\t" + str(self.pd_all_flows[self.pd_all_flows.devAddr == devAddr]["nb_pkts"].sum()) )
                    logger_preprocflow.debug("memory: "+ str(sys.getsizeof(self.pd_all_flows) / (1024 * 1024)) + " MB")

      

        #get the inter packet times for a given devAddr
        logger_preprocflow.info("> Reading new values in Elastic Search ( "+ str(len(list_devAddr_pending)) +" )")
        logger_preprocflow.info("\tdevAddr\t\tNb flows\tNb pkts")
    
        for devAddr in list_devAddr_pending :

            # get the new record(s) for this devAddr (one record per flow)
            pd_records = eq_query_get_interpkt(devAddr)
            #print(pd_records.to_string())
            
            # concatenation to the global pandaframe
            if pd_records is not None :
                if self.pd_all_flows.empty is True:
                    self.pd_all_flows = pd_records
                else:
                    self.pd_all_flows = pd.concat([self.pd_all_flows, pd_records], ignore_index=True)

                #logs
                logger_preprocflow.info("\t" + devAddr + "\t" + str(pd_records.shape[0]) + "\t\t" + str(pd_records["nb_pkts"].sum())  )
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
    pd_all_flows = load_from_disk(verbose=True)
    
    
    # ---- debug for one specific address -----
    #devAddr = "0e7290de"
    #print(pd_all_flows[pd_all_flows['devAddr'] == devAddr])
    #pd_records = load_distribs_forDevAddr_from_disk(pd_all_flows, devAddr, pd_records=[], verbose=False)       # complete distrib already processed
    #pd_records = eq_query_get_interpkt(devAddr)      # distrib reextracted (not processed)
    #print(pd_records)
    #exit(0)

    # -- elastic search ----
    # extract from elastic search what was not read on the disk
    # encapsulated in a class to be able to stop the computation with a ctrl-c
    app = Application(pd_all_flows)
    app.MainLoop()
 
   
                     
    # ---- disk -----
    # save the pandas frame to the disk
    save_to_disk(app.pd_all_flows)


     
    
    

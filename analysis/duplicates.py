""" Duplicates analysis .

This scripts reads the packets stored on the disk (per flow distribution)
and analyzes them (i.e., plot some graphs for all packets together)


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
sys.path.insert(1, '../preproc')


# configuration parameters
import myconfig

# my tool functions in common for the analysis
import tools

# numerical libraries
import pandas as pd
import matplotlib.pyplot as plt
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
import numpy as np
import math
import scipy.stats

# format
from datetime import datetime, timedelta

# Data science and co
import seaborn as sns
import pandas as pd
   
#sys
import sys, getopt
import random

#logs
import logging
logger = logging.getLogger('dups')
logger.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)


# read distributions from the disk (preprocessed)
import extract_interpacket_distribution



#parameters
NB_PKTS_MIN = 5         # minimum number of packets for a given devAddr (else discarded) to compute the median value
FNCT_DIFF_MAX = 100     # maximum average fnct_diff for a flow
NB_PLOTS = 16           # number of plots for individual distributions



# --------------------------------------------------------
#       PLOTS
# --------------------------------------------------------


                
def plot_nbdups_time_heatmap(pd_flat_values):
    """Heatmap: hour/nb duplicates
        
    :param distribution: a (flat) dataframe with all the packets 
    
    """
    
    # extract the hour of the day
    pd_flat_values['hour'] = pd.to_datetime(pd_flat_values['time']).dt.hour
    
    # grouby the two columns
    # unstack to transform into a 2D arrray, completing with zero for N/A values
    values = pd_flat_values[pd_flat_values['nb_duplicates'] < 10].groupby(['nb_duplicates', 'hour']).size().unstack().fillna(0)
         
 
    sns.set()
    sns.set_theme(style='whitegrid')
    sns.set(font_scale=1)

    g = sns.heatmap(
        data=values,
    )
    g.set(xlabel='Hour', ylabel='Number of duplicates')

    #save figure
    g.figure.savefig("figures/duplicates_nbdups_time_heatmap.pdf")

   
   
           
           
def plot_nbdups_CDF(pd_flat_values):
    """ Pairplot SF and PRR
        
    :param distribution: a pandas dataframe with the flows 
    
    """
    
    sns.set()
    sns.set_theme()
    sns.set(rc={"figure.figsize":(11, 5)})
    sns.set_context("notebook", font_scale=2.0)
    
    
    g = sns.ecdfplot(
        pd_flat_values,
    )
    g.set(xlabel='Number of duplicates', ylabel='Cumulative Distribution')
    g.set(xlim=(0, 10))
    g.set_xticks([0,1,2,3,4,5,6,7,8,9,10])
       
        
    #save figure
    plt.tight_layout(pad=1.0, h_pad=None, w_pad=None)
    g.figure.savefig("figures/duplicates_nbdups_cdf.pdf")
    g.figure.clf()
         

   
   


# --------------------------------------------------------
#       MAIN
# --------------------------------------------------------

   
    
# executable
if __name__ == "__main__":
    """Executes the script to analyze the distribution of inter packet times
 
    """
       
    # ---- disk - load data -----
    # load data that is on the disk (already read previously)
    pd_all_flows = extract_interpacket_distribution.load_from_disk(verbose=True)
    
    logger.info("Reading distributions from the disk....")
    if pd_all_flows.empty:
        logger.error("\t\t>0 devAddrs in the disk")
        logger.error("Nothing to read -> stop the analysis here")
        exit(0)
    else:
        logger.debug("\t\t> "+ str(len(pd_all_flows)) + " devAddrs in the disk")
   


    # --- filtering ---
    
    
    #-- MIN NB PACKETS
    nb_records_unfiltered = len(pd_all_flows)
    pd_all_flows = pd_all_flows[pd_all_flows['nb_pkts'] >= NB_PKTS_MIN]
    nb_records_minpkts = len(pd_all_flows)
    logger.info("\t\t> removed "+ str(nb_records_unfiltered - nb_records_minpkts) + " flows without enough packets (<" + str(NB_PKTS_MIN) + ")" )
    
    #-- MAX FNCT DIFF
    pd_all_flows = pd_all_flows[pd_all_flows['mean_fCnt_diff'] <= FNCT_DIFF_MAX]
    nb_records_maxfnctdiff = len(pd_all_flows)
    logger.info("\t\t> removed "+ str(nb_records_minpkts - nb_records_maxfnctdiff) + " flows with a too high fnctdiff (>=" + str(FNCT_DIFF_MAX) + ")" )
    logger.info("\t\t> "+ str(len(pd_all_flows)) + " flows to process")



    # --- Analysis ---

    
    # flat values for all the flows (a single dataframe with all the packets)
    pd_distrib = []
    for devAddr in pd_all_flows['devAddr']:
        pd_distrib = extract_interpacket_distribution.load_distribs_forDevAddr_from_disk(pd_all_flows, devAddr, pd_distrib)
    #transform the array into a dataframe
    pd_flat_values = pd.concat(pd_distrib)

    # time distribution of the packet losses
    #plot_nbdups_time_heatmap(pd_flat_values)
        
    # CDF nb_duplicates
    plot_nbdups_CDF(pd_flat_values['nb_duplicates'])
    
    
    # addresses


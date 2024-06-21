""" inter packet time distribution analysis .

This scripts reads the distribution stored on the disk and
analyzes them (i.e., plot some graphs)


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
sys.path.insert(1, '../preproc')


# configuration parameters
import myconfig

# my tool functions in common for the analysis
import tools

# numerical libraries
import pandas as pd
import matplotlib.pyplot as plt
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
logger_flow = logging.getLogger('flow_distribution')
logger_flow.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)


    
# read distributions from the disk
import extract_interpacket_distribution



#parameters
NB_PKTS_MIN = 25        # minimum number of packets for a given devAddr (else discarded) to compute the median value
NB_PLOTS = 25           # number of plots for individual distributions
INTERPKTIME_MAX = 10**4 # maxium interpacket time considered when plotting the correlation nbpkts / inter packet time


# --------------------------------------------------------
#       PLOTS
# --------------------------------------------------------



def plot_interpkt_time_distribution_grid(pd_all_flows, plot_list, count, nb_cols):
    """ Plot a list of distributions (timeseries) from a pandas dataframe with an ECDF
        
    :param distribution: a pandas dataframe containing a distribution (pandas series) for each row
    
    """
    sns.set()
    sns.set_theme()
    sns.set(font_scale=1/math.pow(len(plot_list), 1/3))

    #a grid of plots
    fig, axs = plt.subplots(ncols=nb_cols, nrows=math.ceil(count/nb_cols))
    
    #live view of the distributions for each packet
    for g_id in range(0, len(plot_list)-1):
    
        col = g_id % nb_cols
        row = math.floor(g_id / nb_cols)
        
        #print("g_id=" + str(g_id) + ' plot=' + str(plot_list[g_id]))
 
        #not enough packets -> nothing to plot
        if pd_all_flows.iloc[plot_list[g_id]]['nb_pkts'] < NB_PKTS_MIN:
            logger_flow.error("Not enough packets for the devAddr " + pd_all_flows.iloc[plot_list[g_id]]['devAddr'] + " -> should not happen (these @ should be filtered first)")
            continue
        
         
        # several rows in the plots
        pd_distrib = extract_interpacket_distribution.load_distribs_forDevAddr_and_fCnt_1st_from_disk(pd_all_flows.iloc[plot_list[g_id]]['devAddr'],  pd_all_flows.iloc[plot_list[g_id]]['fCnt_1st'])
        
     
        #convert into minutes
        pd_distrib['interpkt_time'] = pd_distrib['interpkt_time']/3600
        

        if (count > nb_cols):
            g = sns.ecdfplot(
                pd_distrib['interpkt_time'].array,
                ax=axs[row, col]
            )
        #one single row
        elif count > 1:
            g = sns.ecdfplot(
                pd_distrib['interpkt_time'].array,
                ax=axs[col]
            )
        #one single plot
        else:
             g = sns.ecdfplot(
                pd_distrib['interpkt_time'].array
            )

        g.set(xlabel="inter pkt time (min) /"+ str(pd_distrib['interpkt_time'].size)+" pkts/@="+pd_all_flows.iloc[plot_list[g_id]]['devAddr'], ylabel='Proportion')
        g.set(xlim=(0, np.max(pd_distrib['interpkt_time'].array)))
        
        #debug
        logger_flow.info("g_id=" + str(plot_list[g_id]) + ", devAddr=" + pd_all_flows.iloc[plot_list[g_id]]['devAddr'] + ", max=" + str(np.max(pd_distrib['interpkt_time'].array)))
        
    plt.tight_layout(pad=0.8, h_pad=None, w_pad=None)
    g.figure.savefig("figures/flow_interpkt_time_distribution_collection.pdf")
    g.figure.clf()

    
    

def plot_interpkt_time_distribution_unique(pd_all_flows):
    """ Plot a distribution (pd dataframe) with an ECDF
        
    :param distribution: a pandas dataframe with the data to plot
    
    """
    sns.set()
    sns.set_theme()
    sns.set(font_scale=1)
      
    g = sns.ecdfplot(
        pd_all_flows,
        log_scale=True
    )
    g.set(xlabel='Inter pkt time', ylabel='Proportion')
    
    # remove values under 1s from the x-coordinates
    g.set(xlim=(1, pd_all_flows.max()))
    
    #label with the typical units of time
    values = [1, 60, 3600, 86400, 604800, 2419200]
    labels = ["1s", "1min", "1h", "1d", "1w", "1m" ]
    g.set_xticks(values, labels=labels)
    
    
    #save figure
    plt.tight_layout(pad=1.0, h_pad=None, w_pad=None)
    g.figure.savefig("figures/interpkt_time_distribution_median.pdf")
    g.figure.clf()
         
     
     
def plot_interpkt_nbpkts(pd_all_flows):
    """ Plot the interpacket time vs. the number of packets of the flow
    to see the correlation
        
    :param distribution: a pandas dataframe with the data to plot
    
    """
    
    sns.set()
    sns.set_theme(style='whitegrid')
    sns.set(font_scale=1)


    #g = sns.scatterplot(
    #    x="median_interpkt_time",
    #    y="nb_pkts",
    #    data=pd_all_flows
    #    )
    g = sns.pairplot(
        pd_all_flows,
        diag_kind="kde",
    #   corner=True,
    #    diag_kind="hist",
    )
    g.map_lower(sns.kdeplot, levels=4, color=".2")
    
    #save figure
    g.figure.savefig("figures/flow_interpkttime_nbpkts_correlation.pdf")
    g.figure.clf()
  
    r,p = scipy.stats.pearsonr(pd_all_flows["median_interpkt_time"], pd_all_flows["nb_pkts"])
    logger_flow.info('correlation coefficient =' + str(r))
    logger_flow.info('p-value = ' + str(p))
    






# --------------------------------------------------------
#       MAIN
# --------------------------------------------------------

def help():
    print ("Usage: "+ sys.argv[0] +' -n <nb_plots>')


def main(argv):
    """Extracts the arguments
    
    """
    global NB_PLOTS
        

    try:
        opts, args = getopt.getopt(argv,"hn::",["nb_plots="])
    except getopt.GetoptError:
        print("Error in the arguments")
        help()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
           help()
           sys.exit(0)
        elif opt in ("-n", "--nb_plots"):
           NB_PLOTS = int(arg)


    logger_flow.info('Will generate the individual distribution plots for ' + str(NB_PLOTS) + ' devAddrs')

   
   
   
    
# executable
if __name__ == "__main__":
    """Executes the script to analyze the distribution of inter packet times
 
    """
    
    # ---- arguments -----
    main(sys.argv[1:])
       
    # ---- disk -----
    # load data that is on the disk (already read previously)
    logger_flow.info("Reading distributions from the disk....")
    pd_all_flows = extract_interpacket_distribution.load_from_disk()
    logger_flow.info("> done!")
    
    #filtering
    pd_all_flows = pd_all_flows[(pd_all_flows.nb_pkts >= NB_PKTS_MIN)]


    # --- plots ---
    # plot a grid of distributions (invidividual analysis)
    nb_plots = min(NB_PLOTS, len(pd_all_flows))
    nb_cols = math.ceil(math.sqrt(nb_plots))
    plot_list = random.choices(range(0,len(pd_all_flows)-1), k=nb_plots)
    plot_interpkt_time_distribution_grid(pd_all_flows=pd_all_flows, plot_list=plot_list, count=NB_PLOTS, nb_cols=nb_cols)


    #info
    logger_flow.info("Analysis: " + str(len(pd_all_flows['median_interpkt_time'])) + " / " +  str(len(pd_all_flows)) + " devAddr are significant (min "+str(NB_PKTS_MIN)+" pkts / total)")

    # plot the EDCF of the median inter packet time (remove samples with not enough packets)
    plot_interpkt_time_distribution_unique(pd_all_flows['median_interpkt_time'])

    #correlation inter pkt time / nb packets
    plot_interpkt_nbpkts(pd_all_flows[(pd_all_flows.nb_pkts >= NB_PKTS_MIN) & (pd_all_flows.nb_pkts <= 4000 ) & (pd_all_flows.median_interpkt_time <= INTERPKTIME_MAX)])

    
#0f9aa96f : distribution en escalier

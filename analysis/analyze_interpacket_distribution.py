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
from datetime import datetime, timedelta

# Data science and co
import seaborn as sns
import pandas as pd
   
#sys
import sys, getopt
import random

#logs
import logging
logger_interpkt = logging.getLogger('interpkt_distribution')
logger_interpkt.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)


    
# read distributions from the disk
import extract_interpacket_distribution



#parameters
NB_PKTS_MIN = 25     # minimum number of packets for a given devAddr (else discarded) to compute the median value
NB_PLOTS = 25       # number of plots for individual distributions

          
# --------------------------------------------------------
#       PLOTS
# --------------------------------------------------------



def plot_distribution_grid(pd_frame, plot_list, count, nb_cols):
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
        if pd_frame.iloc[plot_list[g_id]]['nb_pkts'] < NB_PKTS_MIN:
            logger_interpkt.error("Not enough packets for the devAddr " + pd_frame.iloc[plot_list[g_id]]['devAddr'] + " -> should not happen (these @ should be filtered first)")
            continue
        
         
        # several rows in the plots
        pd_distrib = extract_interpacket_distribution.load_distrib_from_disk(pd_frame.iloc[plot_list[g_id]]['devAddr'])
         
        #convert into minues
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

        g.set(xlabel="inter pkt time (min) /"+ str(pd_distrib['interpkt_time'].size)+" pkts/@="+pd_frame.iloc[plot_list[g_id]]['devAddr'], ylabel='Proportion')
        g.set(xlim=(0, np.max(pd_distrib['interpkt_time'].array)))
        
        #debug
        logger_interpkt.info("g_id=" + str(plot_list[g_id]) + ", devAddr=" + pd_frame.iloc[plot_list[g_id]]['devAddr'] + ", max=" + str(np.max(pd_distrib['interpkt_time'].array)))
        
    plt.tight_layout(pad=0.8, h_pad=None, w_pad=None)
    g.figure.savefig("figures/interpkt_time_distributions.pdf")
    g.figure.clf()

    
    

def plot_distribution_unique(pd_frame):
    """ Plot a distribution (pd dataframe) with an ECDF
        
    :param distribution: a pandas dataframe with the data to plot
    
    """
    sns.set()
    sns.set_theme()
    sns.set(font_scale=1)
      
    g = sns.ecdfplot(
        pd_frame,
        log_scale=True
    )
    g.set(xlabel='Inter pkt time (seconds)', ylabel='Proportion')
    
    # remove values under 1s from the x-coordinates
    g.set(xlim=(1, pd_frame.max()))
    
    #label with the typical units of time
    values = [1, 60, 3600, 86400, 604800, 2419200]
    labels = ["1s", "1min", "1h", "1d", "1w", "1m" ]
    g.set_xticks(values, labels=labels)
    
    
    #save figure
    plt.tight_layout(pad=1.0, h_pad=None, w_pad=None)
    g.figure.savefig("figures/interpkt_time_median_distribution.pdf")
    g.figure.clf()
         
     
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


    logger_interpkt.info('Will generate the individual distribution plots for ' + str(NB_PLOTS) + ' devAddrs')

   
   
   
    
# executable
if __name__ == "__main__":
    """Executes the script to analyze the distribution of inter packet times
 
    """
    
    # ---- arguments -----
    main(sys.argv[1:])
       
    # ---- disk -----
    # load data that is on the disk (already read previously)
    logger_interpkt.info("Reading distributions from the disk....")
    pd_interpk = extract_interpacket_distribution.load_from_disk()
    logger_interpkt.info("> done!")



    #print(pd_interpk)
    #print(pd_interpk.iloc[2]['distribution'].to_string())
    #print(pd_interpk.iloc[2]['fCnt'].to_string())


    # --- plots ---
    # plot a grid of distributions (invidividual analysis)
    nb_plots = min(NB_PLOTS, len(pd_interpk))
    nb_cols = math.ceil(math.sqrt(nb_plots))
    plot_list = random.choices(range(0,len(pd_interpk[(pd_interpk.nb_pkts >= NB_PKTS_MIN)])-1), k=nb_plots)
    plot_distribution_grid(pd_interpk[(pd_interpk.nb_pkts >= NB_PKTS_MIN)], plot_list=plot_list, count=NB_PLOTS, nb_cols=nb_cols)
    
    # plot the EDCF of the median inter packet time (remove samples with not enough packets)
    plot_distribution_unique(pd_interpk[(pd_interpk.nb_pkts >= NB_PKTS_MIN)]['median_interpkt_time'])

    #info
    logger_interpkt.info("Analysis: " + str(len(pd_interpk[(pd_interpk.nb_pkts >= NB_PKTS_MIN)]['median_interpkt_time'])) + " / " +  str(len(pd_interpk)) + " devAddr are significant (min "+str(NB_PKTS_MIN)+" pkts / total)")

    
#0f9aa96f : distribution en escalier

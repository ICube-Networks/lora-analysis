""" inter packet time distribution analysis .

This scripts reads the distribution stored on the disk and
analyzes them (i.e., plot some graphs)


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


# read distributions from the disk (preprocessed)
import extract_interpacket_distribution



#parameters
NB_PKTS_MIN = 50        # minimum number of packets for a given devAddr (else discarded) to compute the median value
FNCT_DIFF_MAX = 100     # maximum average fnct_diff for a flow
NB_PLOTS = 16           # number of plots for individual distributions



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
    for g_id in range(0, len(plot_list)):
    
        col = g_id % nb_cols
        row = math.floor(g_id / nb_cols)
        
        #print("g_id=" + str(g_id) + ' plot=' + str(plot_list[g_id]))
 
        #not enough packets -> nothing to plot
        if pd_all_flows.iloc[plot_list[g_id]]['nb_pkts'] < NB_PKTS_MIN:
            logger_flow.error("Not enough packets for the devAddr " + pd_all_flows.iloc[plot_list[g_id]]['devAddr'] + " -> should not happen (these @ should be filtered first)")
            continue
        
         
        # several rows in the plots
        pd_distrib = extract_interpacket_distribution.load_distribs_forDevAddr_and_time_1st_from_disk(pd_all_flows.iloc[plot_list[g_id]]['devAddr'],  pd_all_flows.iloc[plot_list[g_id]]['time_1st'])
        
     
        #convert into minutes
        pd_distrib['interpkt_time_min'] = pd_distrib['interpkt_time_ms']/1000 #(60*1000)
        
        # labels for x-axis
        xvalues = [60, 3600] #, 86400, 604800, 2419200]
        labels = ["1min", "1h"] # "1d", "1w", "1m" ]
        

        if (count > nb_cols):
            g = sns.ecdfplot(
                pd_distrib['interpkt_time_min'].array,
                ax=axs[row, col]
            )
        #one single row
        elif count > 1:
            g = sns.ecdfplot(
                pd_distrib['interpkt_time_min'].array,
                ax=axs[col]
            )
        #one single plot
        else:
             g = sns.ecdfplot(
                pd_distrib['interpkt_time_min'].array,
                log_scale=True
            )

        g.set_xticks(xvalues, labels=labels)
        g.set(xlabel="inter pkt time ("+ str(pd_distrib['interpkt_time_min'].size)+" pkts,@="+pd_all_flows.iloc[plot_list[g_id]]['devAddr']+ ")", ylabel='Cumulative Distribution')
         
        #debug
        logger_flow.info(
        "g_id=" + str(plot_list[g_id]) +
        ", devAddr=" + pd_all_flows.iloc[plot_list[g_id]]['devAddr'] +
        ", max=" + str(np.max(pd_distrib['interpkt_time_min'].array)) +
        ", nbPkts=" + str(pd_all_flows.iloc[plot_list[g_id]]['nb_pkts']) +
        ", nbPkts=" + str(pd_distrib['interpkt_time_min'].size)
        )

        
    plt.tight_layout(pad=0.8, h_pad=None, w_pad=None)
    g.figure.savefig("figures/flow_interpkt_time_distribution_collection.pdf")
    g.figure.clf()

    
    

def plot_interpkt_ecdf(values, figname, xlabel):
    """ Plot a distribution (pd dataframe) with an ECDF
        
    :param distribution: a pandas dataframe with the data to plot
    
    """
    sns.set()
    sns.set_theme()
    sns.set(font_scale=1)
      
    g = sns.ecdfplot(
            values,
        log_scale=True
    )
    g.set(xlabel=xlabel, ylabel='Cumulative Distribution')
    
    # remove values under 1s from the x-coordinates
    g.set(xlim=(values.min(), values.max()))
    
    #label with the typical units of time
    xvalues = [60, 3600, 86400, 604800, 2419200]
    labels = ["1min", "1h", "1d", "1w", "1m" ]
    g.set_xticks(xvalues, labels=labels)
    
    
    #save figure
    plt.tight_layout(pad=1.0, h_pad=None, w_pad=None)
    g.figure.savefig(figname)
    g.figure.clf()
         
     
    
    
    
     
def plot_interpkt_nbpkts(pd_all_flows):
    """ Plot the interpacket time vs. the number of packets of the flow
    to see the correlation
        
    :param distribution: a pandas dataframe with the data to plot
    
    """
    
    sns.set()
    sns.set_theme(style='whitegrid')
    sns.set(font_scale=1)


    g = sns.pairplot(
        pd_all_flows,
        diag_kind="kde",
    )
    g.map_lower(sns.kdeplot, levels=4, color=".2", warn_singular=False)
    
    #save figure
    g.figure.savefig("figures/flow_interpkttime_nbpkts_correlation.pdf")
    g.figure.clf()
  
    # corr coeff
    r,p = scipy.stats.pearsonr(pd_all_flows['median_interpkt_time_ms'], pd_all_flows["nb_pkts"])
    logger_flow.info('Pearson coefficient(median_interpkt_time_ms/nb_pkts) =' + str(r))
    logger_flow.info('\t\t> p-value = ' + str(p))
    




def plot_PRR_distrib(pd_all_flows):
    """ Plot the Packet Reception Rate distribution of flows
        
    :param distribution: a pandas dataframe with the data to plot
    
    """
    logger_flow.info("Plot the distribution of the PRR for all flows")
    
    sns.set()
    sns.set_theme(style='whitegrid')
    sns.set(font_scale=1)
    values = 1 / pd_all_flows['median_fCnt_diff']

    g = sns.ecdfplot(
        values,
        log_scale=False
    )
    g.set(xlabel='Packet Reception Rate', ylabel='Cumulative Distribution')
    
    # remove values under 1s from the x-coordinates
    #g.set(xlim=(values.min(), values.max()))
    
    #save figure
    plt.tight_layout(pad=1.0, h_pad=None, w_pad=None)
    g.figure.savefig("figures/flow_distribution_mean_prr.pdf")
    g.figure.clf()
         


                
def plot_SF_PRR(pd_all_flows):
    """ Pairplot SF and PRR
        
    :param distribution: a pandas dataframe with the packets (SF / PRR)
    
    """
    
    
    # extract for each flow the SF and PRR (median values)
    # NB: SF is mostly constant for a flow
    values = []
    for index, flow in pd_all_flows.iterrows():    
        pd_distrib = extract_interpacket_distribution.load_distribs_forDevAddr_and_time_1st_from_disk(flow['devAddr'],  flow['time_1st'])
        record = {
            'PRR' : 1 / pd_distrib['fCnt_diff'].median(),
            'SF' : round(pd_distrib['SF'].median()),
            'nb_pkts' : pd_distrib.size
        }
        values.append(record)
    # create the associated dataframe
    pd_values = pd.DataFrame(values)
    
    sns.set()
    sns.set_theme(style='whitegrid')
    sns.set(font_scale=1)

    g = sns.relplot(
        data=pd_values,
        x="PRR",
        y="SF",
        size="nb_pkts",
        sizes=(15, 200)
    )
    g.set(xlabel='Packet Reception Rate', ylabel='Spreading Factor')

    #save figure
    g.figure.savefig("figures/SF_PRR_pairplot.pdf")
   
 

                
def plot_PRR_timedistrib(pd_all_flows):
    """ Pairplot SF and PRR
        
    :param distribution: a pandas dataframe with the packets (SF / PRR)
    
    """
    
    
    # extract for each flow the SF and PRR (median values)
    # NB: SF is mostly constant for a flow
    pd_distrib = []
    for devAddr in pd_all_flows['devAddr']:
        pd_distrib = extract_interpacket_distribution.load_distribs_forDevAddr_from_disk(pd_all_flows, devAddr, pd_distrib)

   
    pd_values = pd.concat(pd_distrib)
    pd_values['hour'] = pd.to_datetime(pd_values['mqtt_time']).dt.hour
    print(pd_values['hour'])
    
    exit(4)

    sns.set()
    sns.set_theme(style='whitegrid')
    sns.set(font_scale=1)

    g = sns.relplot(
        data=pd_values,
        x="PRR",
        y="SF",
        sizes=(15, 200)
    )
    g.set(xlabel='Packet Reception Rate', ylabel='Spreading Factor')

    #save figure
    g.figure.savefig("figures/SF_PRR_pairplot.pdf")
   
 


                    





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
    pd_all_flows = extract_interpacket_distribution.load_from_disk(verbose=True)
    
    logger_flow.info("Reading distributions from the disk....")
    if pd_all_flows.empty:
        logger_flow.error("\t\t>0 devAddrs in the disk")
        logger_flow.error("Nothing to read -> stop the analysis here")
        exit(0)
    else:
        logger_flow.debug("\t\t> "+ str(len(pd_all_flows)) + " devAddrs in the disk")

    
    #filtering   -- MIN NB PACKETS
    nb_records_unfiltered = len(pd_all_flows)
    pd_all_flows = pd_all_flows[pd_all_flows['nb_pkts'] >= NB_PKTS_MIN]
    nb_records_minpkts = len(pd_all_flows)
    logger_flow.info("\t\t> removed "+ str(nb_records_unfiltered - nb_records_minpkts) + " flows without enough packets (<" + str(NB_PKTS_MIN) + ")" )
    #filtering   -- MAX FNCT DIFF
    pd_all_flows = pd_all_flows[pd_all_flows['mean_fCnt_diff'] <= FNCT_DIFF_MAX]
    nb_records_maxfnctdiff = len(pd_all_flows)
    logger_flow.info("\t\t> removed "+ str(nb_records_minpkts - nb_records_maxfnctdiff) + " flows with a toot high fnctdiff (>=" + str(FNCT_DIFF_MAX) + ")" )
    logger_flow.info("\t\t> "+ str(len(pd_all_flows)) + " flows to process")

    

 

    # --- plots ---
    
    #PRR (fcnt diff between consecutive packets)
    plot_PRR_distrib(pd_all_flows)
 
    #pairplot SF / PRR for all packets (flows are used to estimate the PRR)
    plot_SF_PRR(pd_all_flows)
    
    # time distribution of the packet losses
    plot_PRR_timedistrib(pd_all_flows)
    
    # plot a grid of distributions (randomly selected flows)
    nb_plots = min(NB_PLOTS, len(pd_all_flows))
    nb_cols = math.ceil(math.sqrt(nb_plots))
    plot_list = random.choices(range(0,len(pd_all_flows)-1), k=nb_plots)
    plot_interpkt_time_distribution_grid(pd_all_flows=pd_all_flows, plot_list=plot_list, count=NB_PLOTS, nb_cols=nb_cols)

    #info
    logger_flow.info("Analysis: " + str(len(pd_all_flows['median_interpkt_time_ms'])) + " / " +  str(len(pd_all_flows)) + " devAddr are significant (min "+str(NB_PKTS_MIN)+" pkts / total)")
    

    # plot the EDCF of the median inter packet time (remove samples with not enough packets)
    plot_interpkt_ecdf(
        values=pd_all_flows['median_interpkt_time_ms']/1000,
        figname="figures/flow_distribution_interpkttime.pdf",
        xlabel='Inter pkt time'
    )
    
    # EDCF of the flow duration
    plot_interpkt_ecdf(
        values=(pd_all_flows['time_last'] - pd_all_flows['time_1st']).dt.total_seconds(),
        figname="figures/flow_distribution_duration.pdf",
        xlabel="Flow duration"
    )

    #correlation between inter pkt time / nb packets
    plot_interpkt_nbpkts(pd_all_flows[['mean_fCnt_diff', 'median_fCnt_diff', 'max_fCnt_diff', 'median_interpkt_time_ms', 'nb_pkts']])


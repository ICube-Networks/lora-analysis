""" distribution of stats per operator .


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

# local libraries for the analysis
import tools
import extract_interpacket_distribution
import lorawan_operators

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
import base64

#logs
import logging
logger = logging.getLogger('operator')
logger.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)
logging.getLogger("fontTools.subset").setLevel(logging.WARNING)



# --------------------------------------------------------
#       SF distribution
# --------------------------------------------------------
      
        
def plot_operators_SF_distrib():
    """ Elastic query to get a list of SF per operator
     
    :param clientES is an active connection to an elastic search server
    
    :returns: a list of all SF and operators
    
    :rtype: list of string
    """

    clientES = tools.elasticsearch_open_connection()
    fieldnames = { "fieldname1" : 'extra_infos.operator.keyword', "fieldname2": 'txInfo.modulation.lora.spreadingFactor'}
    results_df = tools.elasticsearch_query_count_docs_with_twofields(fieldnames)

    #split into columns
    col_ops, col_SF, count = results_df.columns[:3]
    results_df[col_ops] = results_df[col_ops].replace({
        "Shenzhen Tencent Computer Systems Company Limited": "Shenzen Tencent",
        "NTT (Nippon Telephone and Telegraph)": "NTT",
    })
    
    # Construct the matrix of effective
    counts = results_df.pivot_table(
        index = col_SF,
        columns = col_ops,
        values = count,
        aggfunc = "sum",
        fill_value = 0
    )
    counts = counts.loc[:, counts.sum(axis=0).sort_values(ascending=False).index]
    
    # Compute the ratio per SF for each category
    ratios = counts.div(counts.sum(axis=0).replace(0, 1), axis=1) * 100

    # heatmap
    sns.set()
    sns.set_theme()
    sns.set(rc={"figure.figsize":(16, 6)})
    sns.color_palette("colorblind")
    g = sns.heatmap(
        ratios,
        cmap="YlGnBu",
        vmin=0,
        vmax=100,
    )
    cbar = g.collections[0].colorbar
    cbar.set_label("Ratio of packets (%)", fontsize=16)
    cbar.ax.tick_params(labelsize=12)

    g.tick_params(axis="x", labelsize=13)
    g.tick_params(axis="y", labelsize=14)
    plt.xlabel("Operators")
    plt.ylabel("SF value")
    plt.xticks(
        rotation=90,
        ticks=range(len(ratios.columns)),
        labels=ratios.columns
    )
    plt.yticks(rotation=0)
    plt.tight_layout(pad=0)
    g.figure.savefig("figures/operators_SF_distrib.pdf", bbox_inches="tight", pad_inches=0)
    g.figure.clf()
    



# --------------------------------------------------------
#       Packet sizes
# --------------------------------------------------------
      
        
def plot_operators_pksize_distrib():
    """ Elastic query to get a list of SF per operator
     
    :param clientES is an active connection to an elastic search server
    
    :returns: a list of all SF and operators
    
    :rtype: list of string
    """

    clientES = tools.elasticsearch_open_connection()
    fieldnames = { "fieldname1" : 'extra_infos.operator.keyword', "fieldname2": 'extra_infos.phyPayload.length'}
    results_df = tools.elasticsearch_query_count_docs_with_twofields(fieldnames)

    #split into columns
    col_ops, col_pksize, count = results_df.columns[:3]
    results_df[col_ops] = results_df[col_ops].replace({
        "Shenzhen Tencent Computer Systems Company Limited": "Shenzen Tencent",
        "NTT (Nippon Telephone and Telegraph)": "NTT",
    })
    results_df["group_pksize"] = (results_df[col_pksize] // 10) * 10
    
    # Construct the matrix of effective
    counts = results_df.pivot_table(
        index = "group_pksize",
        columns = col_ops,
        values = count,
        aggfunc = "sum",
        fill_value = 0
    )
    # order by descending number of packets per operator
    counts = counts.loc[:, counts.sum(axis=0).sort_values(ascending=False).index]
    
    # Compute the ratio per SF for each category
    ratios = counts.div(counts.sum(axis=0).replace(0, 1), axis=1) * 100

    # heatmap
    sns.set()
    sns.set_theme()
    sns.set(rc={"figure.figsize":(16, 6)})
    sns.color_palette("colorblind")
    g = sns.heatmap(
        ratios,
        cmap="YlGnBu",
        vmin=0,
        vmax=100,
    )
    cbar = g.collections[0].colorbar
    cbar.set_label("Ratio of packets (%)", fontsize=16)
    cbar.ax.tick_params(labelsize=12)
    
    g.tick_params(axis="x", labelsize=13)
    g.tick_params(axis="y", labelsize=14)
    #figure
    plt.xlabel("Operators")
    plt.ylabel("Packet length (10-byte bins)", fontsize=16)
    plt.xticks(
        rotation=90,
        ticks=range(len(ratios.columns)),
        labels=ratios.columns
    )
    g.set_yticks(g.get_yticks()[::2])
    plt.yticks(rotation=0)
    plt.tight_layout(pad=0)
    g.figure.savefig("figures/operators_pksize_distrib.pdf", bbox_inches="tight", pad_inches=0)
    g.figure.clf()
    






# --------------------------------------------------------
#       MAIN
# --------------------------------------------------------

   
    
# executable
if __name__ == "__main__":
    """Executes the script to analyze stats about packets
 
    """
    
    #----- distribution of stats per operator ---------
    plot_operators_SF_distrib()
    plot_operators_pksize_distrib()
   

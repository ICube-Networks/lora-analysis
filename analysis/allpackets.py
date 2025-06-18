""" all packets analysis .


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



# --------------------------------------------------------
#       PLOTS
# --------------------------------------------------------


           
def plot_operators_cdf(devaddr_df):
    """ Pairplot SF and PRR
        
    :param distribution: a pandas dataframe with the flows 
    
    """
    
    print(devaddr_df)
    
    sns.set()
    sns.set_theme()
    #sns.set(rc={"figure.figsize":(11, 5)})
    #sns.color_palette("colorblind")
    sns.set_context("notebook", font_scale=0.6)
    
    g = sns.catplot(
        data = devaddr_df,
        y = 'operator',
        x = 'doc_count',
        log_scale =  (True, False),
        aspect = 0.75,
        margin_titles = True,
    )
    g.set(ylabel='Operators', xlabel='Number of packets')
       
        
    #save figure
    #plt.tight_layout(pad=1.0, h_pad=None, w_pad=None)
    g.figure.savefig("figures/allpackets_operators.pdf")
    g.figure.clf()
         


# --------------------------------------------------------
#       MAIN
# --------------------------------------------------------

   
    
# executable
if __name__ == "__main__":
    """Executes the script to analyze stats about packets
 
    """
    
    #get all the devaddr and corresponding operators
    operators = lorawan_operators.load_operators_csv()
    devaddr_df = extract_interpacket_distribution.es_query_get_devAddr()
    devaddr_df['operator'] = devaddr_df['devAddr'].apply(lambda x: lorawan_operators.find_operators(operators, x))
    
    #shorten this specific operator (too long string)
    devaddr_df.replace('Shenzhen Tencent Computer Systems Company Limited', 'Shenzhen Tencent', inplace = True)
    
    #regroup by operator, removing the devAddr field
    operators_df = devaddr_df.groupby('operator')['doc_count'].sum().reset_index().sort_values(by=['doc_count'], ascending=False).reset_index(drop=True)

    plot_operators_cdf(operators_df)

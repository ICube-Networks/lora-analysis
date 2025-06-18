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
    
    print(devaddr_df)

"""Spreading Factor analysis .

Plots the distribution of the spreading factors of LoRa

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
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
import numpy as np

# format
import requests, json, os, tarfile, pathlib
from datetime import datetime
import matplotlib.dates as mdates

# Import seaborn
import seaborn as sns



   
#logs
import logging
logger_sf = logging.getLogger('SF_analysis')
logger_sf.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)






############################################################
#           SF per week
############################################################


def  es_query_SF():
    """Elastic search query for an histogram.
    
    This function sends a query to an elastic search server  to retrieve an histogram (per week) of the number of packets per SF
    
    :returns: a pandas DataFrame which contains the counts for each window of the histogram
    :rtype: DataFrame
    """

    #open the connection
    clientES = tools.elasticsearch_open_connection()

    #get the number of valid records per SF per channel
    resp = clientES.options(
        basic_auth=(myconfig.user, myconfig.password),
    ).search(
        index=myconfig.index_name,
        size=0,
        query=tools.queries.QUERY_ALL_NODUP,
        aggs={
            "SF": {
                "terms" : { "field" : "txInfo.loRaModulationInfo.spreadingFactor" },
                "aggregations": {
                    "date": {
                        "date_histogram" : {
                            "field" : "rxInfo.time",
                            "calendar_interval": "week",
                            "format": "yyy-MM-dd",
                            "time_zone": "Europe/Paris"
                        }
                    },
                },
            },
         }
    )

            
    

    results_df = tools.elasticsearch_agg_into_dataframe(es_reply=resp, agg_names=("SF", "date"), key_as_string=True, )
    dtime = datetime(2020, 9, 1, 20)
    results_df = results_df[results_df['date'] > dtime.timestamp()]
    results_df = results_df[results_df['count'] > 0]
    results_df['date'] = pd.to_datetime(results_df['date'], unit='ms')

    return(results_df)





def plot_SF(results_df):
    """Plot the SF distribution.
    
    This function plots the histogram of the LoRa Spreading Factors per week all along the dataset.
    
    :param pandas dataFrame containing the histogram
    """


    # Create a seaborn visualization
    sns.set()
    sns.set_theme()
    g = sns.relplot(
        data=results_df,
        kind="line",
        x="date", y="count",
        hue="SF", style="SF",
        palette="tab10",
    )

    # common
    axes = g.axes.flat[0]
    g.set(xlabel='Date', ylabel='Number of packets per day')
    g.set(ylim=(0, None))

    # formating dates: autoformatter to convert the whole duration into something human friendly, with the right size for the xlabel
    locator = mdates.AutoDateLocator()
    formatter = mdates.ConciseDateFormatter(locator)
    axes.xaxis.set_major_locator(locator)
    axes.xaxis.set_major_formatter(formatter)
    axes.margins(x=0)

    # save the figure
    fig = g.figure.savefig("figures/SF_distribution_week.pdf")

 






# executable
if __name__ == "__main__":
    """Executes the script to plot the histogram of the number of packets per SF
 
    """

  
    #elastic search query, transformed in a panda dataFrame
    results_df = es_query_SF()
    logger_sf.info(results_df)
    
    #plot it
    plot_SF(results_df)




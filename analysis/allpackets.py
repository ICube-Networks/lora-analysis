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
logger = logging.getLogger('dups')
logger.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout)


# --------------------------------------------------------
#       Packet sizes
# --------------------------------------------------------
      
        
def es_query_get_pkt_size():
    """ Elastic query to get a list of the packet size
     
    :param clientES is an active connection to an elastic search server
    
    :returns: a list of all devAddrs
    
    :rtype: list of string
    """

    clientES = tools.elasticsearch_open_connection()


    #Until we have no more doc
    pagination_count = 0
    list_pkt_size = []
    minkey = 0
    
    while True:

        try:
            response = clientES.options(
                request_timeout=3000
            ).search(
                index=myconfig.index_name,
                size=tools.queries.QUERY_NB_RESULT,
                pretty=True,
                human=True,
                query=tools.queries.QUERY_EXTRAINFO_EXIST_NODUP,
                source = False,
                fields=[
                    "time",
                    "phyPayload",
                    "_id"
                ],
                sort=[#"phyPayload.keyword"], #random sort --> too slow!!
                {
                    "_script": {
                        "type": "number",
                        #index with a hash of the PHY + time (to have the same key each time)
                        "script": "return(Math.abs(doc['phyPayload.keyword'].hashCode() + doc['time'].hashCode()));",
                        "order": "asc",
                    },
                }],
                search_after=[minkey],
            )
    
        #exception in the ES query
        except Exception as e:
            print(e)
            #print(e.message)
            #print(e.meta)
            #print(e.body)
            exit(66)
        
        
        #keep the next min key
        length = len(response["hits"]["hits"])
        minkey = response['hits']['hits'][length-1]['sort'][0]
        
        # for the next page
        pagination_count = pagination_count + 1
        print("page "+ str(pagination_count))
        
        # no more record
        if (length == 0) or (pagination_count >= 50) :      #in the dataset 26,032 x 10,000 records...
            logger.debug("Elastic search: last page for the aggregate query")
            break;
         
        #transform the fields in a dataframe
        df = pd.DataFrame.from_dict([document['fields'] for document in response['hits']['hits']])
        
        # convert the phyPayload field in hexa and divide the number of chars per 2 to have the length (in bytes)
        tmp_df = df["phyPayload"].apply(
            lambda x: (int)(len(base64.b64decode(x[0]).hex())/2)
        )
        #concatenate the new dataframe with the previous one (accumulation)
        if 'pkt_size_df' in locals():
            pkt_size_df = pd.concat([pkt_size_df, tmp_df], ignore_index=True)
        else:
            pkt_size_df = tmp_df
        #print(pkt_size_df)
            
   
    # end of extraction
    logger.info("\t> Found " + str(pkt_size_df.count()) + " packets")
   
    #result
    return(pkt_size_df)
 
 
 
           
def plot_pkt_size_cdf(pkt_size_df):
    """ ECDF of the packet size
        
    :param distribution: a pandas dataframe with the size of all packets 
    
    """

    sns.set()
    sns.set_theme()
    sns.set(font_scale=1)
      
    g = sns.ecdfplot(
        pkt_size_df,
        log_scale=False
    )
    plt.xlabel('Packet size (in bytes)', fontsize=12)
    plt.ylabel('Cumulative Distribution', fontsize=12)

        
    #save figure
    plt.tight_layout(pad=1.0, h_pad=None, w_pad=None)
    g.figure.savefig("figures/allpackets_pkt_size_ecdf.pdf")
    g.figure.clf()
    
    exit(0)
    
    
    
    

# --------------------------------------------------------
#       Operators
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
        x = 'operator',
        y = 'doc_count',
        log_scale =  (False, True),
        aspect = 1.75,
        margin_titles = True,
    )
    g.set_xticklabels(rotation=90, fontsize=9)
    g.set_yticklabels(fontsize=10)
    #g.set(xlabel='Operators', ylabel='Number of packets')
    plt.xlabel('Operators', fontsize=10)
    plt.ylabel('Number of packets', fontsize=10)

    #save figure
    plt.tight_layout(pad=1.0, h_pad=None, w_pad=None)
    g.figure.savefig("figures/allpackets_operators.pdf")
    g.figure.clf()
         



# --------------------------------------------------------
#       Class / acks
# --------------------------------------------------------

def func(pct, allvals):
    absolute = int(np.round(pct/100.*np.sum(allvals)))
    return f"{pct:.1f}%"
    
    
def plot_class_ack_distrib(data, fieldnames, figname):
    """ double pie plot of two categories (2 x2)
        
    :param: dataset (dataframe with 2 x 2 array)
    
    """
     
      
    #create a 2D np array (initialized with zeros)
    nb1 = data[fieldnames['fieldname1']].drop_duplicates().count()
    nb2 = data[fieldnames['fieldname2']].drop_duplicates().count()
    values = np.zeros((nb1, nb2))
    for val1 in data[fieldnames['fieldname1']].drop_duplicates() :
        for val2 in data[fieldnames['fieldname2']].drop_duplicates() :
            values[val1][val2] = data[(data[fieldnames['fieldname1']] == val1) & (data[fieldnames['fieldname2']] == val2)]['count'].item()
    
    #parameters
    size = 0.3
    tab20c = plt.color_sequences["tab20c"]
    outer_colors = [tab20c[i] for i in [0, 4, 8]]
    inner_colors = [tab20c[i] for i in [1, 2, 5, 6, 9, 10]]
    
    #figures
    fig, ax = plt.subplots()
    
    #outer pie
    wedges, texts, autotext = ax.pie(
        values.sum(axis=1),
        radius=1,
        colors=outer_colors,
        autopct=lambda pct: func(pct, values.sum(axis=1)),
        pctdistance=0.85,
        wedgeprops=dict(width=size,edgecolor='w')
        )
        
    bbox_props = dict(boxstyle="square,pad=0.3", fc="w", ec="k", lw=0.72)
    kw = dict(arrowprops=dict(arrowstyle="-"),
          bbox=bbox_props, zorder=0, va="center")
    
    classes = ['class A', 'class B']
    for i, p in enumerate(wedges):
        ang = (p.theta2 - p.theta1)/2. + p.theta1
        y = np.sin(np.deg2rad(ang))
        x = np.cos(np.deg2rad(ang))
        horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
        connectionstyle = f"angle,angleA=0,angleB={ang}"
        kw["arrowprops"].update({"connectionstyle": connectionstyle})
        ax.annotate(classes[i], xy=(x, y), xytext=(1.15*np.sign(x), 1.1*y),
                    horizontalalignment=horizontalalignment, **kw)

    #inner pie
    wedges, texts, autotext = ax.pie(
        values.flatten(),
        radius=1-size,
        colors=inner_colors,
        autopct=lambda pct: func(pct, values.flatten()),
        pctdistance=0.80,
        wedgeprops=dict(width=size, edgecolor='w')
        )
    acks = ['no ack', 'acks']
    
    
    
    for i, p in enumerate(wedges):
        ang = (p.theta2 - p.theta1)/2. + p.theta1
        y = np.sin(np.deg2rad(ang))
        x = np.cos(np.deg2rad(ang))
        horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
        connectionstyle = f"arc3,rad=0." #angle,angleA=0,angleB={ang}"
        kw["arrowprops"].update({"connectionstyle": connectionstyle})
       
       
       
        print(acks[i % 2])
        print(-0.2*np.sign(x))
        print(y)
        
        ax.annotate(
            acks[i % 2],
            xy=(0.5*np.sign(x), 0.3*y+0.02),
            xytext=(-0.1*np.sign(x), 1.5*y),
            horizontalalignment=horizontalalignment,
            **kw)
          
    #save figure
    plt.tight_layout(pad=0, h_pad=None, w_pad=None)
    plt.savefig(figname)
    plt.close()






# --------------------------------------------------------
#       MAIN
# --------------------------------------------------------

   
    
# executable
if __name__ == "__main__":
    """Executes the script to analyze stats about packets
 
    """
    
    
        
    # ----- packet size ---------
    # distribution
    pkt_size_df = es_query_get_pkt_size()
    plot_pkt_size_cdf(pkt_size_df)
    

    
    # ----- operator ---------
    # distribution
    
    #get all the devaddr and corresponding operators
    operators = lorawan_operators.load_operators_csv()
    devaddr_df = extract_interpacket_distribution.es_query_get_devAddr()
    devaddr_df['operator'] = devaddr_df['devAddr'].apply(lambda x: lorawan_operators.find_operators(operators, x))
    
    #shorten this specific operator (too long string)
    devaddr_df.replace('Shenzhen Tencent Computer Systems Company Limited', 'Shenzhen Tencent', inplace = True)
    devaddr_df.replace('NTT (Nippon Telephone and Telegraph)', 'NTT', inplace = True)

    #regroup by operator, removing the devAddr field
    operators_df = devaddr_df.groupby('operator')['doc_count'].sum().reset_index().sort_values(by=['doc_count'], ascending=False).reset_index(drop=True)

    plot_operators_cdf(operators_df)

    

    # ------- Traffic ---------
    # class A/B, with and wo acks
    
    
    param ={}
    fieldnames = { "fieldname1" : 'extra_infos.phyPayload.macPayload.fhdr.fCtrl.classB', "fieldname2": 'extra_infos.phyPayload.macPayload.fhdr.fCtrl.ack'}
    results_df = tools.elasticsearch_query_count_docs_with_twofields(fieldnames)
    plot_class_ack_distrib(results_df, fieldnames, "figures/allpackets_class_ack_distrib.pdf")
    


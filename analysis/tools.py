# numerical libraries
import pandas as pd
from matplotlib.dates import MO, TU, WE, TH, FR, SA, SU

  
            
            
############################################################
#           ES -> dataframe   (two dimensions array)
############################################################

# transform an elastic search reply to an agg query into a pandas dataframe
def elasticsearch_reply_into_dataframe(es_reply, row_name, col_name, key_as_string=True, debug=False):
    #dates use the key_as_string field to manipulate strings directly. Else, the field key can be used
    if key_as_string:
        key = "key_as_string"
    else:
        key = "key"
    
    if debug:
        print("------ DEBUG for elasticsearch_reply_into_dataframe() --------")
    
    results_df = pd.DataFrame()
    for row_df in es_reply["aggregations"][row_name]["buckets"]:
        nb_total = row_df["doc_count"]
        nb_total_bis = 0
        
        if debug:
            print("row: ", row_df)
        
        #the row does not exist -> add an empty column
        if row_df["key"] not in results_df.index :
            if debug :
                print("the row ", row_df["key"], " does not exist")
            results_df.loc[row_df["key"], :] = [0] * len(results_df.columns)
        
        for col_df in row_df[col_name]["buckets"]:
            if col_df["doc_count"] > 0 :
                
                #create the column if it doesn't exit --> no need to create the corresponding column. Automatically added to the row if it doesn't exist
                #if col_df[key] not in results_df.columns :
                #    if debug :
                #        print("the col ", col_df[key] , " does not exist")
                #    pd.concat(axis=1, [results_df, ])
                #    results_df.insert(0, col_df[key] ,  [0] * len(results_df) , True)

                #store the current value in the corresponding cell of the dataframe
                if debug :
                    print("   >", col_df[key], "=", col_df["doc_count"])
                nb_total_bis += col_df["doc_count"]
                results_df.loc[[row_df["key"]],[col_df[key]]] = col_df["doc_count"]
          
    
        if debug :
            print("       ", nb_total, " =?= ", nb_total_bis)

    if debug:
        print("------------")

    return(results_df)


# walk recursively in an aggregated reply
def elasticsearch_walk_aggrep(es_reply, agg_names, depth, results_df, tuple, key_as_string, debug=False):
    if key_as_string:
        key = "key_as_string"
    else:
        key = "key"
    
    #last recursive call, save the value (doc_count) in the tuple, and push the tuple into the dataframe
    if(depth == len(agg_names)):
        tuple['count'] = [es_reply['doc_count']]
        results_df = pd.concat([results_df, pd.DataFrame.from_dict(tuple)], ignore_index=True)
        return(results_df)

    #keep on walking recursively in the aggregated reply
    for elem in es_reply[agg_names[depth]]["buckets"]:
        # store the corresponding value for this tuple
        tuple[agg_names[depth]] = [elem['key']]
        results_df = elasticsearch_walk_aggrep(es_reply=elem, agg_names=agg_names, depth=depth+1, results_df=results_df, tuple=tuple, key_as_string=key_as_string, debug=debug)
        
    return(results_df)
   


# transform an elastic search reply to an agg query into a pandas dataframe
def elasticsearch_agg_into_dataframe(es_reply, agg_names, key_as_string=True, debug=False):
    if debug:
        print("------ DEBUG for elasticsearch_reply_into_dataframe() --------")
    
    #panda dataframe
    results_df = pd.DataFrame()
    tuple = {}
    
    #walk in the response of elastic search with the aggregated fields
    results_df = elasticsearch_walk_aggrep(es_reply=es_reply["aggregations"], agg_names=agg_names, depth=0, results_df=results_df, tuple=tuple, key_as_string=key_as_string, debug=debug)
    
    if debug:
        print("---'''''''-------'''''''---")
        print(results_df)
        
    return(results_df)



############################################################
#           Day of the week (string) -> int
############################################################



class dayofweek:
     short = ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')
     long = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
 
            
# return the string associated to a weekday id (int)
def shortdayofweek_to_int(day):
    for i in range(0, len(dayofweek.short)):
        if dayofweek.short[i] == day:
            return(i)
      
    raise Exception('Unknown week of day')

# return the int associated to a weekday (string)
def int_to_shortdayofweek(day):
    if (day <0 or day>7):
        raise Exception('Unknown week of day')
    return(dayofweek.short[i])
        
# return the string associated to a weekday id (int)
def longdayofweek_to_int(day):
    for i in range(0, len(dayofweek.long)):
        if dayofweek.long[i] == day:
            return(i)
      
    raise Exception('Unknown week of day')

# return the int associated to a weekday (string)
def int_to_longdayofweek(day):
    if (day <0 or day>7):
        raise Exception('Unknown week of day')
    return(dayofweek.long[i])
     

# numerical libraries
import pandas as pd
from matplotlib.dates import MO, TU, WE, TH, FR, SA, SU

  
            
            
############################################################
#           ES -> dataframe   (two dimensions array)
############################################################

# transform an elastic search reply to an agg query into a pandas dataframe
def elasticsearch_reply_into_dataframe(es_reply, row_name, col_name, debug):
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
                
                #create the column if it doesn't exit
                if col_df["key_as_string"] not in results_df.columns :
                    if debug :
                        print("the col ", col_df["key_as_string"] , " does not exist")
                    results_df.insert(0, col_df["key_as_string"] ,  [0] * len(results_df) , True)

                #store the current value in the corresponding cell of the dataframe
                if debug :
                    print("   >", col_df["key_as_string"], "=", col_df["doc_count"])
                nb_total_bis += col_df["doc_count"]
                results_df.loc[[row_df["key"]],[col_df["key_as_string"]]] = col_df["doc_count"]
          
    
        if debug :
            print("       ", nb_total, " =?= ", nb_total_bis)

    if debug:
        print("------------")

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
        print(day, "==", dayofweek.list[i])
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
        print(day, "==", dayofweek.long[i])
        if dayofweek.long[i] == day:
            return(i)
      
    raise Exception('Unknown week of day')

# return the int associated to a weekday (string)
def int_to_longdayofweek(day):
    if (day <0 or day>7):
        raise Exception('Unknown week of day')
    return(dayofweek.long[i])
     

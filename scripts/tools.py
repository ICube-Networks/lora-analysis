# numerical libraries
import pandas as pd


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

def day_of_week_int(day):
    match day:
        case "Monday":
            return 1
        case "Tuesday":
            return 2
        case "Wednesday":
            return 3
        case "Thursday":
            return 4
        case "Friday":
            return 5
        case "Saturday":
            return 6
        case "Sunday":
            return 7
        case _:
            return 8
        

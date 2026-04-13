import json
import glob
import pandas as pd
from pprint import pprint
import pandas as pd
import warnings

def split_combined_data_dicts(dict_to_split):

    split_data_dict = {}

    # test_fight_stats['sig_str_per_round']
    for key, value_list in dict_to_split.items():
        for i, val in enumerate(value_list):

            ## create new key based on inext
            new_key = f"{key}_{chr(97 + i)}"
            split_data_dict[new_key] = val

    return split_data_dict

def seperate_composite_columns(comp_dict):

    composite_free_dict = {}

    for key, val in comp_dict.items():

        if len(val.split(" of ")) == 2:

            landed_val = val.split(" of ")[0]
            attempted_val = val.split(" of ")[1]
            composite_free_dict['landed_' + key] = landed_val
            composite_free_dict['attempted_' + key] = attempted_val

        else:

            composite_free_dict[key] = val

    return composite_free_dict


def process_percentages(dict):

    for key in dict.keys():

        if 'percentage' in key:

            dict[key] = dict[key].replace("%", "").replace("---", "0")

    return dict

def process_per_round_dict(per_round_dict):

    split_dict = {}

    for round in per_round_dict.keys():

        split_dict[round] = process_percentages(seperate_composite_columns(split_combined_data_dicts(per_round_dict[round])))

    return split_dict

def process_fight(raw_fight):

    if 'error' in raw_fight:
        error_dict = {}

        return {"ERROR" : "Drop entry"}


    raw_fight['fight_totals_split'] = process_percentages(seperate_composite_columns(split_combined_data_dicts(raw_fight['fight_totals'])))
    raw_fight['totals_per_round_split'] = process_per_round_dict(raw_fight['totals_per_round'])
    raw_fight['sig_str_split'] = process_percentages(seperate_composite_columns(split_combined_data_dicts(raw_fight['sig_str'])))
    raw_fight['sig_str_per_round_split'] = process_per_round_dict(raw_fight['sig_str_per_round'])

    ## only select
    keep_keys = ['fight_name','results','fight_totals_split', 'totals_per_round_split', 'sig_str_split', 'sig_str_per_round_split']

    # Standard comprehension
    keep_dict = {k: raw_fight[k] for k in keep_keys if k in raw_fight}
    return keep_dict

def process_fights_in_event(raw_event):

    processed_event = {}

    for key, val in raw_event.items():

        ## every va
        if key in ['event_details', 'FUTURE EVENT']:

            processed_event[key] = val

        else:
            processed_val = process_fight(val)
            processed_event[key] = processed_val
        
    return processed_event


def merge_JsonFiles(data_dir, output_file_name):
    result = {}
    file_paths = glob.glob(data_dir + '/*.json')

    for file in file_paths:
        with open(file, 'r') as infile:
                raw_event = json.load(infile)
                # print(list(raw_event.keys())[0])
                # print(type(raw_event))
                processed_event = {}

                for key, val in raw_event.items():
                     if val == 'FUTURE EVENT':
                          continue

                     processed_event[key] = process_fights_in_event(val)
                # processed_event = process_fights_in_event(raw_event[list(raw_event.keys())[0]])
                
                result = result | processed_event

    with open(output_file_name, 'w') as output_file:
        json.dump(result, output_file, indent=4)
        
def reformat_per_round_dict(per_round_dict):

    reformatted_dict = {}

    for round_num, round_stats_dict in per_round_dict.items():

        for key, val in round_stats_dict.items():
            reformmatted_col_name = round_num + ' ' + key
            reformatted_dict[reformmatted_col_name] = val

    return reformatted_dict


def dataframe_row_from_fight(fight_dict):

    if 'ERROR' in fight_dict:
        return 'ERROR'

    df_name = pd.DataFrame({'matchup':[fight_dict['fight_name']]})
    df_results = pd.DataFrame([fight_dict['results']])
    df_fight_totals = pd.DataFrame([fight_dict['fight_totals_split']])
    df_totals_per_round = pd.DataFrame([reformat_per_round_dict(fight_dict['totals_per_round_split'])])
    df_sig_str = pd.DataFrame([fight_dict['sig_str_split']])
    df_sig_str_per_round = pd.DataFrame([reformat_per_round_dict(fight_dict['sig_str_per_round_split'])])

    df_full = pd.concat([df_name, df_results, df_fight_totals, df_totals_per_round, df_sig_str, df_sig_str_per_round], axis = 1)
    return df_full

def deduplicate_fight_df_row(fight_df_row, dup_cols):

    
    applicable_dup_cols = [col for col in dup_cols if col in fight_df_row.columns.tolist()]
    
    ## First, verify that the duplicate columns have the same value. If a duplicate column has different values, then we have a problem
    for dup_col in applicable_dup_cols:
        
        ## get both duplicate column rows for a given duplicate column name
        dup_df_val = fight_df_row[dup_col]

        if dup_df_val.iloc[0,0] == dup_df_val.iloc[0,1]:

            pass

        else:
            ## if the values are different, raise an error
            raise ValueError("duplicate columns have different values, there is a mismatch in the data")
    
    # Remove one of the duplicated columns
    fight_df_row_dedup = fight_df_row.loc[:,~fight_df_row.columns.duplicated()]

    return fight_df_row_dedup

def dataframe_from_event(event_dict, df_schema, dup_cols_list):

    event_df = df_schema.copy()

    for fight in event_dict:
        if fight != 'event_details':
            df_fight_row = dataframe_row_from_fight(event_dict[fight])

            # if df_fight_row == 'ERROR':

            if  isinstance(df_fight_row, pd.DataFrame):
                df_fight_row_dedup = deduplicate_fight_df_row(df_fight_row, dup_cols_list)
                event_df = pd.concat([event_df, df_fight_row_dedup], ignore_index = True)

            else:
                msg = "no data listed for fight: " + fight
                warnings.warn(msg)
                continue


    event_df['event_date'] = event_dict['event_details']['date']
    event_df['event_location'] = event_dict['event_details']['location']

    return event_df

def process_fight_data(merged_data_file, dup_cols_file, schema_file):

    with open(merged_data_file, 'r') as infile:
        events_data = json.load(infile)

    ## Load in the dup cols list
    dup_cols = pd.read_csv(dup_cols_file)
    dup_col_list = dup_cols['col_name'].tolist()

    ## Load in the df schema
    df_scehma_file = pd.read_csv(schema_file)
    df_cols = df_scehma_file['col_name'].tolist()
    df_schema = pd.DataFrame(columns = df_cols)

    ## initialize a df object for the full dataset
    proccessed_dat = df_schema.copy()

    ## iterate through every event in the data, convert each event to a dataframe and add it to the full data
    for event in events_data:

        # print(event)

        event_df = dataframe_from_event(events_data[event], df_schema, dup_col_list)

        proccessed_dat = pd.concat([proccessed_dat, event_df])

    return proccessed_dat
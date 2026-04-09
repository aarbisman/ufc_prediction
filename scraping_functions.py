import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from datetime import date, datetime
import json
import warnings

def make_soup(url) -> BeautifulSoup:
    '''makes a beautifulsoup object for a given url'''

    source_code = requests.get(url, allow_redirects=False)
    plain_text = source_code.text.encode("ascii", "replace")
    return BeautifulSoup(plain_text, "html.parser")


def get_all_event_details(soup_obj):
    '''gets all referencable event links from the main ufc event results website'''

    all_event_links = []

    for link in soup_obj.find_all("td", {"class": "b-statistics__table-col"}):
                for href in link.find_all("a"):
                    event_link = href.get("href")
                    all_event_links.append(event_link)

    return all_event_links

def get_all_fight_details(event_soup_obj):
    '''gets all fight detail links for a given event (multiple fights for every ufc event) '''
      
    fight_details_links = []

    for fight_detail in event_soup_obj.find_all("tr", {"class": "b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click"}):
        href = fight_detail.get("data-link")
        fight_details_links.append(href)

    return fight_details_links

def parse_agg_table(table, table_structure):
    '''parses the fight statistics tables that are aggregated '''

    ## get all of the table data and initialize the data dictionary
    table_data = table.find_all('td')
    table_dict = {}

    for i in range(len(table_data)):
        ## remove new lines and tabs for easier formatting
        table_data_clean = table_data[i].text.replace("\n\n", "").replace("      ","").replace("\n    \n","").rstrip()

        ## fill in the dictioanry for the corresponding structure
        table_dict[table_structure[i][0]] = table_data_clean.split(table_structure[i][1])

    return table_dict

def parse_per_round_table(per_round_table, per_round_table_structure):
    '''parses the fight statistisc tables that are split per round'''

    ## Save all table headers and table rows:
    headers_and_rows = per_round_table.find_all(['th','tr'])

    ## Initialize the stats_per_round_dict and round unumber object
    stats_per_round_dict = {}
    round = None

    for i in headers_and_rows:

        ## if the soup_item is a table header, set our round variable to it
        if i.name == 'th':
            round_num = i.text.replace("\n              ", "").replace("\n            ","")
            # print(round_num)

        ## if the soup_item is a table row, parse the table row data and assign it to the current round
        if i.name == 'tr':

             stats_per_round_dict[round_num] = parse_agg_table(i, per_round_table_structure)

    return stats_per_round_dict

def get_fight_result_details(fight_soup_obj, fight_results_table_structure, fight_id):
    '''gets the overall fight results kept at the top of the fight details page'''

    fight_results_dict = {}

    fight_results_table = fight_soup_obj.find_all('p',{'class:','b-fight-details__text'})[0].find_all("i",{'class:','b-fight-details__text-item_first','class:','b-fight-details__text-item'})
    fight_results_details = fight_soup_obj.find_all('p',{'class:','b-fight-details__text-item_first','class:','b-fight-details__text'})[1]


    for i in range(len(fight_results_table_structure)):

        results_split = fight_results_table[i].text.strip().split(fight_results_table_structure[i][1])

        ## this should give us 2 list items: ['category':'value'], if it does not return list items, raise a warning
        if len(results_split) != 2:
            msg = "unusual data format found: " + results_split[0] + "for fight: " + fight_id
            warnings.warn(msg)

        fight_results_dict[fight_results_table_structure[i][0]] = results_split[-1]


    results_details = fight_results_details.text.strip().replace("  ","").replace("\n","").split(":")
    if len(results_details) != 2:
            msg = "unusual data format found: " + results_details[0] + "for fight: " + fight_id
            warnings.warn(msg)

    fight_results_dict['details:'] = results_details[-1]


    return fight_results_dict

def extract_fight_data(fight_soup, totals_table_structure, sig_str_table_structure, fight_results_table_structure, fight_id):
    '''extracts all relevant fight data for a given ufc fight'''

    fight_tables = fight_soup.find_all("tbody")
    fight_data = {}

    ## get the fight results
    fight_result_details = get_fight_result_details(fight_soup, fight_results_table_structure, fight_id)
    fight_data['results'] = fight_result_details

    ## if there are no fight tables, return that to the user:
    if len(fight_tables) == 0:
         fight_data['error'] = 'no fight tables available'
         warning_len = str(len(fight_tables))
         warnings.warn(warning_len)

         return fight_data
    
    else:

        ## seperate fight data into different sections 
        totals_table = fight_tables[0]
        totals_per_round_tables = fight_tables[1]
        sig_str_table = fight_tables[2]
        sig_str_per_round_tables = fight_tables[3]

        ## combine per_round tables into a single table
        fight_data['fight_totals'] = parse_agg_table(totals_table, totals_table_structure)
        fight_data['totals_per_round'] = parse_per_round_table(totals_per_round_tables, totals_table_structure)

        ## sig_str table
        fight_data['sig_str'] = parse_agg_table(sig_str_table, sig_str_table_structure)
        fight_data['sig_str_per_round'] = parse_per_round_table(sig_str_per_round_tables, sig_str_table_structure)

        return fight_data

def get_fighters_names(fight_soup_obj):
    '''gets the name of the fight, displayed as fighter_a vs fighter_b'''

    fighter_a = fight_soup_obj.find_all('h3',{"class":"b-fight-details__person-name"})[0].text.replace(" \n","").replace("\n","")
    fighter_b = fight_soup_obj.find_all('h3',{"class":"b-fight-details__person-name"})[1].text.replace(" \n","").replace("\n","")

    fighters_names = fighter_a + ' vs ' + fighter_b

    return fighters_names

def get_event_specific_data(event_soup_obj):
    '''gets all relevant data about a specific ufc event'''

    ## to-do: add functionality to save event name
    event_specific_data_dict = {}

    event_details = [item.text for item in event_soup_obj.find_all('li',{"class":"b-list__box-list-item"})]

    ## extract the event date
    date_r = re.compile("Date")
    event_date = list(filter(date_r.search, event_details)) 
    ## raise error if list contains more than 1 element
    event_date_clean = event_date[0].replace("\n\n        ","").replace("\n    ","").replace("    "," ")


    ## extract the event location
    location_r = re.compile("Location")

    event_location = list(filter(location_r.search, event_details))
    ## raise error if list contains more than 1 element
    event_location_clean = event_location[0].replace("\n\n        ","").replace("\n    ","").replace("  \n  "," ")


    event_specific_data_dict['date'] = event_date_clean.replace("Date: ","")
    event_specific_data_dict['location'] = event_location_clean.replace("Location: ","")
    return event_specific_data_dict

def get_full_event_data(event_soup_object, totals_table_structure, sig_str_table_structure, fight_results_table_structure):
    '''gets all relevant data for a given ufc event, including statistics for each fight'''

    full_event_data_dict = {}

    ## gets the date and location of event
    full_event_data_dict['event_details'] = get_event_specific_data(event_soup_object)

    ## uses the fight dictionaries for each fight in the event
    fight_links_of_single_event = get_all_fight_details(event_soup_object)

    for fight_link in fight_links_of_single_event:
        fight_soup_obj = make_soup(fight_link)

        fight_name = get_fighters_names(fight_soup_obj)
        fight_id = fight_link.split('/fight-details/')[-1]

        fight_stats = extract_fight_data(fight_soup_obj, totals_table_structure, sig_str_table_structure, fight_results_table_structure, fight_id)
        fight_stats['fight_name'] = fight_name

        full_event_data_dict[fight_id] = fight_stats

    ## saves everything as a nested dictionary object
    return full_event_data_dict

def is_future_fight(event_soup):
    '''checks if a listed event is meant for the future or not'''

    event_date_string = get_event_specific_data(event_soup)['date']
    event_dt_object = datetime.strptime(event_date_string, "%B %d, %Y")
    event_date = event_dt_object.date()

    return date.today() < event_date


def get_event_data_from_links(event_link_set, totals_table_structure, sig_str_table_structure, fight_results_table_structure):

    event_history_data = {}

    for event_link in event_link_set:

        event_id = event_link.split('/event-details/')[-1]
        event_soup_obj = make_soup(event_link)

        ## if it is a future event, do not save any data
        if is_future_fight(event_soup_obj):
            event_history_data[event_id] = 'FUTURE EVENT'

        ## otherwise, save the event data
        else:
            event_data = get_full_event_data(event_soup_obj, totals_table_structure, sig_str_table_structure, fight_results_table_structure)
            event_history_data[event_id] = event_data

    return event_history_data

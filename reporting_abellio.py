import requests
import json
import logging
import time
import streamlit as st
import re
import pandas as pd 
from pandas import  ExcelWriter
from io import BytesIO
from datetime import date
import numpy as np
import os
from os import getcwd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timedelta
import streamlit_ext as ste
import collections
import xlsxwriter

import toml





#Get current working directory
cwd = os.getcwd()
#get json credentials for service account through Cred.json file 
#TODO: need a way to save this to a secrets.toml and also able to read json as a variable rather than a path, unsure this is supporred in args of 'from_json_keyfile_name',
#TODO: may need to pass each key through as individual variable from toml into ServiceAccountCredentials
#credentials = ServiceAccountCredentials.from_json_keyfile_name(cwd+'/Cred.json', scope)

secrets = toml.load('secrets_abellio.toml')


#Dictionary mapping domain-substring to client value (will be used in df mapping later on)

#TODO: may need other client names - only have partial set so far 
clients_dict = {
    'arriva-uk-bus': 'Arriva UK', 
    'sg': 'Stagecoach', 
    'firstbus': 'First Bus',
    'drrichard':'Dr Richard', 
    'abellio-uk':'Abellio UK'
}

#Info
st.subheader('Abellio - Reports')

#TODO: Save this to a secrets.toml file 
api_secrets_dict= secrets['api_secrets_dict']

#function to split URL into three substrings
def process_URL(schedule_URL):
    domain_name = re.sub("\.[^.]*", "", schedule_URL[8:])
    schedule_id = schedule_URL
    schedule_id = re.sub(r'^.*?(?=schedules/)', "", schedule_id)
    schedule_id = schedule_id[10:].split('/', -1)[0]
    project_id = schedule_URL
    project_id = re.sub(r'^.*?(?=project/)', "", project_id)
    project_id = project_id[8:].split('/', -1)[0]
    return domain_name, schedule_id , project_id

#Function to get client_id and secret based on the domain name key pasted in the URL
def generate_auth(domain_name, api_secrets_dict):
    client_id = api_secrets_dict[domain_name]["client_id"]
    client_secret = api_secrets_dict[domain_name]["client_secret"]
    return client_id, client_secret

#Function to use auth server endoiunt to get new token 
def get_new_token(client_id, client_secret, domain_name, a):
    auth_server_url = f"https://{domain_name}.optibus.co/api/v2/token"
    token_req_payload = {'grant_type': 'client_credentials'}
    if domain_name != "":
        token_response = requests.post(auth_server_url,
        data=token_req_payload, verify=False, allow_redirects=False,
        auth=(client_id, client_secret))          
        if token_response.status_code !=200:
            col1, col2 = st.columns([8,2])
            with col1:
                st.error(f"Failed to obtain token from the OAuth 2.0 server **{token_response.status_code}**")
            with col2:
                rerun = st.button('Retry')
                if rerun: 
                    st.experimental_rerun()
                else:
                    st.stop()
        else:
            #st.success(f"Successfuly obtained a new token for **{a} Schedule**")
            tokens = json.loads(token_response.text)
            return tokens['access_token']
    else:
        st.stop()

# Variable for download button
download_run_arriva = ''
download_run_optibus = ''

#return individual values from json dict
def get_duties(get_json):
    duty_count = get_json['stats']['crew_schedule_stats']['duties_count']
    return duty_count

#return individual values from json dict
def get_paid_time(get_json):
    paid_time = get_json['stats']['crew_schedule_stats']['paid_time']
    return paid_time

#calculation of avg paid time
def calculate_avg_paid_time(paid_time, duty_count):
    avg_paid_time = [paid_time[i]/duty_count[i] for i in range(len(paid_time))]
    return avg_paid_time

#return individual values from json dict
def get_platform_time(get_json):
    platform_time = get_json['stats']['vehicle_schedule_stats']['platform_time']
    return platform_time

#Calculation of schedule efficiency (FUNCTION SHOULD REALLY BE CHANGED TO calculate_sch_eff)
def get_sch_eff(platform_time, paid_time):
    efficiency = [(platform_time[i]/paid_time[i])*100 for i in range(len(platform_time))]
    return efficiency

#Calculation of efficiency difference 
def calculate_eff_diff(efficiency_ba, efficiency_op):
    eff_diff = [round(efficiency_op[i] - efficiency_ba[i], 2) for i in range(len(efficiency_op))]
    return eff_diff

#Calculation of duty count difference
def calculate_duty_diff(duty_count_ba, duty_count_op):
    duty_count_diff = int(duty_count_ba - duty_count_op)
    return duty_count_diff

#Calculation of paid time difference 
def calculate_paid_time_diff(paid_time_ba, paid_time_op):
    pt_diff = paid_time_ba - paid_time_op
    return pt_diff

#Converting minutes into a HH:MM string time format
def minutes_to_hours(minutes):
    # Calculate the number of hours
    hours = int(minutes // 60)
    # Calculate the number of remaining minutes
    remaining_minutes = int(minutes % 60)
    # Return the hours and minutes as a string, separated by a colon
    return f"{hours}:{remaining_minutes:02d}"

#Function to concatenate the values of one list with the values of another and return two lists, project name and also the concatenated list
def get_values(dict_list, key1, key2):
    return [d[key1] for d in dict_list], [d[key1] +' - '+ d[key2] for d in dict_list]

#get index of where deleted row should be 
def get_index(dict_list, key, value):
    for i, d in enumerate(dict_list):
        if d[key] == value:
            return i
    return -1



def get_days_of_week(get_json):
                dow = get_json['service']['daysOfWeek']
                return dow

def get_optibus_id(get_json):
    opId = get_json['scheduleSet']['optibusId']
    return opId 

def api_services_response(token, domain_name, optibus_id):
    api_call_headers = {'Authorization': 'Bearer ' + token}
    api_call_response = requests.get(f'https://{domain_name}.optibus.co/api/v2/schedule/{optibus_id}/services', headers=api_call_headers, verify=False)
    get_services_json = api_call_response.json()
    return get_services_json

def create_json_list(get_services_json, token, domain_name):
    emp_list = []
    for d in get_services_json:
        emp_list.append(api_header_response(token, domain_name, d['id']))
    return emp_list

def create_schedule_names(get_json):
    name = get_json['scheduleSet']['id']
    return name

def create_service_ids_list(json_data_list):
    list = []
    # Iterate through the list of dictionaries
    for sch_d in json_data_list:
        # Access the value of the 'list_key' key
        list_value = sch_d['service']['daysOfWeek']
        list.append(list_value)
    flat_list = []
    for sublist in list:
        for elements in sublist:
            flat_list.append(elements)

    return flat_list

def create_paid_time_list(json_data_list):
    # List to store the results
    paid_time_list = []
    paid_time_result_list = []

    # Iterate through the list of dictionaries
    for sch_d in json_data_list:
        # Multiply the value of the 'other_key' key by the length of the list
        result = sch_d['service']['stats']['crew_schedule_stats']['paid_time'] 
        paid_time_result_list += [result] * len(sch_d['service']['daysOfWeek'])

        # Append the result to the result list
        paid_time_list.append(result)

    paid_time_list_sum = sum(paid_time_list)
    return paid_time_list, paid_time_list_sum, paid_time_result_list

# Function to retrieve Paid Break time
def create_paid_break_time_list(json_data_list):
    result = []
    for d in json_data_list:
        inner_list = []
        for l in d['service']['stats']['crew_schedule_stats']['custom_time_definitions']:
            if l['name']=='Paid Break':
                inner_list.append(l['value'])
        result.append(sum(inner_list))
    return result

def get_duty_types(json_data_list,service_days):
    duty_type_list = []
    for d in json_data_list:
        duty_type_list.append(d['service']['stats']['crew_schedule_stats']['histograms']['duty_types'])
        # duty_type_df = pd.DataFrame.from_records(duty_type_list)
    duty_type_dic = dict(zip(service_days,duty_type_list)) 
    #duty_type_df = pd.DataFrame.from_dict(duty_type_dic, orient= 'columns')
    # Find the maximum number of items in the lists
    max_length = max(len(v) for v in duty_type_dic.values())

    # Pad the lists with NaN values
    for key in duty_type_dic:
        duty_type_dic[key] += [float('nan')] * (max_length - len(duty_type_dic[key]))

    # Create DataFrame
    duty_type_df = pd.DataFrame(duty_type_dic)
    
    return duty_type_df

def create_platform_time_list(json_data_list):
    # List to store the results
    platform_time_list = []
    platform_time_result_list = []

    # Iterate through the list of dictionaries
    for sch_d in json_data_list:
        # Access the value of the 'list_key' key
        list_value = sch_d['service']['daysOfWeek']

        # Get the length of the list
        list_length = len(list_value)
        result = sch_d['service']['stats']['vehicle_schedule_stats']['platform_time'] 
        for i in range (list_length):
            platform_time_result_list.append(result)

        # Append the result to the result list
        platform_time_list.append(result)

    paid_time_list_sum = sum(platform_time_list)
    return platform_time_list, paid_time_list_sum, platform_time_result_list

def create_duty_count_list(json_data_list):
    # List to store the results
    duty_count_list = []
    duty_count_result_list = []

    # Iterate through the list of dictionaries
    for sch_d in json_data_list:
        # Access the value of the 'list_key' key
        list_value = sch_d['service']['daysOfWeek']

        # Get the length of the list
        list_length = len(list_value)
        # Multiply the value of the 'other_key' key by the length of the list
        result = sch_d['service']['stats']['crew_schedule_stats']['duties_count']
        for i in range (list_length):
            duty_count_result_list.append(result)                            

        # Append the result to the result list
        duty_count_list.append(result)

    duty_count_list_sum = sum(duty_count_list)
    return duty_count_list, duty_count_list_sum, duty_count_result_list

def retrieve_service_groups(json_data_list):
    # List to store the results
    service_groups = []
    # Iterate through the list of dictionaries
    for sch_d in json_data_list:
        # Access the value of the 'list_key' key
        services= sch_d['service']['name']
        service_groups.append(services)  
    return service_groups
def change_to_hours(df,string):
    df.loc[string] = df.loc[string].apply(lambda x: f"{int(x/60)}:{int(abs(x/60-int(x/60))*60):02d}")

dow = None

tab2, tab3 = st.tabs([ 'Part 2b Report', 'Roster Details'])



with tab2:
    st.write('Part 2 B Data')
    with st.form('Custom Report'):

        #Project Name text input as can't pull it consistently from API - MUST not be blank - validation step later on on form submission
        depot_name = st.text_input('Name of Project', placeholder='Derby', key='g')
        #Text input for URL baseline 
        schedule_URL_baseline = st.text_input(label= 'Please type the baseline schedule URL here', placeholder='https://domain.optibus.co/project/t4bx3pnc0/schedules/oBAwkfaRv/gantt?type=duties')
       

        #function to process URL into substring variables used for API 
        domain_name_ba, schedule_id_ba , project_id_ba = process_URL(schedule_URL_baseline)


        #Check if text input is not blank
        if schedule_URL_baseline != '':
            #Get id and secret based on url that has been entered 
            client_id_baseline , client_secret_baseline= generate_auth(domain_name_ba, api_secrets_dict)

        
        
        #Form submit button
        submit = st.form_submit_button('Submit')
        #IF clicked
        if submit:
            #Check if project name is blank
            download_run_optibus = 'run'
            if not depot_name:
                #Info 
                st.warning("**Project Name** can't be left blank")

            #if all other conditions are met - continue to call the API 
            else:
                #Present progress bar 
                my_bar = st.progress(0)
                for percent_complete in range(100):
                    time.sleep(0.01)
                    my_bar.progress(percent_complete + 1)

                token_baseline = get_new_token(client_id_baseline, client_secret_baseline, domain_name_ba, 'Baseline')
    

                #get_json_test1 = api_header_response(token_baseline, domain_name_ba, schedule_id_ba)
                #st.write(get_json_test1)
                #Example to get the optibus ID from a schedule and then use servrices endpoint
                #.compensationtime
                def get_optibus_id(token, domain_name, schedule_id):
                    api_call_headers = {'Authorization': 'Bearer ' + token}
                    api_call_response = requests.get(f'https://{domain_name}.optibus.co/api/v2/schedules/meta?scheduleIds[]={schedule_id}&includeHidden=true&includeDeleted=true', headers=api_call_headers, verify=False)
                    get_json = api_call_response.json()
                    for d in get_json:
                        optibus_id = d['schedule']['optibusId']
                        dataset_id = d['dataset']['optibusId']
                    return optibus_id, dataset_id
                optibus_id_ba, dataset_id_ba  = get_optibus_id(token_baseline, domain_name_ba, schedule_id_ba)
             

                
            
                

                
                

                if 'status' in optibus_id_ba and optibus_id_ba['status'] == 500:
                    url_check = 'Baseline URL'
                    st.warning(f'There is an issue with **{url_check}**, please *Save a new version of the schedule* and try again, this is a known API issue. Please see message below for further details')
                    st.caption(optibus_id_ba)
                    st.stop()
              

                get_services_json_ba = api_services_response(token_baseline, domain_name_ba, optibus_id_ba)

                

                #&statProperties[]=crew_schedule_stats.paid_time&statProperties[]=general_stats&statProperties[]=relief_vehicle_schedule_stats&statProperties[]=relief_vehicle_schedule_stats
                #st.write(get_services_json_ba)
                #st.write(get_services_json_ba)

                def api_meta_response(token, domain_name, schedule_id):
                    api_call_headers = {'Authorization': 'Bearer ' + token}

                    #Stat property list
                    stat_properties = ["crew_schedule_stats.paid_time", 
                    "crew_schedule_stats.attendance_time", 
                    "crew_schedule_stats.custom_time_definitions", 
                    "crew_schedule_stats.depot_pull_time", 
                    "crew_schedule_stats.duties_count", 
                    "crew_schedule_stats.histograms", 
                    "crew_schedule_stats.length", 
                    "crew_schedule_stats.sign_off_time", 
                    "crew_schedule_stats.sign_on_time", 
                    "crew_schedule_stats.split_count", 
                    "vehicle_schedule_stats.depot_allocations", 
                    "vehicle_schedule_stats.driving_time", 
                    "vehicle_schedule_stats.platform_time", 
                    "vehicle_schedule_stats.pvr", 
                    "crew_schedule_stats.changeover_count", 
                    "crew_schedule_stats.standby_time", 
                    "crew_schedule_stats.algorithmic_cost", 
                    "crew_schedule_stats.custom_time_definitions"]

                    #Initial call without parameters
                    api_call = f'https://{domain_name}.optibus.co/api/v2/schedules/meta?scheduleIds[]={schedule_id}&includeHidden=true&includeDeleted=true'

                    #Iterate and append parameters to the stat_property component of api string
                    for property in stat_properties:
                        api_call += f'&statProperties[]={property}'

                        

                    api_call_response = requests.get(api_call, headers=api_call_headers, verify=False)

                    #OLD API STRING (QUITE DIFFICULT TO READ)
                    #api_call_response = requests.get(f'https://{domain_name}.optibus.co/api/v2/schedules/meta?scheduleIds[]={schedule_id}&includeHidden=true&includeDeleted=true&statProperties[]=crew_schedule_stats.paid_time&statProperties[]=crew_schedule_stats.attendance_time&statProperties[]=crew_schedule_stats.custom_time_definitions&statProperties[]=crew_schedule_stats.depot_pull_time&statProperties[]=crew_schedule_stats.duties_count&statProperties[]=crew_schedule_stats.histograms&statProperties[]=crew_schedule_stats.length&statProperties[]=crew_schedule_stats.sign_off_time&statProperties[]=crew_schedule_stats.sign_on_time&statProperties[]=crew_schedule_stats.split_count&statProperties[]=vehicle_schedule_stats.depot_allocations&statProperties[]=vehicle_schedule_stats.driving_time&statProperties[]=vehicle_schedule_stats.platform_time&statProperties[]=vehicle_schedule_stats.pvr', headers=api_call_headers, verify=False)

                    get_json = api_call_response.json()
                    return get_json
                
                
                
                
            
                def create_json_list(get_services_json, token, domain_name):
                    emp_list = []
                    exclude = ['NWD', '#SCH', 'NSCH']
                    for d in get_services_json:
                        if not any(substring in d['name'] for substring in exclude):
                            emp_list.append(api_meta_response(token, domain_name, d['id']))

                    flattened_list = [item for sublist in emp_list for item in sublist]
                    return flattened_list

                json_data_list_ba = create_json_list(get_services_json_ba, token_baseline, domain_name_ba)


            
                for key in clients_dict:
                        # check if the key is a substring of the string
                        if key in domain_name_ba:
                            # if it is, assign a new variable the corresponding value
                            client_instance = clients_dict[key]

                    #call functions defined earlier on to get specific data from the API - CAN ALWAYS BE UPDATED and points added 
                    #dow_ba = get_days_of_week(get_json_ba)
                    #dow_op = get_days_of_week(get_json_op)
                    #opId_ba = get_optibus_id(get_json_ba)
                    #opId_op = get_optibus_id(get_json_op)

                    

                    #st.write(json_data_list_ba)
                    



                    #json_data_list_ba = create_json_list(get_services_json_ba, token_baseline, domain_name_ba)
                    #json_data_list_op = create_json_list(get_services_json_op, token_optimisation, domain_name_op)


                    
                    #BASELINE : https://arriva-uk-bus-northwest.optibus.co/project/da336nrgv/schedules/EvltiNwWMS/gantt?type=duties
                    #OPTIMISATION: https://arriva-uk-bus-northwest.optibus.co/project/da336nrgv/schedules/bBIr4mZwjT/gantt?type=duties
                    #Baseline is inserting two service groups and optimisation inserting 3, so problematic as calculations are wrong, count number of list elements to match to mitigate this 
                    

                def catch_service_lists(json_data_list, key, key2):
                    result = []
                    for d in json_data_list:
                        result.extend(d.get(key, {}).get(key2, []))
                    return result

                check_serv_ba = catch_service_lists(json_data_list_ba, 'service', 'daysOfWeek')

                #


                def return_assciated_Serv_days(check_serv, string):
                    master_list = [2,3,4,5,6,7,1]
                    master_dict =  service_days_dict={1:'Sun',2:'Mon',3:'Tue',4:'Wed',5:'Thur',6:'Fri',7:'Sat'}
                    missing_elements = set(master_list) - set(check_serv)
                    missing_days = [master_dict[x] for x in missing_elements] 
                    return missing_days, string

                missing_days_ba, identifier_ba = return_assciated_Serv_days(check_serv_ba, 'Baseline')
               

                if len(missing_days_ba) != 0:
                    st.error(f"API Error Occuring for **{missing_days_ba}** on **{identifier_ba}** schedule for ****")
                    st.stop()
            
                service_groups_ba = retrieve_service_groups(json_data_list_ba)


                


                list_ba = create_service_ids_list(json_data_list_ba)


                

                
                #TODO: Iterate through histograms and create buckets by the keys by each service day first value in the list is the timeband (8hr ie and second value is the count)
            

                

                # create an empty dictionary to store the dataframes
                dfs = {}
                df_list = []

                # loop through each dataset in json_data_list_ba
                for i, data_dict in enumerate(json_data_list_ba):
                    try:
                        name = data_dict['service']['name']
                        data = data_dict['service']['stats']['crew_schedule_stats']['histograms']['duty_paid_time']
                    except KeyError:
                        st.warning(f"Duty_paid_time key not found in json get request for dataset {i}, please check the schedule")
                        continue

                    bins = [0, 5, 7, 9, 12]
                    labels = ['0:01 - 5:00', '5:01 - 7:00', '7:01 - 9:00', '9:01 - 12:00', '12:01 +']
                    counts = [0]*len(labels)

                   
                    for category, count in data:
                        category = int(category)  # cast category to an integer
                        if category < 6 :
                            counts[0] += count
                        elif category < 8:
                            counts[1] += count
                        elif category < 10:
                            counts[2] += count
                        elif category < 13:
                            counts[3] += count
                        else:
                            counts[4] += count

                  
                
                    # convert the counts to a dataframe with the categories as the index
                    df = pd.DataFrame({'Count': counts, 'Service':name}, index=labels)

                    df_list.append(df)
                

                merged_df = pd.concat(df_list, keys=[df['Service'].iloc[0] for df in df_list])
                merged_df  = merged_df.reset_index()
                merged_df  = merged_df.rename(columns={'level_1': 'Category'})
                

                #merged_df = merged_df.rename(columns={merged_df.iloc[:, 0].name: 'Category'})

                
                pivoted_df = pd.pivot_table(merged_df, index='Service', columns='Category', values='Count')

                pivoted_df = pivoted_df.reindex(columns=labels)
                
                

                # create an empty dictionary to store the dataframes
                dfs_meal_break = {}

                # create an empty dictionary to store the meal break sums for each service name
                meal_break_sum = {}

                # loop through each dataset in json_data_list_ba
                for i, data_dict in enumerate(json_data_list_ba):
                    try:
                        name = data_dict['service']['name']
                        data_duties = data_dict['service']['stats']['crew_schedule_stats']['duties_count']
                        meal_breaks = data_dict['service']['stats']['crew_schedule_stats']['custom_time_definitions']
                        
                        # initialize meal break sum for the service name to 0
                        meal_break_sum[name] = 0
                        
                        # loop through each meal break data for the service name
                        for meal_break_data in meal_breaks:
                            if meal_break_data['name'] == 'Meal break':
                                meal_break_sum[name] += meal_break_data['value']
                        
                        # calculate average meal break duration in minutes
                        avg_meal_break = meal_break_sum[name] / data_duties
                        
                        # convert average meal break duration to hours and minutes format
                        avg_meal_break_hours = int(avg_meal_break / 60)
                        avg_meal_break_minutes = int(avg_meal_break % 60)
                        avg_meal_break_str = f"{avg_meal_break_hours:02d}:{avg_meal_break_minutes:02d}"
                        
                        # add average meal break duration to the dataframe for the service name
                        if name not in dfs_meal_break:
                            dfs_meal_break[name] = {}
                        dfs_meal_break[name][i] = avg_meal_break_str
                        
                    except KeyError:
                        st.warning(f"duties_count or meal break data not found in json get request for dataset {i}, please check the schedule")
                        continue
                
                
                df = pd.DataFrame.from_dict(dfs_meal_break, orient='index').rename(columns={'0': 'Average Meal Break'})


                df = df.stack().reset_index().dropna()[['level_0', 0]]
                df.columns = ['index', 'Average Meal Break']

                df = df.reset_index().set_index('index').rename(columns={'0': 'Average Meal Break'})

                df = df.drop(columns=['level_0'])

                merged_df = pd.merge(pivoted_df, df, left_index=True, right_index=True)

             


                        # create an empty dictionary to store the dataframes
            dfs_paid_time = {}

            # create an empty dictionary to store the average paid time for each service day
            avg_paid_time = {}

            # loop through each dataset in json_data_list_ba
            for i, data_dict in enumerate(json_data_list_ba):
                try:
                    name = data_dict['service']['name']
                    duty_count = data_dict['service']['stats']['crew_schedule_stats']['duties_count']
                    paid_time = data_dict['service']['stats']['crew_schedule_stats']['paid_time']
                    
                    # calculate average paid time for the service day
                    avg_paid_time[name] = paid_time / duty_count
                    
                    # convert average paid time to hours and minutes format
                    avg_paid_time_hours = int(avg_paid_time[name] / 60)
                    avg_paid_time_minutes = int(avg_paid_time[name] % 60)
                    avg_paid_time_str = f"{avg_paid_time_hours:02d}:{avg_paid_time_minutes:02d}"
                    
                    # add average paid time to the dataframe for the service day
                    if name not in dfs_paid_time:
                        dfs_paid_time[name] = {}
                    dfs_paid_time[name][i] = avg_paid_time_str
                    
                except KeyError:
                    st.warning(f"duties_count or paid time data not found in json get request for dataset {i}, please check the schedule")
                    continue
                            

            df_paid = pd.DataFrame.from_dict(dfs_paid_time, orient='index').rename(columns={'0': 'Average Paid Time'})


            df_paid = df_paid.stack().reset_index().dropna()[['level_0', 0]]
            df_paid.columns = ['index', 'Average Paid Time']

            df_paid = df_paid.reset_index().set_index('index').rename(columns={'0': 'Average Paid Time'})

            df_paid = df_paid.drop(columns=['level_0'])

            merged_df_final = pd.merge(merged_df, df_paid, left_index=True, right_index=True)

            st.write(merged_df_final)

            merged_df_excel = merged_df_final.copy()
            
            merged_df_excel = merged_df_excel.reset_index(drop=False)

            # rename the index column to "id"
            merged_df_excel = merged_df_excel.rename(columns={'index': 'id'})

            transend = len(merged_df_excel)+4

            buffer3 = BytesIO()
            with ExcelWriter(buffer3,engine='xlsxwriter') as writer:
                merged_df_excel.to_excel(writer,sheet_name='Results',index=0, startrow=3)
                workbook = writer.book # Access the workbook
                worksheet= writer.sheets['Results'] # Access the Worksheet
                                # Define a format with a thin border
                border_format = workbook.add_format({'border': 1})

                format = workbook.add_format({'border': 1})
                worksheet.conditional_format('A3:H'+str(transend), {'type': 'no_blanks','format': format})

                
                worksheet.write(2, 1, 'Paid Time by Hour', workbook.add_format({'align': 'center', 'bold': True, 'valign': 'vcenter','bg_color': '#FFEED3'}))
                worksheet.merge_range(2, 1, 2, 5, '')
                worksheet.write(2, 6, 'Average Stats', workbook.add_format({'align': 'center', 'bold': True, 'valign': 'vcenter', 'bg_color': '#C6EFCE'}))
                worksheet.merge_range(2, 6, 2, 7, '')

                # add the Service Day header
                worksheet.write(2, 0, 'Service Day', workbook.add_format({'bold': True}))

                                # Set the format for the blue color
                format_grey = workbook.add_format({'bg_color': '#ECECEC'})

                # Set the format for the white color


                # Set the blue color for cells A2 to A6
                worksheet.conditional_format('A3:A8', {'type': 'no_blanks', 'format': format_grey})

                

                for i, col in enumerate(merged_df_excel.columns):
                    column_len = merged_df_excel[col].astype(str).map(len).max()
                    column_len = max(column_len, len(col))
                    worksheet.set_column(i, i, column_len + 2)  # add extra space
                #TODO: Add change to hours format to download output
                workbook.close()

                dow = 'first'  

if dow == 'first':
    st.download_button(label='Download Excel', data=buffer3, file_name=f'{depot_name}part2b.xlsx', mime='application/vnd.ms-excel')

with tab3:
    def process_URL(schedule_URL):
        domain_name = re.sub("\.[^.]*", "", schedule_URL[8:])
        schedule_id = schedule_URL
        schedule_id = re.search(r'(?<=rosterSchedules\/)[^/]+', schedule_URL).group(0)
        project_id = schedule_URL
        project_id = re.sub(r'^.*?(?=project/)', "", project_id)
        project_id = project_id[8:].split('/', -1)[0]
        return domain_name, schedule_id , project_id
    def generate_auth(domain_name, api_secrets_dict):
        client_id = api_secrets_dict[domain_name]["client_id"]
        client_secret = api_secrets_dict[domain_name]["client_secret"]
        return client_id, client_secret
    def get_new_token(client_id, client_secret, domain_name, a):
        auth_server_url = f"https://{domain_name}.optibus.co/api/v2/token"
        token_req_payload = {'grant_type': 'client_credentials'}
        if domain_name != "":
            token_response = requests.post(auth_server_url,
            data=token_req_payload, verify=False, allow_redirects=False,
            auth=(client_id, client_secret))          
            if token_response.status_code !=200:
                col1, col2 = st.columns([8,2])
                with col1:
                    st.error(f"Failed to obtain token from the OAuth 2.0 server **{token_response.status_code}**")
                with col2:
                    rerun = st.button('Retry')
                    if rerun: 
                        st.experimental_rerun()
                    else:
                        st.stop()
            else:
                #st.success(f"Successfuly obtained a new token for **{a} Schedule**")
                tokens = json.loads(token_response.text)
                return tokens['access_token']
        else:
            st.stop()

    with st.form('Custom Report 2'):

        #Project Name text input as can't pull it consistently from API - MUST not be blank - validation step later on on form submission
        depot_name = st.text_input('Name of Project', placeholder='Derby', key='h')
        #Text input for URL baseline 
        schedule_URL_baseline = st.text_input(label= 'Please type the baseline schedule URL here', placeholder='https://domain.optibus.co/project/t4bx3pnc0/schedules/oBAwkfaRv/gantt?type=duties')
        
        if schedule_URL_baseline != '':
            #function to process URL into substring variables used for API 
            domain_name_ba, schedule_id_ba , project_id_ba = process_URL(schedule_URL_baseline)


        #Check if text input is not blank
        
            #Get id and secret based on url that has been entered 
            client_id_baseline , client_secret_baseline= generate_auth(domain_name_ba, api_secrets_dict)

        
        
        #Form submit button
        submit = st.form_submit_button('Submit')
        #IF clicked
        if submit:
            #Check if project name is blank
            download_run_optibus = 'run'
            if not depot_name:
                #Info 
                st.warning("**Project Name** can't be left blank")

            #if all other conditions are met - continue to call the API 
            else:
                #Present progress bar 
                my_bar = st.progress(0)
                for percent_complete in range(100):
                    time.sleep(0.01)
                    my_bar.progress(percent_complete + 1)

                token_baseline = get_new_token(client_id_baseline, client_secret_baseline, domain_name_ba, 'Baseline')

                def get_roster_json(token, domain_name, schedule_id):
                        api_call_headers = {'Authorization': 'Bearer ' + token}
                        api_call_response = requests.get(f'https://{domain_name}.optibus.co/api/v2/rosterSchedule/{schedule_id}', headers=api_call_headers, verify=False)
                        get_json = api_call_response.json()
                        
                        return get_json
                get_json  = get_roster_json(token_baseline, domain_name_ba, schedule_id_ba)

                #st.write(get_json)

                # Access the "rows" key and its first dictionary
                rows = get_json["groups"][0]["weeks"][0]["rows"]

                # Create an empty list to store the dictionaries
                result = []

                for row in rows:

                    # Iterate over each task in the "tasks" list
                    for task in row["tasks"]:
                        # Extract the "name" and "paidTime" values and create a new dictionary
                        task_type = task["task"].get("type")
                        if task_type != "Day Off":
                            service_name = task["task"].get("service", {}).get("name")
                            paid_time = task["task"]["paidTime"]
                        else:
                            continue

                        new_dict = {service_name: paid_time}
                        
                        # Append the new dictionary to the result list
                        result.append(new_dict)

                    # Print the final list of dictionaries
                

                # Create an empty dictionary to store the final data
                for d in result:
                    for key in d:
                        value = d[key] / 3600  # Convert seconds to hours
                        hours = int(value)
                        minutes = int((value - hours) * 60)
                        d[key] = f"{hours:02d}:{minutes:02d}"

                # Create dataframe
                df = pd.DataFrame(result)
                

                
                
                # Create an empty list to store the dictionaries
                result2 = []

                # Iterate over each group in the "groups" list
                for group in get_json["groups"]:
                    group_name = group["name"]
                    # Iterate over each week in the "weeks" list
                    for week in group["weeks"]:
                        # Iterate over each row in the "rows" list
                        for row in week["rows"]:
                            row_name = row["name"]
                            # Create a new dictionary for the row
                            row_dict = {"roster_id": row_name, "roster_group": group_name}
                            # Extract the "paidTime" values from each task in the "tasks" list
                            for task in row["tasks"]:
                                task_type = task["task"].get("type")
                                if task_type != "Day Off":
                                    service_name = task["task"].get("service", {}).get("name")
                                    paid_time = task["task"].get("paidTime")
                                
                                else:
                                    continue

                                # Add the "service_name: paid_time" pair to the row dictionary
                                row_dict[service_name] = paid_time

                            # Append the row dictionary to the result list
                            result2.append(row_dict)

                

                df2 = pd.DataFrame(result2)

                def convert_to_hhmm(df):
                    for col in df.columns:
                        if df[col].dtype == 'float64' or df[col].dtype == 'int64':
                            # Convert numeric values to minutes
                            df[col] = df[col].fillna(0).astype(int) // 60
                            # Convert minutes to hh:mm format
                            df[col] = df[col].apply(lambda x: '' if x == 0 else '{:02d}:{:02d}'.format(x // 60, x % 60))
                    return df

                df2 = convert_to_hhmm(df2)

                # skip blank values
                #df2 = df2.dropna(how="all")
                df2 = df2.loc[:, (df2 != '').any(axis=0)]

                st.write(df2)
                buffer4 = BytesIO()
                with ExcelWriter(buffer4,engine='xlsxwriter') as writer:
                    df2.to_excel(writer,sheet_name='Roster Details',index=0, startrow=3)
                    
                   

                dow = 'second'  

if dow == 'second':
    st.download_button(label='Download Excel', data=buffer4, file_name=f'{depot_name}rosterdetails.xlsx', mime='application/vnd.ms-excel')
            

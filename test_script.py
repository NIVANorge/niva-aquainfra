# Following example:
# https://github.com/AstraLabuce/aquainfra-usecase-Daugava/blob/containerize/pygeoapi_documentation/test_post_requests.py


import requests
import time
import sys
import os

'''
This is just a little script to test whether the OGC processing
services of the AquaINFRA project NIVA use case were properly
installed using pygeoapi and run as expected.
This does not test any edge cases, just a very basic setup. The input
data may already be on the server, so proper downloading is not 
guaranteed.

Check the repository here:
https://github.com/AstraLabuce/aquainfra-usecase-Daugava/

Merret Buurman (IGB Berlin), 2026-04-09
'''

base_url = os.getenv('PYSERVER') # e.g. "our.server.de/pygeoapi"
url_waterbody = os.getenv('URL_WATERBODY') # e.g. "our.server.de/pygeoapi"
# In Linux, define by running:
# export PYSERVER="our.server.de/pygeoapi"
# export URL_WATERBODY="our.server.de/waterbody/vannforekomster.zip"
base_url = f'https://{base_url}'
print(f'TESTING THIS SERVER: {base_url}')
headers_sync = {'Content-Type': 'application/json'}
headers_async = {'Content-Type': 'application/json', 'Prefer': 'respond-async'}


# Get started...
session = requests.Session()
result_ferrybox_url = None
result_scatter_station_plot_url = None
result_tile_plot_url = None
result_riverdata_url = None
result_assessment_area_plot_url = None
result_joined_url = None



force_async = False

# Define helper for polling for asynchronous results
def poll_for_json_result(resp201, session, seconds_polling=2, max_seconds=60*60):
    link_to_result = poll_for_links(resp201, session, 'application/json', seconds_polling, max_seconds)
    result_application_json = session.get(link_to_result)
    #print('The result JSON document: %s' % result_application_json.json())
    return result_application_json.json()

def poll_for_links(resp201, session, required_type='application/json', seconds_polling=2, max_seconds=60*60):
    # Returns link to result in required_type

    if not resp201.status_code == 201:
        print(f'[ERROR] This should return HTTP status 201, but we got: {resp201.status_code}.')

    print(f'[async] polling for status at: {resp201.headers['location']}')
    print(f'[async] polling every {seconds_polling} seconds...')
    seconds_passed = 0
    polling_url = resp201.headers['location']
    while True:
        polling_result = session.get(polling_url)
        job_status = polling_result.json()['status'].lower()
        print(f'[async] job status: {job_status}')

        if job_status == 'accepted' or job_status == 'running':
            if seconds_passed >= max_seconds:
                print(f'[ERROR] Polled for {max_seconds} seconds, giving up...')
            else:
                time.sleep(seconds_polling)
                seconds_passed += seconds_polling

        elif job_status == 'failed':
            print(f'[ERROR] job failed after {seconds_passed} seconds!')
            print(f'[ERROR] ################################# FAILURE #################################')
            print(f'[ERROR] ### debug info: {polling_result.json()}')
            print('Stopping.')
            sys.exit(1)

        elif job_status == 'successful':
            print(f'[async] job successful after {seconds_passed} seconds!')
            links_to_results = polling_result.json()['links']
            #print('[async] Links to results: {links_to_results}')
            print(f'[async] picking link of type "{required_type}" from {len(links_to_results)} result links.')
            link_types = []
            for link in links_to_results:
                link_types.append(link['type'])
                if link['type'] == required_type:
                    #print(f'[async] We pick this one (type {required_type}): {link['href']}')
                    link_to_result = link['href']
                    return link_to_result

            print(f'[ERROR] ################################# FAILURE #################################')
            print(f'[ERROR] ### did not find a link of type "{required_type}"! Only: {link_types}')
            print(f'[ERROR] ### debug info: {polling_result.json()}')
            print('Stopping.')
            sys.exit(1)

        else:
            print(f'[ERROR] ################################# FAILURE #################################')
            print(f'[ERROR] ### could not understand job status: {polling_result.json()['status'].lower()}')
            print(f'[ERROR] ### debug info: {polling_result.json()}')
            print('Stopping.')
            sys.exit(1)


# Define request function
def execute_and_retrieve_result(base_url, process_id, inputs, output_name, force_async=False):
    #print(f'______________________________________________')
    print(f'\n\n{process_id}')
    url = f'{base_url}/processes/{process_id}/execution'

    # Add a nesting level:
    inputs = {"inputs": inputs}

    # Try sync:
    print(f'[sync]  request to: {process_id}')
    resp = session.post(url, headers=headers_sync, json=inputs)
    print(f'[sync]  request to: {process_id}: HTTP {resp.status_code}') # should be HTTP 200

    # Handle success:
    if resp.status_code == 200:
        result_application_json = resp.json()
        print(f'[sync]  response: {result_application_json}')

    # Handle error during sync:
    if not resp.status_code == 200 and not resp.status_code == 504:
        try:
            print(f'[ERROR] ################################# FAILURE #################################')
            print(f'[ERROR] ### HTTP: {resp.status_code}')
            print(f'[ERROR] ### response: {resp.json()}')
            print('Stopping.')
            sys.exit(1)
        except Exception as e:
            print(f'[ERROR] ################################# FAILURE #################################')
            print(f'[ERROR] ### ran into error: {e}')
            print('Stopping.')
            sys.exit(1)

    # Handle Gateway timeout
    if resp.status_code == 504 or force_async:
        print(f'[async] request to: {process_id}')
        resp = session.post(url, headers=headers_async, json=inputs)
        print(f'[async] request to: {process_id}: HTTP {resp.status_code}') # should be HTTP 201
        result_application_json = poll_for_json_result(resp, session)
        print(f'[async] response: {result_application_json}')

    # Get link to result file (sync / async, does not matter):
    resultlink = result_application_json['outputs'][output_name]['href']
    print(f'[res]   result link: {resultlink}')

    # Get result file:
    final_result = session.get(resultlink)
    final_result.raise_for_status()
    print('[res]   result content: %s...' % str(final_result.content)[0:200])

    # Return URL for next one:
    return resultlink


################
### Sequence ###
################

'''
First, generate "ferrybox.csv" from THREDDS server:
(1) netcdf_extract_fb_data  --> ferrybox.csv

Based on that, you can create a scatter plot, or a tile plot:
(2) ferrybox.csv --> netcdf_scatter_station_plot --> PNG
(3) ferrybox.csv --> netcdf_tile_plot --> PNG

Then, retrieve river data:
(4) netcdf_logger_extract   --> logger.csv

With ferrybox data and river data, you can plot the assessment area:
(5) ferrybox.csv+logger.csv --> netcdf_assessment_area --> PNG

You can also join them both;
(6) ferrybox.csv+logger.csv --> netcdf_join_dataframes --> joined.csv

With that joined one, you can create another scatter plot:
(7) joined.csv --> netcdf_scatter_datax_vs_datay --> PNG

Summary: Seven runs and how they are connected:
(1) --> (2)
(1) --> (3)
(1)+(4) --> (5)
(1)+(4) --> (6) --> (7)

'''



###################################
### (1a) extract (without bbox) ###
###################################

process_id = "netcdf-extract-fb-data"
output_name = "csv_results"
inputs = {
    "url_thredds": "https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc",
    "start_date": "2023-01-01",
    "end_date": "2023-12-31",
    "parameters": ["temperature", "salinity", "oxygen_sat", "chlorophyll", "turbidity", "fdom"]
}
result_ferrybox_url = execute_and_retrieve_result(base_url, process_id, inputs, output_name, force_async)


################################
### (1a) extract (with bbox) ###
################################

process_id = "netcdf-extract-fb-data"
output_name = "csv_results"
inputs = {
    "url_thredds": "https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc",
    "study_area_bbox": {"bbox": [58.5, 9.5, 59.9, 11.9]},
    "start_date": "2023-01-01",
    "end_date": "2023-12-31",
    "parameters": ["temperature", "salinity", "oxygen_sat", "chlorophyll", "turbidity", "fdom"]
}
result_ferrybox_url = execute_and_retrieve_result(base_url, process_id, inputs, output_name, force_async)


################################
### (2) scatter station plot ###
################################

process_id = "netcdf-scatter-plot"
output_name = "scatter_plot"
inputs = {
    "url_input_csv": result_ferrybox_url,
    "param1": "chlorophyll",
    "param2": "salinity"
}
result_scatter_station_plot_url = execute_and_retrieve_result(base_url, process_id, inputs, output_name, force_async)


#####################
### (3) tile plot ###
#####################

process_id = "netcdf-tile-plot"
output_name = "tile_plot"
inputs = {
    "url_input_csv": result_ferrybox_url,
    "start_date": "2023-01-01",
    "end_date": "2023-12-31",
    "parameters": ["salinity", "chlorophyll"],
    "storm_date": "2023-08-08"
}
result_tile_plot_url = execute_and_retrieve_result(base_url, process_id, inputs, output_name, force_async)

###############################
### (4a) extract river data ###
### with parameters         ###
###############################

process_id = "netcdf-logger-extract"
output_name = "output_csv"
inputs = {
    "url_thredds": "https://thredds.niva.no/thredds/dodsC/datasets/loggers/glomma/baterod.nc",
    "parameters": ["temp_water_avg", "phvalue_avg", "condvalue_avg", "turbidity_avg", "cdomdigitalfinal"],
    "start_date": "2023-01-01",
    "end_date": "2023-12-31"
}
result_riverdata_url = execute_and_retrieve_result(base_url, process_id, inputs, output_name, force_async)

###############################
### (4b) extract river data ###
### without parameters      ###
###############################

process_id = "netcdf-logger-extract"
output_name = "output_csv"
inputs = {
    "url_thredds": "https://thredds.niva.no/thredds/dodsC/datasets/loggers/glomma/baterod.nc",
    "start_date": "2023-01-01",
    "end_date": "2023-12-31"
}
result_riverdata_url = execute_and_retrieve_result(base_url, process_id, inputs, output_name, force_async)


#################################
### (5a) plot assessment area ###
### without waterbody         ###
#################################

process_id = "netcdf-assessment-area"
output_name = "assessment_area"
inputs = {
    "url_input_csv": result_ferrybox_url,
    "url_input_river_logger_csv": result_riverdata_url,
    "river_label_col": "station_name"
}
result_assessment_area_plot_url = execute_and_retrieve_result(base_url, process_id, inputs, output_name, force_async)

#################################
### (5b) plot assessment area ###
### with waterbody            ###
#################################

process_id = "netcdf-assessment-area"
output_name = "assessment_area"
inputs = {
    "url_input_csv": result_ferrybox_url,
    "url_input_river_logger_csv": result_riverdata_url,
    "river_label_col": "station_name",
    "url_input_waterbody": url_waterbody,
    "study_area_layer": "VannforekomstKyst"

}
result_assessment_area_plot_url = execute_and_retrieve_result(base_url, process_id, inputs, output_name, force_async)


###########################
### (6) join dataframes ###
###########################

process_id = "netcdf-join-dataframes"
output_name = "joined_csv"
inputs = {
    "url_input_ferrybox_csv": result_ferrybox_url,
    "url_input_river_logger_csv": result_riverdata_url,
    "param_dataframe1": "turbidity",
    "param_dataframe2": "turbidity_avg",
    "colname_station2": "station_name",
    "colname_station_filter2": "Baterod",
    "colname_time1": "datetime",
    "colname_time2": "datetime"
}
result_joined_url = execute_and_retrieve_result(base_url, process_id, inputs, output_name, force_async)


############################################
### (7a) scatterplot with latitude range ###
############################################

process_id = "netcdf-scatter-datax-vs-datay"
output_name = "scatter_plot"
inputs = {
    "url_input_csv": result_joined_url,
    "latitude_min": 55.0,
    "latitude_max": 60.0
}
result_scatter2 = execute_and_retrieve_result(base_url, process_id, inputs, output_name, force_async)


############################################
### (7b) scatterplot with latitude range ###
############################################

'''
# TODO: Does not work yet, which ids to use
# TODO: When I provide the waterbody, do we still need lat range?

process_id = "netcdf-scatter-datax-vs-datay"
output_name = "scatter_plot"
inputs = {
    "url_input_csv": result_joined_url,
    "url_input_waterbody": url_waterbody,
    "study_area_layer": "VannforekomstKyst",
    "waterbody_ids_to_summarize": ["id1", "id2", "id3"],
    "waterbody_id_col": "id3",
    "latitude_min": 59.1,
    "latitude_max": 59.2
}
result_scatter2 = execute_and_retrieve_result(base_url, process_id, inputs, output_name, force_async)
'''

###################
### Finally ... ###
###################
print('\nDone!')



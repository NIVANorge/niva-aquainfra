import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import json
import os
import traceback
import datetime
import requests
# niva repo has hyphen in it, so we cannot import it in the normal python way:
#from pygeoapi.process.niva-aquainfra.pygeoapi_processes.docker_utils import run_docker_container3
import importlib  
docker_utils = importlib.import_module("pygeoapi.process.niva-aquainfra.pygeoapi_processes.docker_utils")


'''
# NOT TESTED YET
curl -X POST https://${PYSERVER}/processes/netcdf-join-dataframes/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "url_input_ferrybox_csv": "https://csv-output-from-extraction-process.csv",
        "url_input_river_logger_csv": "https://bla-csv",
        "param_dataframe1": "turbidity",
        "param_dataframe2": "turbidity_avg",
        "colname_station2": "station_name",
        "colname_station_filter2": "Baterod",
        "colname_time1": "datetime",
        "colname_time2": "datetime"
    }
}'; date
'''


# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class NivaNetcdfJoinDataframesProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.job_id = None
        self.process_id = self.metadata["id"]
        config_file_path = os.environ.get('AQUAINFRA_CONFIG_FILE', "./config.json")
        with open(config_file_path, 'r') as config_file:
            config = json.load(config_file)
            self.download_dir = config["download_dir"].rstrip('/')
            self.download_url = config["download_url"].rstrip('/')
            self.docker_executable = config["docker_executable"]
            self.image_name = "ferry-rscripts:20260317-fc29281"
            self.script_name = 'netcdf_join_dataframes.R'


    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def __repr__(self):
        return f'<NivaNetcdfJoinDataframesProcessor> {self.name}'


    def execute(self, data):
        LOGGER.info(f'Starting process NIVA Ferrybox: {self.script_name}')
        try:
            mimetype, result = self._execute(data)
            return mimetype, result

        except Exception as e:
            err_msg = str(e)
            LOGGER.error(f'Error during execute: {err_msg}')
            # This seems to look the same:
            #LOGGER.error(f'Error during execute: {e}')
            LOGGER.error('TRACEBACK:')
            print(traceback.format_exc())
            raise ProcessorExecuteError(err_msg) from e


    def _execute(self, data):

        ##############
        ### Inputs ###
        ##############

        # Retrieve user inputs:
        url_input_ferrybox_csv = data.get('url_input_ferrybox_csv')
        url_input_river_logger_csv = data.get('url_input_river_logger_csv')
        param_dataframe1 = data.get('param_dataframe1')
        param_dataframe2 = data.get('param_dataframe1')
        colname_station2 = data.get('colname_station2')
        colname_station_filter2 = data.get('colname_station_filter2')
        colname_time1 = data.get('colname_time1')
        colname_time2 = data.get('colname_time2')

        # Check user inputs:
        if url_input_ferrybox_csv is None:
            raise ProcessorExecuteError("Missing parameter 'url_input_ferrybox_csv'. Please provide a URL.")
        if url_input_river_logger_csv is None:
            raise ProcessorExecuteError("Missing parameter 'url_input_river_logger_csv'. Please provide a URL.")

        # Check existence:
        requests.head(url_input_ferrybox_csv), raise_for_status()
        requests.head(url_input_river_logger_csv), raise_for_status()

        ##################
        ### Input data ###
        ##################

        # Where to store input data (will be mounted read-write into container):
        #input_dir = f'{self.download_dir}/in/{self.process_id}/job_{self.job_id}'
        #os.makedirs(input_dir, exist_ok=True)
        # Not needed, no input data is downloaded!
        input_dir = None

        # Directory where static input data can be found (will be mounted readonly into container):
        # Not needed, no input data is downloaded!
        readonly_dir = None


        ###############
        ### Outputs ###
        ###############

        # Where to store output data
        output_dir = f'{self.download_dir}/out/{self.process_id}/job_{self.job_id}'
        output_url = f'{self.download_url}/out/{self.process_id}/job_{self.job_id}'
        os.makedirs(output_dir, exist_ok=True)
        LOGGER.debug(f'All results will be stored     in: {output_dir}')
        LOGGER.debug(f'All results will be accessible in: {output_url}')
        # Output filename
        out_result_path = f'{output_dir}/joined_{self.job_id}.csv'
        out_result_url  = f'{output_url}/joined_{self.job_id}.csv'

        ###########
        ### Run ###
        ###########

        r_args = [
            url_input_ferrybox_csv,
            url_input_river_logger_csv,
            param_dataframe1,
            param_dataframe2,
            colname_station2,
            colname_station_filter2,
            colname_time1,
            colname_time2,
            out_result_path
        ]

        r_args = [url_input_csv, out_result_path, url_input_river_logger_csv, url_input_waterbody]
        LOGGER.debug(f"r_args: {r_args}")
        returncode, stdout, stderr, user_err_msg = docker_utils.run_docker_container3(
            self.docker_executable,
            self.image_name,
            self.script_name,
            output_dir,
            r_args
        )

        # Return R error message if exit code not 0:
        if not returncode == 0:
            raise ProcessorExecuteError(user_msg = user_err_msg)



        ######################
        ### Return results ###
        ######################

        # Return link to output file wrapped in JSON:
        outputs = {
            "outputs": {
                "joined_csv": {
                    "title": PROCESS_METADATA['outputs']['joined_csv']['title'],
                    "description": PROCESS_METADATA['outputs']['joined_csv']['description'],
                    "href": out_result_url
                }
            }
        }

        return 'application/json', outputs






import logging
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
LOGGER = logging.getLogger(__name__)

import json
import os
import traceback
import datetime
# niva repo has hyphen in it, so we cannot import it in the normal python way:
#from pygeoapi.process.niva-aquainfra.pygeoapi_processes.docker_utils import run_docker_container3
import importlib  
docker_utils = importlib.import_module("pygeoapi.process.niva-aquainfra.pygeoapi_processes.docker_utils")


'''
# TESTED 2026-01-23
curl -X POST https://${PYSERVER}/processes/netcdf-tile-plot/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "url_input_csv": "https://csv-output-from-extraction-process.csv",
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "parameters": ["salinity", "chlorophyll"],
        "storm_date": "2023-08-08"
}
}'; date
'''


# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class NivaTilePlotProcessor(BaseProcessor):

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
            self.image_name = "ferry-rscripts:20260123"
            self.script_name = 'netcdf_tile_plot.R'


    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def __repr__(self):
        return f'<NivaTilePlotProcessor> {self.name}'


    def execute(self, data):
        LOGGER.info('Starting process NIVA Ferrybox Tile Plot!')
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
        url_input_csv = data.get('url_input_csv')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        parameters = data.get('parameters')
        lat1 = data.get('lat1', None)
        lat2 = data.get('lat2', None)
        storm_date = data.get('end_date', None)

        # Check user inputs:
        if url_input_csv is None:
            raise ProcessorExecuteError('Missing parameter "url_input_csv". Please provide a URL.')
        if start_date is None:
            raise ProcessorExecuteError("Missing parameter 'start_date'. Please provide a date.")
        if end_date is None:
            raise ProcessorExecuteError("Missing parameter 'end_date'. Please provide a date.")
        if parameters is None:
            raise ProcessorExecuteError("Missing parameter 'parameters'. Please provide a list of parameters.") # TODO HOW MANY?
        
        # Parse and validate the dates, to see whether it is valid:
        parsed_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        LOGGER.debug(f'The provided start_date is valid: {parsed_date}')
        parsed_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        LOGGER.debug(f'The provided end_date is valid: {parsed_date}')
        if storm_date is not None:
            parsed_date = datetime.datetime.strptime(storm_date, "%Y-%m-%d")
            LOGGER.debug(f'The provided storm_date is valid: {parsed_date}')
        # Parse the latitudes, to check whether they ar enumbers
        if lat1 is not None:
            float(lat1)
        if lat2 is not None:
            float(lat2)



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
        out_result_path = f'{output_dir}/tile_plot_{self.job_id}.png'
        out_result_url  = f'{output_url}/tile_plot_{self.job_id}.png'

        ###########
        ### Run ###
        ###########

        params = ','.join(parameters)
        r_args = [url_input_csv, out_result_path, start_date, end_date, params, lat1, lat2, storm_date]
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
                "tile_plot": {
                    "title": PROCESS_METADATA['outputs']['tile_plot']['title'],
                    "description": PROCESS_METADATA['outputs']['tile_plot']['description'],
                    "href": out_result_url
                }
            }
        }

        return 'application/json', outputs





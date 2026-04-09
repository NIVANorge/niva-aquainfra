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
# TESTED by Merret, 2026-03-17
# TESTED by Merret, 2026-04-08
curl -X POST https://${PYSERVER}/processes/netcdf-logger-extract/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
       "url_thredds": "https://thredds.niva.no/thredds/dodsC/datasets/loggers/glomma/baterod.nc",
       "parameters": null,
       "start_date": "2023-01-01",
       "end_date": "2023-12-31"
    }
}'; date

# TESTED by Merret, 2026-03-17
# TESTED by Merret, 2026-04-08
curl -X POST https://${PYSERVER}/processes/netcdf-logger-extract/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "url_thredds": "https://thredds.niva.no/thredds/dodsC/datasets/loggers/glomma/baterod.nc",
        "parameters": ["temp_water_avg", "phvalue_avg", "condvalue_avg", "turbidity_avg", "cdomdigitalfinal"],
        "start_date": "2023-01-01",
        "end_date": "2023-12-31"
    }
}'; date

'''


# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class NivaNetcdfLoggerExtractProcessor(BaseProcessor):

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
            self.image_name = "ferry-rscripts:20260409-f9c28ed"
            self.script_name = 'netcdf_logger_extract.R'


    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def __repr__(self):
        return f'<NivaNetcdfLoggerExtractProcessor> {self.name}'


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
        url_thredds = data.get('url_thredds')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        parameters = data.get('parameters', None)

        # Check user inputs:
        if url_thredds is None:
            raise ProcessorExecuteError("Missing parameter 'url_thredds'. Please provide a URL.")
        if start_date is None:
            raise ProcessorExecuteError("Missing parameter 'start_date'. Please provide a date (yyyy-mm-dd).")
        if end_date is None:
            raise ProcessorExecuteError("Missing parameter 'end_date'. Please provide a date (yyyy-mm-dd).")

        # Check validity of argument:
        # Parse and validate the dates, to see whether it is valid:
        parsed_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        LOGGER.debug(f'The provided start_date is valid: {parsed_date}')
        parsed_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        LOGGER.debug(f'The provided end_date is valid:   {parsed_date}')

        # Check existence:
        # Note: During testing, this gets HTTP 400. Maybe THREDDS does not reply to HEAD requests.
        #requests.head(url_thredds).raise_for_status()

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
        out_result_path = f'{output_dir}/logger_{self.job_id}.csv'
        out_result_url  = f'{output_url}/logger_{self.job_id}.csv'

        ###########
        ### Run ###
        ###########

        # Make a proper string from the parameters that we can pass over to the R script:
        if parameters is not None:
            parameters = ','.join(parameters)

        # Assemble R args:
        r_args = [
            url_thredds,
            out_result_path,
            parameters,
            start_date,
            end_date
        ]
        LOGGER.debug(f"r_args: {r_args}")

        # Actually call R script:
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
                "output_csv": {
                    "title": PROCESS_METADATA['outputs']['output_csv']['title'],
                    "description": PROCESS_METADATA['outputs']['output_csv']['description'],
                    "href": out_result_url
                }
            }
        }

        return 'application/json', outputs






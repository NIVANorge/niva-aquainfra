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
curl -X POST https://${PYSERVER}/processes/netcdf-assessment-area/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "url_input_csv": "https://csv-output-from-extraction-process.csv",  # this the fb data. Bad labeling by me.
        "url_input_river_logger_csv": "https://bla-something.csv",
        "river_label_col": "station_name", 
        "url_input_waterbody": "NULL",  #https://karteksport.miljodirektoratet.no/ pick vannforekomster and "egendefinert område" and square around Oslofjord for example. (example https://nedlasting.miljodirektoratet.no/Miljodata/egendefinert/Vannforekomster_202603200841.zip)
        "study_area_layer": "VannforekomstKyst"
    }
}'; date


# NOT TESTED YET
curl -X POST https://${PYSERVER}/processes/netcdf-assessment-area/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "url_input_csv": "https://csv-output-from-extraction-process.csv",
        "url_input_river_logger_csv": "https://bla-something.csv"
    }
}'; date
'''


# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class NivaNetcdfAssessmentAreaProcessor(BaseProcessor):

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
            self.script_name = 'netcdf_assessment_area.R'


    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def __repr__(self):
        return f'<NivaNetcdfAssessmentAreaProcessor> {self.name}'


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
        url_input_csv = data.get('url_input_csv')
        url_input_river_logger_csv = data.get('url_input_river_logger_csv')
        river_label_col = data.get('river_label_col', None) #Need to specify the columns name where the river names are available
        url_input_waterbody = data.get('url_input_waterbody', None) # optional

        # Check user inputs:
      if url_input_csv is None:
        raise ProcessorExecuteError("Missing parameter 'url_input_csv'. Please provide a URL.")

    if url_input_river_logger_csv is None:
        raise ProcessorExecuteError("Missing parameter 'url_input_river_logger_csv'. Please provide a URL.")

    if url_input_river_logger_csv is not None and river_label_col is None:
        raise ProcessorExecuteError(
        "Parameter 'river_label_col' must be provided when 'url_input_river_logger_csv' is used."
    ) # added error if the name column is not provided


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


        #raise_for_status returns error in other scripts, so also removed here.
        # Check existence:
        #requests.head(url_input_csv), raise_for_status()
        #requests.head(url_input_river_logger_csv), raise_for_status()
        #if url_input_waterbody is not None:
        #    requests.head(url_input_waterbody), raise_for_status()


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
        out_result_path = f'{output_dir}/assessment_area_{self.job_id}.png'
        out_result_url  = f'{output_url}/assessment_area_{self.job_id}.png'

        ###########
        ### Run ###
        ###########

       # params = ','.join(parameters) parameter not used in script
        r_args = [url_input_csv, out_result_path, url_input_river_logger_csv, river_label_col, url_input_waterbody] #added river_label_col
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
                "assessment_area": {
                    "title": PROCESS_METADATA['outputs']['assessment_area']['title'],
                    "description": PROCESS_METADATA['outputs']['assessment_area']['description'],
                    "href": out_result_url
                }
            }
        }

        return 'application/json', outputs






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
# Without a bounding box:
# Tested 2025-11-12
curl -i -X POST https://${PYSERVER}/processes/netcdf-extract-save-fb/execution \
--header 'Content-Type: application/json' \
--header 'Prefer: respond-async' \
--data '{
    "inputs": {
        "url_thredds": "https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc",
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "parameters": ["temperature", "salinity", "oxygen_sat", "chlorophyll", "turbidity", "fdom"]
    }
}'; date


# With a bounding box:
# Tested 2025-11-18
curl -i -X POST https://${PYSERVER}/processes/netcdf-extract-save-fb/execution \
--header 'Content-Type: application/json' \
--header 'Prefer: respond-async' \
--data '{
    "inputs": {
        "url_thredds": "https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc",
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "study_area_bbox": {"bbox": [58.5, 9.5, 59.9, 11.9]},
        "parameters": ["temperature", "salinity", "oxygen_sat", "chlorophyll", "turbidity", "fdom"]
    }
}'; date

# About passing the bbox:

This is the order that the r script wants (bbox):
lon_min, lon_max, lat_min, lat_max
'9.5'    '11.9'   '58.5'   '59.9'

This is the order that the OGC API wants (bbox):
"study_area_bbox": {"bbox": [58.5,     9.5,     59.9,     11.9    ]}
"study_area_bbox": {"bbox": [42.08333, 8.15250, 50.24500, 29.73583]}
"study_area_bbox": {"bbox": [lat_min, lon_min, lat_max, lon_max]}

'''


# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class NivaFerryboxProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.job_id = None
        self.process_id = self.metadata["id"]

        # Set config:
        config_file_path = os.environ.get('AQUAINFRA_CONFIG_FILE', "./config.json")
        with open(config_file_path, 'r') as config_file:
            config = json.load(config_file)
            self.download_dir = config["download_dir"].rstrip('/')
            self.download_url = config["download_url"].rstrip('/')
            self.docker_executable = config["docker_executable"]
            self.image_name = "ferry-rscripts:20251118"
            self.script_name = 'netcdf_extract_save_fb.R'


    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def __repr__(self):
        return f'<NivaFerryboxProcessor> {self.name}'


    def execute(self, data):
        LOGGER.info('Starting process NIVA Ferrybox!')
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
        # OGC API has an explicit schema for Bounding Box:
        # https://docs.ogc.org/is/18-062r2/18-062r2.html#bounding-box-value
        #"study_area_bbox": {"bbox": [42.08333, 8.15250, 50.24500, 29.73583]}
        #"study_area_bbox": {"bbox": [lat_min, lon_min, lat_max, lon_max]}
        study_area_bbox = data.get('study_area_bbox', None)
        if study_area_bbox is None:
            lat_min = None
            lon_min = None
            lat_max = None
            lon_max = None
        else:
            lat_min = study_area_bbox["bbox"][0]
            lon_min = study_area_bbox["bbox"][1]
            lat_max = study_area_bbox["bbox"][2]
            lon_max = study_area_bbox["bbox"][3]

        # Check user inputs:
        if url_thredds is None:
            raise ProcessorExecuteError('Missing parameter "url_thredds". Please provide a URL.')
        if start_date is None:
            raise ProcessorExecuteError('Missing parameter "start_date". Please provide a date (yyyy-mm-dd).')
        if end_date is None:
            raise ProcessorExecuteError('Missing parameter "end_date". Please provide a date (yyyy-mm-dd).')

        # Check validity of argument:
        # Parse and validate the dates, to see whether it is valid:
        parsed_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        LOGGER.debug(f'The provided start_date is valid: {parsed_date}')
        parsed_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        LOGGER.debug(f'The provided end_date is valid:   {parsed_date}')


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

        # Where to store output data
        # ferrybox_temperature_salinity_oxygen_sat_chlorophyll_turbidity_fdom_20251103_163347.csv
        params_string = '-'.join(parameters)
        out_result_path = f'{output_dir}/ferrybox_{self.job_id}_{params_string}.csv'

        # Where to access output data
        out_result_url = out_result_path.replace(self.download_dir, self.download_url)


        ###########
        ### Run ###
        ###########


        # Actually call R script:
        params_string = ','.join(parameters)
        r_args = [url_thredds, out_result_path, start_date, end_date, params_string, lon_min, lon_max, lat_min, lat_max]
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

        # Return link to output csv files and return it wrapped in JSON:
        outputs = {
            "outputs": {
                "csv_results": {
                    "title": PROCESS_METADATA['outputs']['csv_results']['title'],
                    "description": PROCESS_METADATA['outputs']['csv_results']['description'],
                    "href": out_result_url
                }
            }
        }

        return 'application/json', outputs




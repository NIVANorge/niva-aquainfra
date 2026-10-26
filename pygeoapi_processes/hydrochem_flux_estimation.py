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
# TESTED 2026-10-26
curl -X POST https://${PYSERVER}/processes/hydrochem-flux-estimation/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "url_interpolated_waterchem": "https://aquainfra.ogc.igb-berlin.de/exampledata/niva/hydrochem/inputs_fluxes/test_interpolation_output.nc",
        "url_discharge": "https://aquainfra.ogc.igb-berlin.de/exampledata/niva/hydrochem/Q_daily_mean_Glomma_Solbergfoss_2_605_0_cleaned.nc",
        "river_name": "Glomma",
        "q_station_name": "Solbergfoss",
        "latitude": 59.27980207,
        "longitude": 11.13411158,
        "variables": ["DOC", "TOTN", "TOTP"],
        "variable_units": ["DOC=mg/l","TOTN=ug/l","TOTP=ug/l"]
    }
}'; date
'''


# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class NivaHydrochemistryFluxEstimationProcessor(BaseProcessor):

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
        self.image_name = "niva-hydrochem:20261026"

    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def __repr__(self):
        return f'<NivaHydrochemistryFluxEstimationProcessor> {self.name}'


    def execute(self, data):
        LOGGER.info(f'Starting process NIVA Hydrochemistry (Interpolation)')
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

        # ============================================================
        # General parameters
        # ============================================================

        variables = data.get('variables')
        if variables is not None:
            if not isinstance(variables, list) or not all(isinstance(v, str) for v in variables):
                raise ProcessorExecuteError(
                    f"Malformed parameter 'variables', got {variables}, "
                    "expecting a list of strings."
                )

        variable_units = data.get('variable_units')
        if variable_units is not None:
            if not isinstance(variable_units, list) or not all(isinstance(v, str) for v in variable_units):
                raise ProcessorExecuteError(
                    f"Malformed parameter 'variable_units', got {variable_units}, "
                    "expecting a list of variable=unit pairs."
                )

        non_mass_vars = data.get('non_mass_vars')
        if non_mass_vars is not None:
            if not isinstance(non_mass_vars, list) or not all(isinstance(v, str) for v in non_mass_vars):
                raise ProcessorExecuteError(
                    f"Malformed parameter 'non_mass_vars', got {non_mass_vars}, "
                    "expecting a list of strings."
                )


        # ============================================================
        # River interpolation parameters
        # (These are shared with the interpolation step!)
        # ============================================================

        url_discharge = data.get('url_discharge')
        if url_discharge is None:
            raise ProcessorExecuteError(
                f"Missing parameter 'url_discharge', "
                "expecting a URL string."
            )

        elif not isinstance(url_discharge, str):
            raise ProcessorExecuteError(
                f"Malformed parameter 'url_discharge', got {url_discharge}, "
                "expecting a URL string."
            )


        river_name = data.get('river_name')
        if river_name is not None and not isinstance(river_name, str):
            raise ProcessorExecuteError(
                f"Malformed parameter 'river_name', got {river_name}, "
                "expecting a non-empty string."
            )

        q_station_name = data.get('q_station_name')
        if q_station_name is not None and not isinstance(q_station_name, str):
            raise ProcessorExecuteError(
                f"Malformed parameter 'q_station_name', got {q_station_name}, "
                "expecting a non-empty string."
            )

        latitude = data.get('latitude')
        if latitude is not None:
            if not isinstance(latitude, (int, float)) or not -90 <= latitude <= 90:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'latitude', got {latitude}, "
                    "expecting a number between -90 and 90."
                )

        longitude = data.get('longitude')
        if longitude is not None:
            if not isinstance(longitude, (int, float)) or not -180 <= longitude <= 180:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'longitude', got {longitude}, "
                    "expecting a number between -180 and 180."
                )


        # ============================================================
        # Flux-estimation parameters
        # ============================================================

        url_interpolated_waterchem = data.get('url_interpolated_waterchem')
        if url_interpolated_waterchem is None:
            raise ProcessorExecuteError(
                f"Missing parameter 'url_interpolated_waterchem', "
                "expecting a URL string."
            )
        elif not isinstance(url_interpolated_waterchem, str):
            raise ProcessorExecuteError(
                f"Malformed parameter 'url_interpolated_waterchem', got {url_interpolated_waterchem}, "
                "expecting a URL string."
            )

        # already validated above:
        # discharge, river_name, q_station_name, latitude and longitude

        flux_date_col = data.get('flux_date_col')
        if flux_date_col is not None:
            if not isinstance(flux_date_col, str) or not flux_date_col:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'flux_date_col', got {flux_date_col}, "
                    "expecting a non-empty string."
                )

        flux_station_col = data.get('flux_station_col')
        if flux_station_col is not None:
            if not isinstance(flux_station_col, str) or not flux_station_col:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'flux_station_col', got {flux_station_col}, "
                    "expecting a non-empty string."
                )

        flux_discharge_col = data.get('flux_discharge_col')
        if flux_discharge_col is not None:
            if not isinstance(flux_discharge_col, str) or not flux_discharge_col:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'flux_discharge_col', got {flux_discharge_col}, "
                    "expecting a non-empty string."
                )


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
        out_result_filename_flux_daily   = f'flux_daily_{self.job_id}.nc'
        out_result_filename_flux_monthly = f'flux_monthly_{self.job_id}.nc'
        out_result_filename_flux_annual  = f'flux_annual_{self.job_id}.nc'
        out_result_path_flux_daily   = f'{output_dir}/{out_result_filename_flux_daily}'
        out_result_path_flux_monthly = f'{output_dir}/{out_result_filename_flux_monthly}'
        out_result_path_flux_annual  = f'{output_dir}/{out_result_filename_flux_annual}'
        out_result_url_flux_daily    = f'{output_url}/{out_result_filename_flux_daily}'
        out_result_url_flux_monthly  = f'{output_url}/{out_result_filename_flux_monthly}'
        out_result_url_flux_annual   = f'{output_url}/{out_result_filename_flux_annual}'

        ###########
        ### Run ###
        ###########

        # Assemble R args:
        # TODO check if this is not too long...
        r_args = [
            '--steps', 'fluxes',
            '--output-dir', output_dir,
            '--daily-flux-output', out_result_path_flux_daily,
            '--monthly-flux-output', out_result_path_flux_monthly,
            '--annual-flux-output', out_result_path_flux_annual
        ]

        # General parameters

        if variables is not None:
            r_args += ['--variables', ','.join(variables)]

        if variable_units is not None:
            r_args += ['--variable-units', ','.join(variable_units)]

        if non_mass_vars is not None:
            r_args += ['--non-mass-vars', ','.join(non_mass_vars)]

        # River interpolation parameters

        if url_discharge is not None:
            r_args += ['--discharge', url_discharge]

        if river_name is not None:
            r_args += ['--river-name', river_name]

        if q_station_name is not None:
            r_args += ['--q-station-name', q_station_name]

        if latitude is not None:
            r_args += ['--latitude', latitude]

        if longitude is not None:
            r_args += ['--longitude', longitude]

        # Flux-estimation parameters

        if url_interpolated_waterchem is not None:
            r_args += ['--interpolated-waterchem', url_interpolated_waterchem]

        if flux_date_col is not None:
            r_args += ['--flux-date-col', flux_date_col]

        if flux_station_col is not None:
            r_args += ['--flux-station-col', flux_station_col]

        if flux_discharge_col is not None:
            r_args += ['--flux-discharge-col', flux_discharge_col]

        LOGGER.debug(f"r_args: {r_args}")


        # Actually call R script:
        returncode, stdout, stderr, user_err_msg = docker_utils.run_docker_container3(
            self.docker_executable,
            self.image_name,
            "not_needed",
            output_dir,
            r_args,
            style="py"
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
                "daily_flux": {
                    "title": PROCESS_METADATA['outputs']['daily_flux']['title'],
                    "description": PROCESS_METADATA['outputs']['daily_flux']['description'],
                    "href": out_result_url_flux_daily
                },
                "monthly_flux": {
                    "title": PROCESS_METADATA['outputs']['monthly_flux']['title'],
                    "description": PROCESS_METADATA['outputs']['monthly_flux']['description'],
                    "href": out_result_url_flux_monthly
                },
                "annual_flux": {
                    "title": PROCESS_METADATA['outputs']['annual_flux']['title'],
                    "description": PROCESS_METADATA['outputs']['annual_flux']['description'],
                    "href": out_result_url_flux_annual
                }
            }
        }

        return 'application/json', outputs







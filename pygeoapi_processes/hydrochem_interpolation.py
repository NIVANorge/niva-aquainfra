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
curl -X POST https://${PYSERVER}/processes/hydrochem-interpolation/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "url_waterchem": "https://thredds.niva.no/thredds/dodsC/datasets/samples/cleaned_riverchem_40356.nc",
        "url_discharge": "https://aquainfra.ogc.igb-berlin.de/exampledata/niva/hydrochem/Q_daily_mean_Glomma_Solbergfoss_2_605_0_cleaned.nc",
        "river_name": "Glomma",
        "wc_station_name": "Glomma, Sarpsfossen",
        "q_station_name": "Solbergfoss",
        "latitude": 59.27980207,
        "longitude": 11.13411158,
        "variables": ["DOC", "TOTN", "TOTP"],
        "variable-units": ["DOC=mg/l","TOTN=ug/l","TOTP=ug/l"]
    }
}'; date
'''


# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class NivaHydrochemistryInterpolationProcessor(BaseProcessor):

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
        return f'<NivaHydrochemistryInterpolationProcessor> {self.name}'


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
        # ============================================================

        url_waterchem = data.get('url_waterchem')
        if url_waterchem is None:
            raise ProcessorExecuteError(
                f"Missing parameter 'url_waterchem', "
                "expecting a URL string."
            )
        elif not isinstance(url_waterchem, str):
            raise ProcessorExecuteError(
                f"Malformed parameter 'url_waterchem', got {url_waterchem}, "
                "expecting a URL string."
            )

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

        wc_station_name = data.get('wc_station_name')
        if wc_station_name is not None and not isinstance(wc_station_name, str):
            raise ProcessorExecuteError(
                f"Malformed parameter 'wc_station_name', got {wc_station_name}, "
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

        wc_time_col = data.get('wc_time_col')
        if wc_time_col is not None and not isinstance(wc_time_col, str):
            raise ProcessorExecuteError(
                f"Malformed parameter 'wc_time_col', got {wc_time_col}, "
                "expecting a non-empty string."
            )

        q_time_col = data.get('q_time_col')
        if q_time_col is not None and not isinstance(q_time_col, str):
            raise ProcessorExecuteError(
                f"Malformed parameter 'q_time_col', got {q_time_col}, "
                "expecting a non-empty string."
            )

        wc_station_col = data.get('wc_station_col')
        if wc_station_col is not None and not isinstance(wc_station_col, str):
            raise ProcessorExecuteError(
                f"Malformed parameter 'wc_station_col', got {wc_station_col}, "
                "expecting a non-empty string."
            )

        q_station_col = data.get('q_station_col')
        if q_station_col is not None and not isinstance(q_station_col, str):
            raise ProcessorExecuteError(
                f"Malformed parameter 'q_station_col', got {q_station_col}, "
                "expecting a non-empty string."
            )

        discharge_var = data.get('discharge_var')
        if discharge_var is not None and not isinstance(discharge_var, str):
            raise ProcessorExecuteError(
                f"Malformed parameter 'discharge_var', got {discharge_var}, "
                "expecting a non-empty string."
            )


        # ============================================================
        # Interpolation and validation settings
        # ============================================================

        linear_max_gap = data.get('linear_max_gap')
        if linear_max_gap is not None:
            if not isinstance(linear_max_gap, int) or linear_max_gap < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'linear_max_gap', got {linear_max_gap}, "
                    "expecting a non-negative integer."
                )

        gam_n_splines_q = data.get('gam_n_splines_q')
        if gam_n_splines_q is not None:
            if not isinstance(gam_n_splines_q, int) or gam_n_splines_q <= 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'gam_n_splines_q', got {gam_n_splines_q}, "
                    "expecting a positive integer."
                )

        gam_n_splines_doy = data.get('gam_n_splines_doy')
        if gam_n_splines_doy is not None:
            if not isinstance(gam_n_splines_doy, int) or gam_n_splines_doy <= 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'gam_n_splines_doy', got {gam_n_splines_doy}, "
                    "expecting a positive integer."
                )

        validation_repeats = data.get('validation_repeats')
        if validation_repeats is not None:
            if not isinstance(validation_repeats, int) or validation_repeats <= 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'validation_repeats', got {validation_repeats}, "
                    "expecting a positive integer."
                )

        validation_block_size = data.get('validation_block_size')
        if validation_block_size is not None:
            if not isinstance(validation_block_size, int) or validation_block_size <= 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'validation_block_size', got {validation_block_size}, "
                    "expecting a positive integer."
                )

        validation_min_observations = data.get('validation_min_observations')
        if validation_min_observations is not None:
            if not isinstance(validation_min_observations, int) or validation_min_observations < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'validation_min_observations', got {validation_min_observations}, "
                    "expecting a non-negative integer."
                )

        validation_min_points = data.get('validation_min_points')
        if validation_min_points is not None:
            if not isinstance(validation_min_points, int) or validation_min_points < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'validation_min_points', got {validation_min_points}, "
                    "expecting a non-negative integer."
                )

        validation_min_coverage = data.get('validation_min_coverage')
        if validation_min_coverage is not None:
            if not isinstance(validation_min_coverage, (int, float)) or not 0 <= validation_min_coverage <= 1:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'validation_min_coverage', got {validation_min_coverage}, "
                    "expecting a number between 0 and 1."
                )

        selection_min_r2 = data.get('selection_min_r2')
        if selection_min_r2 is not None:
            if not isinstance(selection_min_r2, (int, float)) or not -1 <= selection_min_r2 <= 1:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'selection_min_r2', got {selection_min_r2}, "
                    "expecting a number between -1 and 1."
                )

        selection_r2_tolerance = data.get('selection_r2_tolerance')
        if selection_r2_tolerance is not None:
            if not isinstance(selection_r2_tolerance, (int, float)) or selection_r2_tolerance < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'selection_r2_tolerance', got {selection_r2_tolerance}, "
                    "expecting a non-negative number."
                )

        selection_max_bias_fraction = data.get('selection_max_bias_fraction')
        if selection_max_bias_fraction is not None:
            if not isinstance(selection_max_bias_fraction, (int, float)) or selection_max_bias_fraction < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'selection_max_bias_fraction', got {selection_max_bias_fraction}, "
                    "expecting a non-negative number."
                )

        selection_extreme_ratio_limit = data.get('selection_extreme_ratio_limit')
        if selection_extreme_ratio_limit is not None:
            if not isinstance(selection_extreme_ratio_limit, (int, float)) or selection_extreme_ratio_limit < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'selection_extreme_ratio_limit', got {selection_extreme_ratio_limit}, "
                    "expecting a non-negative number."
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
        out_result_filename = f'interpolation_{self.job_id}.nc'
        out_result_path = f'{output_dir}/{out_result_filename}'
        out_result_url  = f'{output_url}/{out_result_filename}'

        ###########
        ### Run ###
        ###########

        # Assemble R args:
        # TODO check if this is not too long...
        r_args = [
            '--steps', 'interpolate',
            '--output-dir', output_dir,
            '--interpolation-output', out_result_path
        ]

        # General parameters

        if variables is not None:
            r_args += ['--variables', ','.join(variables)]

        if variable_units is not None:
            r_args += ['--variable-units', ','.join(variable_units)]

        if non_mass_vars is not None:
            r_args += ['--non-mass-vars', ','.join(non_mass_vars)]

        # River interpolation parameters

        if url_waterchem is not None:
            r_args += ['--waterchem', url_waterchem]

        if url_discharge is not None:
            r_args += ['--discharge', url_discharge]

        if river_name is not None:
            r_args += ['--river-name', river_name]

        if wc_station_name is not None:
            r_args += ['--wc-station-name', wc_station_name]

        if q_station_name is not None:
            r_args += ['--q-station-name', q_station_name]

        if latitude is not None:
            r_args += ['--latitude', latitude]

        if longitude is not None:
            r_args += ['--longitude', longitude]

        if wc_time_col is not None:
            r_args += ['--wc-time-col', wc_time_col]

        if q_time_col is not None:
            r_args += ['--q-time-col', q_time_col]

        if wc_station_col is not None:
            r_args += ['--wc-station-col', wc_station_col]

        if q_station_col is not None:
            r_args += ['--q-station-col', q_station_col]

        if discharge_var is not None:
            r_args += ['--discharge-var', discharge_var]

        # Interpolation and validation settings

        if linear_max_gap is not None:
            r_args += ['--linear-max-gap', linear_max_gap]

        if gam_n_splines_q is not None:
            r_args += ['--gam-n-splines-q', gam_n_splines_q]

        if gam_n_splines_doy is not None:
            r_args += ['--gam-n-splines-doy', gam_n_splines_doy]

        if validation_repeats is not None:
            r_args += ['--validation-repeats', validation_repeats]

        if validation_block_size is not None:
            r_args += ['--validation-block-size', validation_block_size]

        if validation_min_observations is not None:
            r_args += ['--validation-min-observations', validation_min_observations]

        if validation_min_points is not None:
            r_args += ['--validation-min-points', validation_min_points]

        if validation_min_coverage is not None:
            r_args += ['--validation-min-coverage', validation_min_coverage]

        if selection_min_r2 is not None:
            r_args += ['--selection-min-r2', selection_min_r2]

        if selection_r2_tolerance is not None:
            r_args += ['--selection-r2-tolerance', selection_r2_tolerance]

        if selection_max_bias_fraction is not None:
            r_args += ['--selection-max-bias-fraction', selection_max_bias_fraction]

        if selection_extreme_ratio_limit is not None:
            r_args += ['--selection-extreme-ratio-limit', selection_extreme_ratio_limit]

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
                "interpolation_output": {
                    "title": PROCESS_METADATA['outputs']['interpolation_output']['title'],
                    "description": PROCESS_METADATA['outputs']['interpolation_output']['description'],
                    "href": out_result_url
                }
            }
        }

        return 'application/json', outputs







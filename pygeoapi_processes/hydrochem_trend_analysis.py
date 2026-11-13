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
# TESTED by Merret 2026-11-10
curl -i -X POST https://${PYSERVER}/processes/hydrochem-trend-analysis/execution \
--header 'Content-Type: application/json' \
--header 'Prefer: respond-async' \
--data '{
    "inputs": {
        "urls_rivers_daily_flux": ["https://aquainfra.ogc.igb-berlin.de/exampledata/niva/hydrochem/outputs_fluxes/fluxes_daily_example.nc"],
        "url_marine_chemistry": "https://thredds.niva.no/thredds/dodsC/datasets/samples/oslofjord_cleaned_standardized_data.nc",
        "variables": ["DOC", "TOTN", "TOTP", "SST"],
        "trend_frequency": "monthly"
    }
}'; date
'''


# Process metadata and description
# Has to be in a JSON file of the same name, in the same dir! 
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))



class NivaHydrochemistryTrendAnalysisProcessor(BaseProcessor):

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
        self.image_name = "niva-hydrochem:20261109-d0c9f6f"

    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def __repr__(self):
        return f'<NivaHydrochemistryTrendAnalysisProcessor> {self.name}'


    def execute(self, data):
        LOGGER.info(f'Starting process NIVA Hydrochemistry (Trend Analysis)')
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
        # TODO do we have to check for empty list?
        if variables is not None:
            if not isinstance(variables, list) or not all(isinstance(v, str) for v in variables):
                raise ProcessorExecuteError(
                    f"Malformed parameter 'variables', got {variables}, "
                    "expecting a list of strings."
                )

        variable_units = data.get('variable_units')
        # TODO do we have to check for empty list?
        if variable_units is not None:
            if not isinstance(variable_units, list) or not all(isinstance(v, str) for v in variable_units):
                raise ProcessorExecuteError(
                    f"Malformed parameter 'variable_units', got {variable_units}, "
                    "expecting a list of variable=unit pairs."
                )

        non_mass_vars = data.get('non_mass_vars')
        # TODO do we have to check for empty list?
        if non_mass_vars is not None:
            if not isinstance(non_mass_vars, list) or not all(isinstance(v, str) for v in non_mass_vars):
                raise ProcessorExecuteError(
                    f"Malformed parameter 'non_mass_vars', got {non_mass_vars}, "
                    "expecting a list of strings."
                )


        # ============================================================
        # Trend-analysis parameters
        # ============================================================

        # instead of: river_flux_dir
        urls_rivers_daily_flux = data.get('urls_rivers_daily_flux')
        if urls_rivers_daily_flux is not None:
            if not isinstance(urls_rivers_daily_flux, list):
                raise ProcessorExecuteError(
                    f"Malformed parameter 'urls_rivers_daily_flux': "
                    f"expected a list of URLs, received {type(urls_rivers_daily_flux).__name__}."
               )
            if not urls_rivers_daily_flux:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'urls_rivers_daily_flux': "
                    f"expected a list of URLs, received empty list."
               )
            if not all(isinstance(item, str) for item in urls_rivers_daily_flux):
                raise ProcessorExecuteError(
                    f"Malformed parameter 'urls_rivers_daily_flux'"
                    " expected a list of URLs (list items must be strings)"
                    f" received: {urls_rivers_daily_flux}."
                )
            if not all(item.startswith('http') for item in urls_rivers_daily_flux):
                raise ProcessorExecuteError(
                    f"Malformed parameter 'urls_rivers_daily_flux'"
                    " expected a list of URLs (list items must start with http)"
                    f" received: {urls_rivers_daily_flux}."
                )
            if len(urls_rivers_daily_flux) != len(set(urls_rivers_daily_flux)):
                raise ProcessorExecuteError("Duplicate river input files are not allowed.")


        url_marine_chemistry = data.get('url_marine_chemistry')
        if url_marine_chemistry is not None:
            if not isinstance(url_marine_chemistry, str) or not url_marine_chemistry:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'url_marine_chemistry',"
                    " expected a URL,"
                    f" received: {url_marine_chemistry}"
                )
            if not url_marine_chemistry.startswith('http'):
                raise ProcessExecuteError(
                    f"Malformed parameter 'url_marine_chemistry',"
                    " expected a URL,"
                    f" received: {url_marine_chemistry}"
                )


        # Either marine or freshwater has to be given:
        marine_provided = (url_marine_chemistry is not None)
        rivers_provided = (urls_rivers_daily_flux is not None) and (len(urls_rivers_daily_flux)>0)
        LOGGER.debug(f"Input data passed: Marine {marine_provided} and Rivers {rivers_provided}")
        if marine_provided and not rivers_provided:
            LOGGER.debug('Inputs: Using only marine!')
        elif rivers_provided and not marine_provided:
            LOGGER.debug('Inputs: Using only rivers!')
        elif marine_provided and rivers_provided:
            LOGGER.debug('Inputs: Using marine and rivers!')
        elif not marine_provided and not rivers_provided:
            err_msg = "You have to provide either marine input data ('url_marine_chemistry') or river input data ('urls_rivers_daily_flux') or both!"
            LOGGER.error(err_msg)
            raise ProcessorExecuteError(err_msg)


        trend_frequency = data.get('trend_frequency')
        accepted_trend_frequency = [
            'config',
            'monthly',
            'annual',
            'seasonal_by_season',
        ]
        if trend_frequency is not None:
            if trend_frequency not in accepted_trend_frequency:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'trend_frequency', got {trend_frequency}, "
                    f"expecting one of: {accepted_trend_frequency}."
                )

        mk_mode = data.get('mk_mode')
        accepted_mk_modes = ['auto', 'original', 'seasonal']
        if mk_mode is not None:
            if mk_mode not in accepted_mk_modes:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'mk_mode', got {mk_mode}, "
                    f"expecting one of: {accepted_mk_modes}."
                )

        trend_start_year = data.get('trend_start_year')
        if trend_start_year is not None:
            if not isinstance(trend_start_year, int):
                raise ProcessorExecuteError(
                    f"Malformed parameter 'trend_start_year', got {trend_start_year}, "
                    "expecting an integer."
                )

        trend_end_year = data.get('trend_end_year')
        if trend_end_year is not None:
            if not isinstance(trend_end_year, int):
                raise ProcessorExecuteError(
                    f"Malformed parameter 'trend_end_year', got {trend_end_year}, "
                    "expecting an integer."
                )

        trend_require_full_period = data.get('trend_require_full_period')
        if trend_require_full_period is not None:
            if not isinstance(trend_require_full_period, bool):
                raise ProcessorExecuteError(
                    f"Malformed parameter 'trend_require_full_period', "
                    f"got {trend_require_full_period}, expecting a boolean."
                )

        trend_alpha = data.get('trend_alpha')
        if trend_alpha is not None:
            if not isinstance(trend_alpha, (int, float)) or not 0 < trend_alpha < 1:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'trend_alpha', got {trend_alpha}, "
                    "expecting a number strictly between 0 and 1."
                )

        trend_min_points_monthly = data.get('trend_min_points_monthly')
        if trend_min_points_monthly is not None:
            if not isinstance(trend_min_points_monthly, int) or trend_min_points_monthly < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'trend_min_points_monthly', "
                    f"got {trend_min_points_monthly}, expecting a non-negative integer."
                )

        trend_min_points_annual = data.get('trend_min_points_annual')
        if trend_min_points_annual is not None:
            if not isinstance(trend_min_points_annual, int) or trend_min_points_annual < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'trend_min_points_annual', "
                    f"got {trend_min_points_annual}, expecting a non-negative integer."
                )

        trend_min_points_seasonal = data.get('trend_min_points_seasonal')
        if trend_min_points_seasonal is not None:
            if not isinstance(trend_min_points_seasonal, int) or trend_min_points_seasonal < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'trend_min_points_seasonal', "
                    f"got {trend_min_points_seasonal}, expecting a non-negative integer."
                )


        # ============================================================
        # River aggregation settings
        # ============================================================

        river_min_obs_monthly = data.get('river_min_obs_monthly')
        if river_min_obs_monthly is not None:
            if not isinstance(river_min_obs_monthly, int) or river_min_obs_monthly < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'river_min_obs_monthly', "
                    f"got {river_min_obs_monthly}, expecting a non-negative integer."
                )

        river_min_obs_annual = data.get('river_min_obs_annual')
        if river_min_obs_annual is not None:
            if not isinstance(river_min_obs_annual, int) or river_min_obs_annual < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'river_min_obs_annual', "
                    f"got {river_min_obs_annual}, expecting a non-negative integer."
                )

        river_min_obs_seasonal = data.get('river_min_obs_seasonal')
        if river_min_obs_seasonal is not None:
            if not isinstance(river_min_obs_seasonal, int) or river_min_obs_seasonal < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'river_min_obs_seasonal', "
                    f"got {river_min_obs_seasonal}, expecting a non-negative integer."
                )


        # ============================================================
        # Marine settings
        # ============================================================

        marine_depth_min = data.get('marine_depth_min')
        if marine_depth_min is not None:
            if not isinstance(marine_depth_min, (int, float)) or marine_depth_min < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'marine_depth_min', got {marine_depth_min}, "
                    "expecting a non-negative number."
                )

        marine_depth_max = data.get('marine_depth_max')
        if marine_depth_max is not None:
            if not isinstance(marine_depth_max, (int, float)) or marine_depth_max < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'marine_depth_max', got {marine_depth_max}, "
                    "expecting a non-negative number."
                )

        marine_min_obs_monthly = data.get('marine_min_obs_monthly')
        if marine_min_obs_monthly is not None:
            if not isinstance(marine_min_obs_monthly, int) or marine_min_obs_monthly < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'marine_min_obs_monthly', "
                    f"got {marine_min_obs_monthly}, expecting a non-negative integer."
                )

        marine_min_obs_annual = data.get('marine_min_obs_annual')
        if marine_min_obs_annual is not None:
            if not isinstance(marine_min_obs_annual, int) or marine_min_obs_annual < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'marine_min_obs_annual', "
                    f"got {marine_min_obs_annual}, expecting a non-negative integer."
                )

        marine_min_obs_seasonal = data.get('marine_min_obs_seasonal')
        if marine_min_obs_seasonal is not None:
            if not isinstance(marine_min_obs_seasonal, int) or marine_min_obs_seasonal < 0:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'marine_min_obs_seasonal', "
                    f"got {marine_min_obs_seasonal}, expecting a non-negative integer."
                )

        marine_require_all_seasons = data.get('marine_require_all_seasons')
        if marine_require_all_seasons is not None:
            if not isinstance(marine_require_all_seasons, bool):
                raise ProcessorExecuteError(
                    f"Malformed parameter 'marine_require_all_seasons', "
                    f"got {marine_require_all_seasons}, expecting a boolean."
                )

        marine_min_seasons_per_year = data.get('marine_min_seasons_per_year')
        if marine_min_seasons_per_year is not None:
            if not isinstance(marine_min_seasons_per_year, int) or not 0 <= marine_min_seasons_per_year <= 4:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'marine_min_seasons_per_year', "
                    f"got {marine_min_seasons_per_year}, expecting an integer between 0 and 4."
                )

        marine_station_dim = data.get('marine_station_dim')
        if marine_station_dim is not None:
            if not isinstance(marine_station_dim, str) or not marine_station_dim:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'marine_station_dim', got {marine_station_dim}, "
                    "expecting a non-empty string."
                )

        marine_station_coord = data.get('marine_station_coord')
        if marine_station_coord is not None:
            if not isinstance(marine_station_coord, str) or not marine_station_coord:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'marine_station_coord', got {marine_station_coord}, "
                    "expecting a non-empty string."
                )

        marine_depth_dim = data.get('marine_depth_dim')
        if marine_depth_dim is not None:
            if not isinstance(marine_depth_dim, str) or not marine_depth_dim:
                raise ProcessorExecuteError(
                    f"Malformed parameter 'marine_depth_dim', got {marine_depth_dim}, "
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
        out_result_filename = f'trend_output_{self.job_id}.xlsx'
        out_result_path     = f'{output_dir}/{out_result_filename}'
        out_result_url      = f'{output_url}/{out_result_filename}'

        ###########
        ### Run ###
        ###########

        # Assemble R args:
        # TODO check if this is not too long...
        r_args = [
            '--steps', 'trends',
            '--output-dir', output_dir,
            '--trend-output', out_result_path
        ]

        # General parameters

        if variables is not None:
            r_args += ['--variables', ','.join(variables)]

        if variable_units is not None:
            r_args += ['--variable-units', ','.join(variable_units)]

        if non_mass_vars is not None:
            r_args += ['--non-mass-vars', ','.join(non_mass_vars)]

        # Trend-analysis parameters

        # instead of: river_flux_dir
        # (river-flux-dir works with local files, but we're dealing with URLs of remote files)
        if urls_rivers_daily_flux is not None:
            urls_rivers_daily_flux_concat = ','.join(urls_rivers_daily_flux)
            r_args += ['--inputfiles_rivers', urls_rivers_daily_flux_concat]

        if url_marine_chemistry is not None:
            r_args += ['--marine-input', url_marine_chemistry]

        if trend_frequency is not None:
            r_args += ['--trend-frequency', trend_frequency]

        if mk_mode is not None:
            r_args += ['--mk-mode', mk_mode]

        if trend_start_year is not None:
            r_args += ['--trend-start-year', trend_start_year]

        if trend_end_year is not None:
            r_args += ['--trend-end-year', trend_end_year]

        if trend_require_full_period is not None:
            r_args += ['--trend-require-full-period', trend_require_full_period]

        if trend_alpha is not None:
            r_args += ['--trend-alpha', trend_alpha]

        if trend_min_points_monthly is not None:
            r_args += ['--trend-min-points-monthly', trend_min_points_monthly]

        if trend_min_points_annual is not None:
            r_args += ['--trend-min-points-annual', trend_min_points_annual]

        if trend_min_points_seasonal is not None:
            r_args += ['--trend-min-points-seasonal', trend_min_points_seasonal]

        # River aggregation settings

        if river_min_obs_monthly is not None:
            r_args += ['--river-min-obs-monthly', river_min_obs_monthly]

        if river_min_obs_annual is not None:
            r_args += ['--river-min-obs-annual', river_min_obs_annual]

        if river_min_obs_seasonal is not None:
            r_args += ['--river-min-obs-seasonal', river_min_obs_seasonal]

        # Marine settings

        if marine_depth_min is not None:
            r_args += ['--marine-depth-min', marine_depth_min]

        if marine_depth_max is not None:
            r_args += ['--marine-depth-max', marine_depth_max]

        if marine_min_obs_monthly is not None:
            r_args += ['--marine-min-obs-monthly', marine_min_obs_monthly]

        if marine_min_obs_annual is not None:
            r_args += ['--marine-min-obs-annual', marine_min_obs_annual]

        if marine_min_obs_seasonal is not None:
            r_args += ['--marine-min-obs-seasonal', marine_min_obs_seasonal]

        if marine_require_all_seasons is not None:
            r_args += ['--marine-require-all-seasons', marine_require_all_seasons]

        if marine_min_seasons_per_year is not None:
            r_args += ['--marine-min-seasons-per-year', marine_min_seasons_per_year]

        if marine_station_dim is not None:
            r_args += ['--marine-station-dim', marine_station_dim]

        if marine_station_coord is not None:
            r_args += ['--marine-station-coord', marine_station_coord]

        if marine_depth_dim is not None:
            r_args += ['--marine-depth-dim', marine_depth_dim]


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
                "combined_trends": {
                    "title": PROCESS_METADATA['outputs']['combined_trends']['title'],
                    "description": PROCESS_METADATA['outputs']['combined_trends']['description'],
                    "href": out_result_url
                }
            }
        }

        return 'application/json', outputs



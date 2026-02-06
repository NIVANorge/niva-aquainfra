import logging
import json
import os
import traceback
import datetime

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

LOGGER = logging.getLogger(__name__)

# Assumes docker_utils.py is in the same directory as this process file
from . import docker_utils


# Metadata: JSON file with same basename as this .py
script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace('.py', '.json')
PROCESS_METADATA = json.load(open(metadata_title_and_path))


class NivaFerryboxExtractionProcessor(BaseProcessor):
    """
    pygeoapi processor that runs the R script netcdf_extract_fb_data.R in Docker.

    External API inputs:
      - url_thredds (required)
      - start_date (required)
      - end_date (required)
      - parameters (optional array of strings; omitted/empty => ALL)
      - study_area_bbox (optional object): {"bbox":[lat_min, lon_min, lat_max, lon_max]}

    R script args order:
      source, save_path, parameters_csv_or_NULL, start_date, end_date, lon_min, lon_max, lat_min, lat_max
    """

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.job_id = None
        self.process_id = self.metadata["id"]

        # Load config
        config_file_path = os.environ.get('AQUAINFRA_CONFIG_FILE', "./config.json")
        with open(config_file_path, 'r') as config_file:
            config = json.load(config_file)
            self.download_dir = config["download_dir"].rstrip('/')
            self.download_url = config["download_url"].rstrip('/')
            self.docker_executable = config["docker_executable"]

        # Docker image + script
        # Consider reading image_name from config too, but keeping your current setup:
        self.image_name = "ferry-rscripts:20260123"
        self.script_name = "netcdf_extract_fb_data.R"

    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def __repr__(self):
        return f'<NivaFerryboxExtractionProcessor> {self.name}'

    def execute(self, data):
        LOGGER.info('Starting process: netcdf-extract-fb-data')
        try:
            mimetype, result = self._execute(data)
            return mimetype, result
        except Exception as e:
            err_msg = str(e)
            LOGGER.error(f'Error during execute: {err_msg}')
            LOGGER.error('TRACEBACK:')
            LOGGER.error(traceback.format_exc())
            raise ProcessorExecuteError(err_msg) from e

    def _execute(self, data):
        # ----------------
        # Inputs
        # ----------------
        url_thredds = data.get('url_thredds')
        start_date = data.get('start_date')
        end_date = data.get('end_date')

        parameters = data.get('parameters', None)
        if parameters is None:
            parameters = []  # means "ALL"
        if not isinstance(parameters, list):
            raise ProcessorExecuteError('Input "parameters" must be an array of strings if provided.')
        for p in parameters:
            if not isinstance(p, str):
                raise ProcessorExecuteError('All entries in "parameters" must be strings.')

        study_area_bbox = data.get('study_area_bbox', None)

        # Validate required
        if not url_thredds:
            raise ProcessorExecuteError('Missing parameter "url_thredds". Please provide a URL.')
        if not start_date:
            raise ProcessorExecuteError('Missing parameter "start_date". Please provide a date (yyyy-mm-dd).')
        if not end_date:
            raise ProcessorExecuteError('Missing parameter "end_date". Please provide a date (yyyy-mm-dd).')

        # Validate date format
        try:
            datetime.datetime.strptime(start_date, "%Y-%m-%d")
            datetime.datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise ProcessorExecuteError('Invalid date format. Use YYYY-MM-DD for start_date and end_date.')

        # Parse bbox (OGC order: [lat_min, lon_min, lat_max, lon_max])
        lat_min = lon_min = lat_max = lon_max = None
        if study_area_bbox is not None:
            bbox = study_area_bbox.get("bbox") if isinstance(study_area_bbox, dict) else None
            if bbox is None or not isinstance(bbox, list) or len(bbox) != 4:
                raise ProcessorExecuteError('"study_area_bbox" must be an object with "bbox": [lat_min, lon_min, lat_max, lon_max].')
            lat_min, lon_min, lat_max, lon_max = bbox[0], bbox[1], bbox[2], bbox[3]

        # ----------------
        # Outputs
        # ----------------
        if self.job_id is None:
            # pygeoapi should set this, but guard anyway
            self.job_id = "no_job_id"

        output_dir = f'{self.download_dir}/out/{self.process_id}/job_{self.job_id}'
        output_url = f'{self.download_url}/out/{self.process_id}/job_{self.job_id}'
        os.makedirs(output_dir, exist_ok=True)

        params_for_name = "all" if len(parameters) == 0 else "-".join(parameters)
        out_result_path = f'{output_dir}/ferrybox_{self.job_id}_{params_for_name}.csv'
        out_result_url = out_result_path.replace(self.download_dir, self.download_url)

        # ----------------
        # Run (Docker)
        # ----------------
        params_csv = "NULL" if len(parameters) == 0 else ",".join(parameters)

        def as_r_arg(x):
            # R helper treats "null"/"NULL"/blank as NULL; pass NULL explicitly
            return "NULL" if x is None else str(x)

        # R script expects: lon_min lon_max lat_min lat_max
        r_args = [
            url_thredds,
            out_result_path,
            params_csv,
            start_date,
            end_date,
            as_r_arg(lon_min),
            as_r_arg(lon_max),
            as_r_arg(lat_min),
            as_r_arg(lat_max)
        ]

        LOGGER.debug(f"Calling docker image={self.image_name}, script={self.script_name}, r_args={r_args}")

        returncode, stdout, stderr, user_err_msg = docker_utils.run_docker_container3(
            self.docker_executable,
            self.image_name,
            self.script_name,
            output_dir,
            r_args
        )

        if returncode != 0:
            # Prefer a user-friendly message produced by docker_utils
            raise ProcessorExecuteError(user_err_msg or "Docker/R process failed.")

        # ----------------
        # Return results
        # ----------------
        outputs = {
            "outputs": {
                "csv_results": {
                    "title": PROCESS_METADATA["outputs"]["csv_results"]["title"],
                    "description": PROCESS_METADATA["outputs"]["csv_results"]["description"],
                    "href": out_result_url
                }
            }
        }

        return "application/json", outputs





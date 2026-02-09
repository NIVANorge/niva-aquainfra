import logging
import json
import os
import traceback
import datetime

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

LOGGER = logging.getLogger(__name__)

from . import docker_utils

script_title_and_path = __file__
metadata_title_and_path = script_title_and_path.replace(".py", ".json")
PROCESS_METADATA = json.load(open(metadata_title_and_path))


def as_r_arg(x):
    if x is None:
        return "NULL"
    s = str(x).strip()
    if s == "" or s.lower() == "null":
        return "NULL"
    return s


class NivaLoggerExtractProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)
        self.job_id = None
        self.process_id = self.metadata["id"]

        config_file_path = os.environ.get("AQUAINFRA_CONFIG_FILE", "./config.json")
        with open(config_file_path, "r") as config_file:
            config = json.load(config_file)
            self.download_dir = config["download_dir"].rstrip("/")
            self.download_url = config["download_url"].rstrip("/")
            self.docker_executable = config["docker_executable"]

        self.image_name = "ferry-rscripts:20260123"
        self.script_name = "netcdf_logger_extract.R"

    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def execute(self, data):
        LOGGER.info("Starting process netcdf-logger-extract")
        try:
            return self._execute(data)
        except Exception as e:
            err_msg = str(e)
            LOGGER.error(f"Error during execute: {err_msg}")
            LOGGER.error("TRACEBACK:")
            LOGGER.error(traceback.format_exc())
            raise ProcessorExecuteError(err_msg) from e

    def _execute(self, data):
        inputs = data.get("inputs", data)

        url_thredds = inputs.get("url_thredds")
        parameters = inputs.get("parameters", None)
        start_date = inputs.get("start_date", None)
        end_date = inputs.get("end_date", None)

        if not url_thredds:
            raise ProcessorExecuteError('Missing required input "url_thredds".')

        # Validate optional date pair logic
        if (start_date and not end_date) or (end_date and not start_date):
            raise ProcessorExecuteError('Provide both start_date and end_date, or neither.')

        # Validate date formats if provided
        if start_date:
            try:
                datetime.datetime.strptime(start_date, "%Y-%m-%d")
                datetime.datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                raise ProcessorExecuteError("Invalid date format. Use YYYY-MM-DD.")

        # parameters: array -> comma string or NULL
        if parameters is None:
            parameters = []
        if not isinstance(parameters, list):
            raise ProcessorExecuteError('"parameters" must be an array of strings if provided.')
        for p in parameters:
            if not isinstance(p, str):
                raise ProcessorExecuteError('All entries in "parameters" must be strings.')

        params_csv = "NULL" if len(parameters) == 0 else ",".join(parameters)
        params_for_name = "all" if len(parameters) == 0 else "-".join(parameters)

        if self.job_id is None:
            self.job_id = "no_job_id"

        output_dir = f"{self.download_dir}/out/{self.process_id}/job_{self.job_id}"
        os.makedirs(output_dir, exist_ok=True)

        # R script will write logger.csv when save_path is a directory
        out_csv_path = f"{output_dir}/logger.csv"
        out_csv_url = out_csv_path.replace(self.download_dir, self.download_url)

        # R args order: source, save_path, parameters, start_date, end_date
        r_args = [
            url_thredds,
            output_dir,
            params_csv,
            as_r_arg(start_date),
            as_r_arg(end_date)
        ]

        LOGGER.debug(f"r_args: {r_args}")

        returncode, stdout, stderr, user_err_msg = docker_utils.run_docker_container3(
            self.docker_executable,
            self.image_name,
            self.script_name,
            output_dir,
            r_args
        )

        if returncode != 0:
            raise ProcessorExecuteError(user_err_msg or "Docker/R process failed.")

        outputs = {
            "outputs": {
                "csv_results": {
                    "title": PROCESS_METADATA["outputs"]["csv_results"]["title"],
                    "description": PROCESS_METADATA["outputs"]["csv_results"]["description"],
                    "href": out_csv_url
                }
            }
        }

        return "application/json", outputs

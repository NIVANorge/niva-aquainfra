import logging
import json
import os
import traceback

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


class NivaJoinDataframesProcessor(BaseProcessor):

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
        self.script_name = "netcdf_join_dataframes.R"

    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def execute(self, data):
        LOGGER.info("Starting process netcdf-join-dataframes")
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

        data_x_csv = inputs.get("data_x_csv")
        data_y_csv = inputs.get("data_y_csv")
        parameter_x = inputs.get("parameter_x")
        parameter_y = inputs.get("parameter_y")
        station_col = inputs.get("station_col")
        station_ID = inputs.get("station_ID")
        time_col_x = inputs.get("time_col_x")
        time_col_y = inputs.get("time_col_y")

        # Required checks
        for k, v in [
            ("data_x_csv", data_x_csv),
            ("data_y_csv", data_y_csv),
            ("parameter_x", parameter_x),
            ("parameter_y", parameter_y),
            ("station_col", station_col),
            ("station_ID", station_ID),
            ("time_col_x", time_col_x),
            ("time_col_y", time_col_y),
        ]:
            if v is None or str(v).strip() == "":
                raise ProcessorExecuteError(f'Missing required input "{k}".')

        if self.job_id is None:
            self.job_id = "no_job_id"

        # Output dir for this job
        output_dir = f"{self.download_dir}/out/{self.process_id}/job_{self.job_id}"
        os.makedirs(output_dir, exist_ok=True)

        # Script will write joined.csv into save_path when save_path is a directory
        out_csv_path = f"{output_dir}/joined.csv"
        out_csv_url = out_csv_path.replace(self.download_dir, self.download_url)

        # R args order matches your script:
        # 1 x, 2 y, 3 parameter_x, 4 parameter_y, 5 station_col, 6 station_ID, 7 time_col_x, 8 time_col_y, 9 save_path
        r_args = [
            as_r_arg(data_x_csv),
            as_r_arg(data_y_csv),
            as_r_arg(parameter_x),
            as_r_arg(parameter_y),
            as_r_arg(station_col),
            as_r_arg(station_ID),
            as_r_arg(time_col_x),
            as_r_arg(time_col_y),
            output_dir
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





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


class NivaScatterStationPlotProcessor(BaseProcessor):

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
        # Set this to the actual script filename inside your Docker image:
        self.script_name = "netcdf_scatter_station_plot.R"

    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def execute(self, data):
        LOGGER.info("Starting process netcdf-scatter-station-plot")
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

        input_csv = inputs.get("input_csv")
        parameter_x = inputs.get("parameter_x")
        parameter_y = inputs.get("parameter_y")

        if not input_csv:
            raise ProcessorExecuteError('Missing required input "input_csv".')
        if not parameter_x:
            raise ProcessorExecuteError('Missing required input "parameter_x".')
        if not parameter_y:
            raise ProcessorExecuteError('Missing required input "parameter_y".')

        if self.job_id is None:
            self.job_id = "no_job_id"

        output_dir = f"{self.download_dir}/out/{self.process_id}/job_{self.job_id}"
        os.makedirs(output_dir, exist_ok=True)

        # R script writes scatterplot.png when save_path is a directory
        out_png_path = f"{output_dir}/scatterplot.png"
        out_png_url = out_png_path.replace(self.download_dir, self.download_url)

        # R args order: input_path, save_path, parameter_x, parameter_y
        r_args = [
            as_r_arg(input_csv),
            output_dir,
            as_r_arg(parameter_x),
            as_r_arg(parameter_y)
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
                "png_result": {
                    "title": PROCESS_METADATA["outputs"]["png_result"]["title"],
                    "description": PROCESS_METADATA["outputs"]["png_result"]["description"],
                    "href": out_png_url
                }
            }
        }

        return "application/json", outputs

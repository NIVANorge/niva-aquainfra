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
    """Convert optional values to the string 'NULL' understood by your R helpers."""
    if x is None:
        return "NULL"
    s = str(x).strip()
    if s == "" or s.lower() == "null":
        return "NULL"
    return s


class NivaAssessmentAreaProcessor(BaseProcessor):

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

        # Update these if your tag/script name differs
        self.image_name = "ferry-rscripts:20260123"
        self.script_name = "netcdf_assessment_area.R"

    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def execute(self, data):
        LOGGER.info("Starting process netcdf-assessment-area")
        try:
            return self._execute(data)
        except Exception as e:
            err_msg = str(e)
            LOGGER.error(f"Error during execute: {err_msg}")
            LOGGER.error("TRACEBACK:")
            LOGGER.error(traceback.format_exc())
            raise ProcessorExecuteError(err_msg) from e

    def _execute(self, data):
        # pygeoapi often wraps in {"inputs": {...}}
        inputs = data.get("inputs", data)

        ferrybox_csv = inputs.get("ferrybox_csv")
        river_csv = inputs.get("river_csv", None)
        waterbody_shp = inputs.get("waterbody_shp", None)
        river_label_col = inputs.get("river_label_col", None)

        if not ferrybox_csv:
            raise ProcessorExecuteError('Missing required input "ferrybox_csv".')

        if self.job_id is None:
            self.job_id = "no_job_id"

        # Output dirs
        output_dir = f"{self.download_dir}/out/{self.process_id}/job_{self.job_id}"
        output_url = f"{self.download_url}/out/{self.process_id}/job_{self.job_id}"
        os.makedirs(output_dir, exist_ok=True)

        # Script writes: save_png_path/assessment_area.png
        expected_png_path = f"{output_dir}/assessment_area.png"
        expected_png_url = expected_png_path.replace(self.download_dir, self.download_url)

        # R args (script expects: fb_csv, save_png_path, river_csv, waterbody_shp, river_label_col)
        r_args = [
            ferrybox_csv,
            output_dir,
            as_r_arg(river_csv),
            as_r_arg(waterbody_shp),
            as_r_arg(river_label_col)
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
                    "href": expected_png_url
                }
            }
        }

        return "application/json", outputs




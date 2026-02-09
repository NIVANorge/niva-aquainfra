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


class NivaScatterFromJoinedProcessor(BaseProcessor):

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
        # Set to your actual script filename inside the container:
        self.script_name = "netcdf_scatter_datax_vs_datay.R"

    def set_job_id(self, job_id: str):
        self.job_id = job_id

    def execute(self, data):
        LOGGER.info("Starting process netcdf-scatter-from-joined")
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

        joined_csv = inputs.get("joined_csv")

        waterbodies_shp = inputs.get("waterbodies_shp", None)
        waterbody_ids = inputs.get("waterbody_ids", None)
        waterbody_id_col = inputs.get("waterbody_id_col", None)

        lat_min = inputs.get("lat_min", None)
        lat_max = inputs.get("lat_max", None)

        if not joined_csv:
            raise ProcessorExecuteError('Missing required input "joined_csv".')

        using_waterbodies = (waterbodies_shp is not None and str(waterbodies_shp).strip() != "" and str(waterbodies_shp).lower() != "null")

        if using_waterbodies:
            if not waterbody_id_col:
                raise ProcessorExecuteError('When waterbodies_shp is provided, "waterbody_id_col" is required.')
            if not waterbody_ids or not isinstance(waterbody_ids, list) or len(waterbody_ids) == 0:
                raise ProcessorExecuteError('When waterbodies_shp is provided, "waterbody_ids" (non-empty array) is required.')
        else:
            if lat_min is None or lat_max is None:
                raise ProcessorExecuteError('Provide lat_min and lat_max when waterbodies_shp is not provided.')

        if self.job_id is None:
            self.job_id = "no_job_id"

        output_dir = f"{self.download_dir}/out/{self.process_id}/job_{self.job_id}"
        os.makedirs(output_dir, exist_ok=True)

        out_png_path = f"{output_dir}/scatter.png"
        out_png_url = out_png_path.replace(self.download_dir, self.download_url)

        # Convert waterbody_ids list -> comma string (R script currently reads a single arg)
        waterbody_ids_arg = "NULL"
        if using_waterbodies:
            waterbody_ids_arg = ",".join([str(x) for x in waterbody_ids])

        # lat_range arg: provide as "c(min,max)" string so R can use it if you implement parsing,
        # or adjust R to accept two separate args.
        lat_range_arg = "NULL"
        if not using_waterbodies:
            lat_range_arg = f"c({lat_min},{lat_max})"

        # R args order (your script):
        # 1 input_path, 2 save_path, 3 waterbodies_path, 4 waterbody_ids, 5 waterbody_id_col, 6 lat_range
        r_args = [
            as_r_arg(joined_csv),
            output_dir,
            as_r_arg(waterbodies_shp) if using_waterbodies else "NULL",
            as_r_arg(waterbody_ids_arg),
            as_r_arg(waterbody_id_col) if using_waterbodies else "NULL",
            as_r_arg(lat_range_arg)
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

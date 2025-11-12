# Ferrybox R scripts

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/NIVANorge/niva-aquainfra/main?urlpath=rstudio)

The following scripts extract ferrybox measurements from NIVA thredds for specified parameters.   

**First** run the script **"netcdf_extract_save_fb.R"** in the **"extract_fb_data"** folder. This script extracts data for a specified time and lon/lat, if no lon/lat is given the full boundary area is returned. The extracted data is then stored in a simple R dataframe and if desired saved/downloaded to the users Downloads folder.  

The two other scripts can be run as desired. The **netcdf_coords_value_point_plot.R** creates point plot data for the specified parameter with either **longitude or latitude** on the x-axis and measurement values on the y-axis. The **netcdf_time_value_plot.R** creates a point plot with **time** on the x-axis and measurement value on y-axis. Both scripts allows the user to download the figures as PNG files.  

## Docker

The environment for the R scripts can also be created using docker

```bash
# build
today=$(date '+%Y%m%d')
docker build . -t ferry-rscripts${today}

# run an interactive session to execute several scripts
docker run -it --entrypoint /bin/bash ferry-rscripts${today}

# run one script
docker run \
  -v './testresults:/out:rw' \
  -e 'SCRIPT=netcdf_extract_save_fb.R' \
  ferry-rscripts${today}

# run one script, with input params:
mkdir testresults
docker run \
  -v './testresults:/out:rw' \
  -e 'SCRIPT=netcdf_extract_save_fb.R' \
  ferry-rscripts${today} \
  'https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc' \
  '/out/ferrybox.csv' \
  '2023-01-01' \
  '2023-12-31' \
  'temperature,salinity,oxygen_sat,chlorophyll,turbidity,fdom' \
  'null' 'null' 'null' 'null'
```

## Pygeoapi / OGC HTTP API

If you have a pygeoapi instance running (see ), you can deploy the script
`netcdf_extract_save_fb.R` as a pygeoapi process, offering an OGC API,
thus making it available via HTTP.

For this, do the following steps (for more details, please refer to the pygeoapi
docomentation):

* Build the docker image, but with the (hard-coded!) date of the last modification
 (this is to simplify version tracking and reproducibility):

```
docker build -t ferry-rscripts:20251104 .
```

* Add this snippet to the `pygeoapi-config.yml`:

```
resources:

    ...

    netcdf-extract-save-fb:
        type: process
        processor:
            name: NivaFerryboxProcessor

   ...
```

* Add this snippet to the `pygeoapi/plugin.py`:

```
...

    'process': {
        'HelloWorld': 'pygeoapi.process.hello_world.HelloWorldProcessor',
        'NivaFerryboxProcessor': 'pygeoapi.process.niva-aquainfra.pygeoapi_processes.netcdf_extract_save_fb.NivaFerryboxProcessor',
        ...
    }

...
```

* Install them by running this in the directory (and in the virtual environment) where pygeoapi is installed:

```
source venv/bin/activate
cd pygeoapi
pip install -e .
```

* Re-generate the `pygeoapi-openapi.yml` file by running this in the directory (and in
the virtual environment) where pygeoapi is installed:

```
source venv/bin/activate
cd pygeoapi
export PYGEOAPI_CONFIG=pygeoapi-config.yml
export PYGEOAPI_OPENAPI=pygeoapi-openapi.yml
date; pygeoapi openapi generate $PYGEOAPI_CONFIG --output-file $PYGEOAPI_OPENAPI
```

* Restart the pygeoapi instance

* And then call it via http, e.g.:

```
export PYSERVER="your.pygeoapi.instance.com/pygeoapi"
curl -X POST https://${PYSERVER}/processes/netcdf-extract-save-fb/execution \
--header 'Content-Type: application/json' \
--data '{
    "inputs": {
        "url_thredds": "https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc",
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "study_area_bbox": {"bbox": [42.08333, 8.15250, 50.24500, 29.73583]},
        "parameters": ["temperature", "salinity", "oxygen_sat", "chlorophyll", "turbidity", "fdom"]
    }
}'; date
```

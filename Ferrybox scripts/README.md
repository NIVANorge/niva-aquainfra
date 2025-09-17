# Ferrybox R scripts

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/NIVANorge/niva-aquainfra/main?urlpath=rstudio)

The following scripts extract ferrybox measurements from NIVA thredds for specified parameters.   

**First** run the script **"netcdf_extract_save_fb.R"** in the **"extract_fb_data"** folder. This script extracts data for a specified time and lon/lat, if no lon/lat is given the full boundary area is returned. The extracted data is then stored in a simple R dataframe and if desired saved/downloaded to the users Downloads folder.  

The two other scripts can be run as desired. The **netcdf_coords_value_point_plot.R** creates point plot data for the specified parameter with either **longitude or latitude** on the x-axis and measurement values on the y-axis. The **netcdf_time_value_plot.R** creates a point plot with **time** on the x-axis and measurement value on y-axis. Both scripts allows the user to download the figures as PNG files.  

## Docker

The environment for the R scripts can also be created using docker

```bash
# build
docker build . -t ferry-rscripts
# run one command
docker run ferry-rscripts
# run an interactive session to execute several scripts
docker run -it --entrypoint /bin/bash ferry-rscripts
```
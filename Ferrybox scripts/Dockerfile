# Use the official R base image
FROM r-base:4.4.1

RUN apt-get update && apt-get install -y \
    libnetcdf-dev \
    libx11-dev \
    libcurl4-openssl-dev \
    pandoc \
    libfreetype6-dev \
    libtiff-dev \
    libwebp-dev \
    libfontconfig1-dev \
    libfribidi-dev \
    libharfbuzz-dev \
    libnetcdf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /code

COPY dependencies.R dependencies.R

RUN R --slave --no-restore -e 'source("dependencies.R")'

COPY all_functions/ all_functions/

# Add an entrypoint that can deal with CLI arguments that contain spaces:
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD []


# Example build command:
#today=$(date '+%Y%m%d')
#docker build . -t ferry-rscripts:${today}

# Example run command:
# run one script (netcdf_extract_save_fb.R), with input params:
# netcdf_extract_fb_data.R
date; docker run \
  -v './testresults:/out:rw' \
  -e 'SCRIPT=netcdf_extract_fb_data.R' \
  ferry-rscripts:${today} \
  # Thredds link to FerryBox data
  'https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc' \
  # Output CSV (if only a directory is given, defaults to ferrybox.csv)
  '/out/myferryboxtest.csv' \
  # Parameters (NULL = ALL)
  'temperature,salinity,chlorophyll,turbidity' \
  # Start date
  '2023-01-01' \
  # End date
  '2023-12-31' \
  # Bounding box (minLon maxLon minLat maxLat) or 'null' 'null' 'null' 'null'
  'null' 'null' 'null' 'null'


# netcdf_logger_extract.R
date; docker run \
  -v './testresults:/out:rw' \
  -e 'SCRIPT=netcdf_logger_extract.R' \
  ferry-rscripts:${today} \
  'https://thredds.niva.no/thredds/dodsC/datasets/loggers/glomma/baterod.nc' \
  # Output CSV (if only a directory is given, defaults to logger.csv)
  '/out/data/myloggertest.csv' \
  # Parameters (NULL = ALL)
  'NULL' \
  # Start date
  '2023-01-01' \
  # End date
  '2023-12-31'


# netcdf_assessment_area.R
date; docker run \
  -v './testresults:/out:rw' \
  -e 'SCRIPT=netcdf_assessment_area.R' \
  ferry-rscripts:${today} \
  # Input FerryBox CSV
  '/out/data/myferryboxtest.csv' \
  # Output plot (if only a directory is given, defaults to assessment_area.png)
  '/out/plots/mypositionplottest.png' \
  # Input river/logger CSV
  '/out/data/myloggertest.csv' \
  # Waterbodies shapefile (NULL if none)
  'NULL'


# netcdf_scatter_station_plot.R
date; docker run \
  -v './testresults:/out:rw' \
  -e 'SCRIPT=netcdf_scatter_station_plot.R' \
  ferry-rscripts:${today} \
  # Input FerryBox CSV
  '/out/data/myferryboxtest.csv' \
  # Output plot (if only a directory is given, defaults to ferrybox_scatter.png)
  '/out/plots/myscatterplottest.png' \
  # Parameter X
  'chlorophyll' \
  # Parameter Y
  'salinity'


# netcdf_join_dataframes.R
date; docker run \
  -v './testresults:/out:rw' \
  -e 'SCRIPT=netcdf_join_dataframes.R' \
  ferry-rscripts:${today} \
  # FerryBox dataframe CSV
  '/out/data/myferryboxtest.csv' \
  # River/logger dataframe CSV
  '/out/data/myloggertest.csv' \
  # Parameter from first dataframe
  'turbidity' \
  # Parameter from second dataframe
  'turbidity_avg' \
  # Station column name in second dataframe
  'station_name' \
  # Station ID/name to filter in second dataframe
  'Baterod' \
  # Time column in first dataframe
  'datetime' \
  # Time column in second dataframe
  'datetime' \
  # Output joined CSV (if only a directory is given, defaults to joined.csv)
  '/out/data/myjoinedtest.csv'


# netcdf_scatter_datax_vs_datay.R
date; docker run \
  -v './testresults:/out:rw' \
  -e 'SCRIPT=netcdf_scatter_datax_vs_datay.R' \
  ferry-rscripts:${today} \
  # Input joined CSV
  '/out/data/myjoinedtest.csv' \
  # Output plot (if only a directory is given, defaults to scatter.png)
  '/out/plots/scatter.png' \
  # Waterbody shapefile (NULL if none)
  'NULL' \
  # Waterbody IDs/names to summarise across (NULL if none)
  'NULL' \
  # Waterbody ID column name in shapefile (NULL if none)
  'NULL' \
  # Latitude range to summarise across (e.g. c(59.1,59.2) as text, or NULL)
  'c(59.1,59.2)'


# netcdf_tile_plot.R
date; docker run \
  -v './testresults:/out:rw' \
  -e 'SCRIPT=netcdf_tile_plot.R' \
  ferry-rscripts:${today} \
  # Input FerryBox CSV
  '/out/data/myferryboxtest.csv' \
  # Output plot (if only a directory is given, defaults to ferrybox_tile.png)
  '/out/plots/mytileplottest.png' \
  # Start date
  '2023-01-01' \
  # End date
  '2023-12-31' \
  # Parameters (comma-separated)
  'salinity,chlorophyll' \
  # Latitude filter (minLat maxLat) or 'null' 'null'
  'null' 'null' \
  # Storm date (or 'null')
  '2023-08-08'




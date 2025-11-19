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

COPY position_plot/position_plot/
COPY extract_fb_data/ extract_fb_data/
COPY time_series/ time_series/

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
#docker run \
#  -v './testresults:/out:rw' \
#  -e 'SCRIPT=netcdf_extract_save_fb.R' \
#  ferry-rscripts:${today} \
#  'https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc' \
#  '/out/ferrybox.csv' \
#  '2023-01-01' \
#  '2023-12-31' \
#  'temperature,salinity,oxygen_sat,chlorophyll,turbidity,fdom' \
#  'null' 'null' 'null' 'null'


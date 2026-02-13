# Use the official R base image
FROM r-base:4.4.1

# Include git commit hash as label (at the end):
ARG GIT_COMMIT

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

# Include git commit hash as label:
LABEL org.opencontainers.image.revision=$GIT_COMMIT


# Example build command:
# This includes the git commit hash, so please
# make sure all your changes are committed/stashed:
#today=$(date '+%Y%m%d')
#githash=$(git rev-parse --short HEAD)
#docker build \
#  --build-arg GIT_COMMIT=${githash} \
#  -t ferry-rscripts:${today}-${githash} .


# Example run command:
# More more example commands and for an explanation of the parameters, please
# refer to the README.md file in the directory of the Ferrybox scripts.
# How to run one script (netcdf_extract_save_fb.R), with input params:
#
#date; docker run \
#  -v './testresults:/out:rw' \
#  -e 'SCRIPT=netcdf_extract_fb_data.R' \
#  ferry-rscripts:${today}-${githash} \
#  'https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc' \
#  '/out/myferryboxtest.csv' \
#  'temperature,salinity,chlorophyll,turbidity' \
#  '2023-01-01' \
#  '2023-12-31' \
#  'null' 'null' 'null' 'null'



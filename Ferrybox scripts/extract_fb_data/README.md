# Ferrybox data extraction
This R script extracts raw Ferrybox data from the NIVA threds server.  # https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc 

# Features
- Pulls data directly from NIVA thredds server (e.g. MS Color Fantasy Ferrybox)
- Select a date range
- Filter by longitude and latitude bounding box (optional, if nothing is specified entire boundary area is returned)
- Select between available parameters, by default all parameters are selected (temperature, salinity, oxygen_sat, chlorophyll, turbidity, fdom)
- Output data as R dataframe and .csv
- Supports local Rstudio use and CLI/Docker        

---

## Run example (command line) 
# Rstudio
source("extract_fb_data/netcdf_extract_save_fb.R")

This will:
- Use default URL (Color Fantasy dataset)
- Use default date range (2023-01-01 to 2023-12-31)
- Save output as data/out/ferrybox_default.csv

# CLI 
Rscript extract_fb_data/netcdf_extract_save_fb.R \
  "https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc" \
  "2023-01-01" \
  "2023-12-31" \
  "./output/results.csv"

- Output goes to: ./output/results.csv
- Output folder is created if missing
- Parameters default to all available variables

# Docker
```bash
docker run --rm \
  -v $(pwd)/results:/out \
  -e "R_SCRIPT=netcdf_extract_save_fb.R" niva_ferrybox:latest \
  "https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc" \
  "2023-01-01" "2023-12-31" "/out/ferrybox_2023.csv"


# Ferrybox position plot
Position plot of ferrybox for specified parameter. **Before** runing this script, ferrybox data must be extracted using the "netcdf_extract_save_fb.R" script.  
The output of this script is a point plot showing the position

# Features
- Spatial representation of sampling points 
- Select between available parameters, by default all parameters are selected (temperature, salinity, oxygen_sat, chlorophyll, turbidity, fdom)
- Output data as R image and .png image 
- Supports local Rstudio use and CLI/Docker        

---

## Run example (command line) 
# Rstudio
source("position_plot/netcdf_position_plot.R")

This will:
- Use input .csv file (must be comma seperated)
- Use default date range in dataset
- Save output as data/out/ferrybox_position_plot.png

# CLI 
Rscript position_plot/netcdf_position_plot.R \
  "path_to_csv" \
  "salinity" \
  "./output/position_plot.png"

- Output goes to: ./output/position_plot.png
- Output folder is created if missing
- Parameters default to all available if none is specified.

# Docker
```bash
docker run --rm \
  -v $(pwd)/results:/out \
  -e "R_SCRIPT=netcdf_position_plot.R" niva_ferrybox:latest \
  "path_to_csv" \
  "salinity" "/out/ferrybox_position.png"

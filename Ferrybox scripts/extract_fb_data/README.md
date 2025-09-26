# Ferrybox data extraction

This R script extracts raw Ferrybox data from the NIVA THREDDS server.  
The user can select a time period, geographic bounding box, and specific parameters.  
The script returns a dataframe in R and can optionally save the results as a CSV file.  

---

## Run example (command line)

```bash
Rscript extract_fb_data/fetch_ferrybox.R \
  --date_from "2023-01-01" \
  --date_to   "2023-01-31" \
  --vars "temperature,salinity,chlorophyll" \
  --save_csv TRUE

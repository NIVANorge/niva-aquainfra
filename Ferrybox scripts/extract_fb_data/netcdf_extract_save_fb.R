args <- commandArgs(trailingOnly = TRUE)
print(paste0('R Command line args: ', args))
url             = args[1] # e.g. "https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc"
start_date      = args[2] # e.g. "2023-01-01"
end_date        = args[3] # e.g. "2023-12-31"
out_result_path = args[4] # path where output CSV will be written

out_file_name = basename(out_result_path)
out_dir_name = dirname(out_result_path)


# R script for extracting Ferrybox data (MS Color Fantasy, Oslo–Kiel) 
# from THREDDS netCDF server and saving as dataframe
# THREDDS catalog: https://thredds.niva.no/thredds/catalog/subcatalogs/ferryboxes.html

# --- Load (and install if necessary) required packages ---
required_packages <- c("tidyverse", "ncdf4", "lubridate")
new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
if (length(new_packages)) install.packages(new_packages)
invisible(lapply(required_packages, function(p) 
  suppressPackageStartupMessages(library(p, character.only = TRUE))))

# --- Open THREDDS dataset ---
#url <- "https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc"
fb_nc <- tryCatch(nc_open(url), error = function(e) 
  stop("Could not open THREDDS dataset: ", conditionMessage(e)))

# --- Extract and convert time dimension ---
time_raw <- ncvar_get(fb_nc, "time")
time_converted <- as.POSIXct(time_raw, origin = "1970-01-01", tz = "UTC")

# --- Helper function: filter time range ---
get_time_index <- function(start_date = NULL, end_date = NULL) {
  if (xor(is.null(start_date), is.null(end_date))) 
    stop("Please specify both start and end date.")
  if (is.null(start_date)) return(seq_along(time_converted))
  start_date <- as.POSIXct(paste0(start_date, " 00:00:00"), tz = "UTC")
  end_date   <- as.POSIXct(paste0(end_date,   " 23:59:59"), tz = "UTC")
  which(time_converted >= start_date & time_converted <= end_date)
}

# Example: extract data for 2023
#time_index <- get_time_index("2023-01-01", "2023-12-31")
time_index <- get_time_index(start_date, end_date)

# --- Identify available variables ---
all_vars   <- names(fb_nc$var)
param_vars <- all_vars[!grepl("_qc|latitude|longitude|trajectory_name|time", all_vars)]
print(param_vars)
# Available parameters: "temperature" "salinity" "oxygen_sat" 
#                       "chlorophyll" "turbidity" "fdom"

# --- Main function: extract selected variables ---
df_ferrybox <- function(parameters, time_index,
                        lon_min = NULL, lon_max = NULL,
                        lat_min = NULL, lat_max = NULL,
                        save_csv = TRUE, out_dir = out_dir_name, out_name = out_file_name) {
  invalid_params <- setdiff(parameters, param_vars)
  if (length(invalid_params)) {
    stop("Invalid parameter(s): ", paste(invalid_params, collapse = ", "),
         "\nAvailable: ", paste(param_vars, collapse = ", "))
  }
  
  # Extract spatial/temporal dimensions
  lat_all <- ncvar_get(fb_nc, "latitude")
  lon_all <- ncvar_get(fb_nc, "longitude")
  lat  <- lat_all[time_index]
  lon  <- lon_all[time_index]
  time <- time_converted[time_index]
  
  # Default to full bounding box if none is given
  if (all(is.null(c(lon_min, lon_max, lat_min, lat_max)))) {
    lon_min <- min(lon); lon_max <- max(lon)
    lat_min <- min(lat); lat_max <- max(lat)
    message("No bounding box specified – returning full area for selected time range.")
  }
  
  # Check coordinate limits
  if (lon_min < min(lon) | lon_max > max(lon) | 
      lat_min < min(lat) | lat_max > max(lat)) {
    stop("Coordinates outside data range.\nlon: ", min(lon), "–", max(lon),
         "\nlat: ", min(lat), "–", max(lat))
  }
  
  # Function to read a single variable
  read_param <- function(param) {
    x <- ncvar_get(fb_nc, param)[time_index]
    fv <- ncatt_get(fb_nc, param, "_FillValue")$value
    mv <- ncatt_get(fb_nc, param, "missing_value")$value
    if (!is.null(fv) && !is.na(fv)) x[x == fv] <- NA
    if (!is.null(mv) && !is.na(mv)) x[x == mv] <- NA
    
    unit <- ncatt_get(fb_nc, param, "units")$value
    tibble::tibble(
      datetime  = time,
      latitude  = lat,
      longitude = lon,
      value     = x,
      unit      = unit %||% NA_character_,
      parameter = param
    ) |> dplyr::filter(!is.na(value))
  }
  
  df_combined <- purrr::map(parameters, read_param) |> dplyr::bind_rows() |>
    dplyr::mutate(
      year   = lubridate::year(datetime),
      month  = lubridate::month(datetime),
      day    = lubridate::day(datetime),
      hour   = lubridate::hour(datetime),
      minute = lubridate::minute(datetime)
    ) |>
    dplyr::filter(dplyr::between(longitude, lon_min, lon_max),
                  dplyr::between(latitude,  lat_min,  lat_max))
  
  # Save as CSV if requested
  if (isTRUE(save_csv)) {
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
    param_tag <- gsub("[^A-Za-z0-9_-]+", "-", paste(parameters, collapse="_"))
    stamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    file_name <- out_name %||% sprintf("ferrybox_%s_%s.csv", param_tag, stamp)
    file_path <- file.path(out_dir, file_name)
    utils::write.csv(df_combined, file_path, row.names = FALSE)
    message("Saved CSV: ", file_path)
    attr(df_combined, "saved_csv_path") <- file_path
  } else {
    message('To save as CSV, set save_csv=TRUE (written to ', out_dir, ').')
  }
  df_combined
}

`%||%` <- function(a,b) if (is.null(a)) b else a

# --- Example run ---
df_all <- df_ferrybox(
  parameters = c("temperature", "salinity", "oxygen_sat",
                 "chlorophyll", "turbidity", "fdom"),
  time_index = time_index,
  save_csv   = FALSE
)

# --- Close dataset ---
nc_close(fb_nc)

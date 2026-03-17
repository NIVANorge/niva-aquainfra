#!/usr/bin/env Rscript
library(tidyverse)
library(ncdf4)
library(lubridate)
library(data.table)


`%||%` <- function(x, y) {
  if (is.null(x) || all(is.na(x))) y else x
}

# -------------------------------------------------------------------
# Main function which reads the ferrybox data from URL
# -------------------------------------------------------------------
df_ferrybox <- function(
    source,                   
    parameters      = NULL,    
    start_date      = NULL,    
    end_date        = NULL,
    lon_min         = NULL,
    lon_max         = NULL,
    lat_min         = NULL,
    lat_max         = NULL
) {
  
  # ------------------------------------------------------------
  # source is NetCDF via URL (THREDDS)
  # ------------------------------------------------------------
  url <- source
  message("Opening NetCDF/THREDDS dataset: ", url)
  fb_nc <- tryCatch(
    nc_open(url),
    error = function(e) stop("Could not open URL: ", conditionMessage(e))
  )
  on.exit(try(nc_close(fb_nc), silent = TRUE), add = TRUE)
  
  # --- Time------------------------------------------------------
  time_raw       <- ncvar_get(fb_nc, "time")
  time_converted <- as.POSIXct(time_raw, origin = "1970-01-01", tz = "UTC")
  
  # --- Standard bounding box ----------
  lon_all <- ncvar_get(fb_nc, "longitude")
  lat_all <- ncvar_get(fb_nc, "latitude")

  
  # --- Variables -----------------------------------------------
  all_vars   <- names(fb_nc$var)
  not_contain <- "_qc|latitude|longitude|trajectory_name|time"
  param_vars <- all_vars[!grepl(not_contain, all_vars)]
  
  # If no parameters is specified, use all
  if (is.null(parameters)) {
    parameters <- param_vars
  }
  
  message("URL:   ", url)
  message("Full period:   ", format(min(time_converted), "%Y-%m-%d"), " → ",
          format(max(time_converted), "%Y-%m-%d"))
  message("Parameters: ", paste(parameters, collapse = ", "))
 
  
  # --- Time index helper function ---------------------------
  get_time_index <- function(start_date = NULL, end_date = NULL) {
    
    if (!is.null(start_date) && is.na(start_date)) start_date <- NULL
    if (!is.null(end_date)   && is.na(end_date))   end_date   <- NULL
    
    if (xor(is.null(start_date), is.null(end_date)))
      stop("start_date is after end_date.")
    
    if (is.null(start_date) && is.null(end_date)) {
      message("No date interval specified → using entire period.")
      return(seq_along(time_converted))
    }
    
    start_dt <- as.POSIXct(paste0(start_date, " 00:00:00"), tz = "UTC")
    end_dt   <- as.POSIXct(paste0(end_date,   " 23:59:59"), tz = "UTC")
    
    if (start_dt > end_dt) stop("start_date is after end_date.")
    
    idx <- which(time_converted >= start_dt & time_converted <= end_dt)
    
    if (length(idx) == 0) {
      stop("No data found in the date interval.")
    }
    
    idx
  }
  
  time_index <- get_time_index(start_date, end_date)
  
  # --- check parameters ------------------------------------------
  invalid_params <- setdiff(parameters, param_vars)
  if (length(invalid_params)) {
    stop(
      "Invalid parameter(s): ", paste(invalid_params, collapse = ", "),
      "\nAvailable: ", paste(param_vars, collapse = ", ")
    )
  }
  
  lon_all_all <- ncvar_get(fb_nc, "longitude")
  lat_all_all <- ncvar_get(fb_nc, "latitude")
  lat  <- lat_all_all[time_index]
  lon  <- lon_all_all[time_index]
  time <- time_converted[time_index]
  
  # --- Bounding box ----
  # If NULL is parsed use default value
  if (is.null(lon_min)) lon_min <- min(lon, na.rm = TRUE)
  if (is.null(lon_max)) lon_max <- max(lon, na.rm = TRUE)
  if (is.null(lat_min)) lat_min <- min(lat, na.rm = TRUE)
  if (is.null(lat_max)) lat_max <- max(lat, na.rm = TRUE)
  
  # Return message with lat/lon extracted (what was available)
  lon_min_available <- min(lon, na.rm = TRUE)
  lon_max_available <- max(lon, na.rm = TRUE)
  lat_min_available <- min(lat, na.rm = TRUE)
  lat_max_available <- max(lat, na.rm = TRUE)
  
  message("Data available in BBOX:  ",
          paste(c(lon_min_available, lon_max_available,
                  lat_min_available, lat_max_available), collapse = ", "))
  
  # --- Read parameter ------------------------------------
  read_param <- function(param) {
    x  <- ncvar_get(fb_nc, param)[time_index]
    fv <- ncatt_get(fb_nc, param, "_FillValue")$value
    mv <- ncatt_get(fb_nc, param, "missing_value")$value
    if (!is.null(fv) && !is.na(fv)) x[x == fv] <- NA
    if (!is.null(mv) && !is.na(mv)) x[x == mv] <- NA
    unit <- ncatt_get(fb_nc, param, "units")$value %||% NA_character_
    
    tibble(
      datetime  = time,
      latitude  = lat,
      longitude = lon,
      value     = x,
      unit      = unit,
      parameter = param
    ) %>%
      filter(!is.na(value))
  }
  
  # --- combine parameter --------------------------------------
  df_combined <- purrr::map(parameters, read_param) %>%
    bind_rows() %>%
    mutate(
      year    = year(datetime),
      month   = month(datetime),
      day     = day(datetime),
      hour    = hour(datetime),
      minute  = minute(datetime)
    ) %>%
    filter(
      between(longitude, lon_min, lon_max),
      between(latitude,  lat_min, lat_max)
    ) 
  
  return(df_combined)
}
# -------------------------------------------------------------------
as_null_if_blank <- function(x) {
  if (is.null(x)) return(NULL)
  x <- trimws(x)
  if (!nzchar(x) || tolower(x) == "null") NULL else x
}

parse_parameters <- function(x) {
  x <- as_null_if_blank(x)
  if (is.null(x)) return(NULL)
  trimws(strsplit(x, ",", fixed = TRUE)[[1]])
}

args <- commandArgs(trailingOnly = TRUE)
message("R Command line args: ", paste(args, collapse = " | "))

if (length(args) < 2) stop("Provide source (URL/CSV) and path to save")

source    <- args[1]
save_path <- args[2]
message("[arg] input csv:  ", source)
message("[arg] output csv: ", save_path)

parameters <- if (length(args) >= 3) parse_parameters(args[3]) else NULL
message("[arg] parameters: ", parameters)

start_date <- if (length(args) >= 4) as_null_if_blank(args[4]) else NULL
end_date   <- if (length(args) >= 5) as_null_if_blank(args[5]) else NULL
message("[arg] start_date: ", start_date)
message("[arg] end_date:   ", start_date)

lon_min <- if (length(args) >= 6) as_null_if_blank(args[6]) else NULL
lon_max <- if (length(args) >= 7) as_null_if_blank(args[7]) else NULL
lat_min <- if (length(args) >= 8) as_null_if_blank(args[8]) else NULL
lat_max <- if (length(args) >= 9) as_null_if_blank(args[9]) else NULL
message("[arg] lon_min, lon_max: ", lon_min, " - ", lon_max)
message("[arg] lat_min, lat_max: ", lat_min, " - ", lat_max)

# Convert numeric bbox args if present
lon_min <- if (!is.null(lon_min)) as.numeric(lon_min) else NULL
lon_max <- if (!is.null(lon_max)) as.numeric(lon_max) else NULL
lat_min <- if (!is.null(lat_min)) as.numeric(lat_min) else NULL
lat_max <- if (!is.null(lat_max)) as.numeric(lat_max) else NULL


# e.g URL to netcdf  "https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc"

df_all <- df_ferrybox(
  source      = source,
  parameters  = parameters,
  start_date  = start_date,
  end_date    = end_date,
  lon_min     = lon_min,
  lon_max     = lon_max,
  lat_min     = lat_min,
  lat_max     = lat_max
  )

# If .csv name is pased in save_path using that as saving name else default "ferrybox.csv" is used. 
if (grepl("\\.csv$", save_path, ignore.case = TRUE)) {
  file_path <- save_path
  dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
} else {
  dir.create(save_path, recursive = TRUE, showWarnings = FALSE)
  file_path <- file.path(save_path, "ferrybox.csv")
}

message(paste0('Writing result to CSV file: ', file_path, ' (may take a little while if the data is big)...'))

utils::write.table(
  df_all,
  file = file_path,
  sep = ",",
  row.names = FALSE,
  col.names = TRUE,
  quote = TRUE,
  append = FALSE
)

message(paste0('Writing result to CSV file... done'))

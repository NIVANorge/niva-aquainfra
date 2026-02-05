#!/usr/bin/env Rscript

library(tidyverse)
library(ncdf4)
library(lubridate)
library(data.table)

`%||%` <- function(x, y) if (is.null(x) || all(is.na(x))) y else x

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

# -------------------------------------------------------------------
# Main function: extract logger time series from NetCDF (THREDDS)
# -------------------------------------------------------------------
df_logger <- function(
    source,
    parameters   = NULL,
    start_date   = NULL,
    end_date     = NULL
) {
  url <- source
  message("Opening NetCDF/THREDDS dataset: ", url)
  
  logger_nc <- tryCatch(
    nc_open(url),
    error = function(e) stop("Could not open THREDDS dataset: ", conditionMessage(e))
  )
  on.exit(try(nc_close(logger_nc), silent = TRUE), add = TRUE)
  
  # --- Time dimension (logger uses seconds since 1970-01-01) ---
  time_raw <- ncvar_get(logger_nc, "time")
  time_converted <- as.POSIXct(time_raw, origin = "1970-01-01", tz = "UTC")
  
  # --- Available variables (exclude non-parameters) ---
  all_vars <- names(logger_nc$var)
  exclude_pattern <- "_qc|station_name|latitude|longitude|trajectory_name|time"
  param_vars <- all_vars[!grepl(exclude_pattern, all_vars)]
  
  # If no parameters provided, use all available data variables
  if (is.null(parameters)) {
    parameters <- param_vars
    message("No parameters specified → using all available parameters.")
  } else {
    parameters <- tolower(parameters)
    # Keep original var names (NetCDF is case-sensitive), but your vars are already lower
  }
  
  message("TIME: ", format(min(time_converted), "%Y-%m-%d"), " → ", format(max(time_converted), "%Y-%m-%d"))
  message("PARAM: ", paste(parameters, collapse = ", "))
  
  # --- Validate parameters ---
  invalid_params <- setdiff(parameters, param_vars)
  if (length(invalid_params)) {
    stop(
      "Invalid parameter(s): ", paste(invalid_params, collapse = ", "),
      "\nAvailable: ", paste(param_vars, collapse = ", ")
    )
  }
  
  # --- Time index helper ---
  get_time_index <- function(start_date = NULL, end_date = NULL) {
    if (xor(is.null(start_date), is.null(end_date))) {
      stop("Please specify both start_date and end_date, or neither.")
    }
    if (is.null(start_date) && is.null(end_date)) {
      message("No date interval specified → using entire period.")
      return(seq_along(time_converted))
    }
    
    start_dt <- as.POSIXct(paste0(start_date, " 00:00:00"), tz = "UTC")
    end_dt   <- as.POSIXct(paste0(end_date,   " 23:59:59"), tz = "UTC")
    
    if (start_dt > end_dt) stop("start_date is after end_date.")
    
    idx <- which(time_converted >= start_dt & time_converted <= end_dt)
    if (length(idx) == 0) stop("No data found in the requested date interval.")
    
    idx
  }
  
  time_index <- get_time_index(start_date, end_date)
  
  # --- Station metadata (point geometry) ---
  lat <- ncvar_get(logger_nc, "latitude")
  lon <- ncvar_get(logger_nc, "longitude")
  station_name <- NA_character_
  if ("station_name" %in% names(logger_nc$var)) {
    # station_name is char array; ncvar_get returns a character vector/array
    station_name <- tryCatch(
      paste0(ncvar_get(logger_nc, "station_name"), collapse = "") |> trimws(),
      error = function(e) NA_character_
    )
  }
  
  # --- Read one parameter into long format ---
  read_param <- function(param) {
    x <- ncvar_get(logger_nc, param)[time_index]
    
    fv <- ncatt_get(logger_nc, param, "_FillValue")$value
    mv <- ncatt_get(logger_nc, param, "missing_value")$value
    if (!is.null(fv) && !is.na(fv)) x[x == fv] <- NA
    if (!is.null(mv) && !is.na(mv)) x[x == mv] <- NA
    
    unit <- ncatt_get(logger_nc, param, "units")$value %||% NA_character_
    
    tibble(
      datetime = time_converted[time_index],
      latitude = as.numeric(lat)[1],
      longitude = as.numeric(lon)[1],
      station_name = station_name,
      value = x,
      unit = unit,
      parameter = param
    ) |>
      filter(!is.na(value))
  }
  
  df_combined <- purrr::map(parameters, read_param) |>
    bind_rows() |>
    mutate(
      year   = year(datetime),
      month  = month(datetime),
      day    = day(datetime),
      hour   = hour(datetime),
      minute = minute(datetime)
    )
  
  return(df_combined)
}

# -------------------------------------------------------------------
# CLI args (script #2 style)
# -------------------------------------------------------------------
# Args order:
#  1: url              (required) -> THREDDS OPeNDAP .nc
#  2: save_path    (required) -> full file path, e.g. "/out/logger.csv"
#  3: start_date       (optional) -> "YYYY-MM-DD" or "null"
#  4: end_date         (optional) -> "YYYY-MM-DD" or "null"
#  5: parameters       (optional) -> "temp_water_avg,phvalue_avg" or "null"

args <- commandArgs(trailingOnly = TRUE)
message("R Command line args: ", paste(args, collapse = " | "))

if (length(args) < 2) {
  stop("Provide url and save path for csv.")
}

source <- args[1]
save_path <- args[2]

parameters <- if (length(args) >= 3) parse_parameters(args[3]) else NULL
start_date <- if (length(args) >= 4) as_null_if_blank(args[4]) else NULL
end_date   <- if (length(args) >= 5) as_null_if_blank(args[5]) else NULL


df_logger <- df_logger(
  source = source,
  parameters = parameters,
  start_date = start_date,
  end_date = end_date
)


## If .csv name is pased in save_path using that as saving name else default "ferrybox.csv" is used. 
if (grepl("\\.csv$", save_path, ignore.case = TRUE)) {
  file_path <- save_path
  dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
} else {
  dir.create(save_path, recursive = TRUE, showWarnings = FALSE)
  file_path <- file.path(save_path, "logger.csv")
}

print(paste0('Write result to csv file: ', file_path))

utils::write.table(
  df_logger,
  file = file_path,
  sep = ",",
  row.names = FALSE,
  col.names = TRUE,
  quote = TRUE,
  append = FALSE)



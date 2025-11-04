# R script for extracting Ferrybox data (MS Color Fantasy, Oslo–Kiel) 
# from THREDDS netCDF server and saving as dataframe
# THREDDS catalog: https://thredds.niva.no/thredds/catalog/subcatalogs/ferryboxes.html
#!/usr/bin/env Rscript

# --- Load (and install if necessary) required packages -----------------------
required_packages <- c("tidyverse", "ncdf4", "lubridate")
new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
if (length(new_packages)) install.packages(new_packages)
invisible(lapply(required_packages, function(p)
  suppressPackageStartupMessages(library(p, character.only = TRUE))))

# --- Helpers -----------------------------------------------------------------
`%||%` <- function(a, b) if (is.null(a)) b else a

as_null_if_blank <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x) || (is.character(x) && trimws(x) == "")) return(NULL)
  x
}

# --- CLI args + interactive fallbacks ----------------------------------------
args <- commandArgs(trailingOnly = TRUE)
print(paste0("R Command line args: ", paste(args, collapse = " | ")))

if (length(args) >= 4) {
  message("Reading CLI args...")
  print(paste0('R Command line args: ', args))
  url             <- args[1]
  start_date      <- args[2]
  end_date        <- args[3]
  out_result_path <- args[4]
  parameters      <- args[5]
  lon_min         <- args[6]
  lon_max         <- args[7]
  lat_min         <- args[8]
  lat_max         <- args[9]

  # Split/define parameter set:
  if (is.character(parameters) && trimws(parameters) == "") {
    parameters <- "temperature,salinity,oxygen_sat,chlorophyll,turbidity,fdom"
    message("No parameter set passed, using hardcoded set: ", parameters)
  }
  parameters <- strsplit(parameters, "\\s*,\\s*")[[1]]

} else {
  message("No CLI args detected → using defaults...")
  url             <- "https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc"
  start_date      <- "2023-01-01"
  end_date        <- "2023-12-31"
  out_result_path <- "data/out/ferrybox_default.csv"
  parameters      <- c("temperature", "salinity", "oxygen_sat",
                       "chlorophyll", "turbidity", "fdom")
  lon_min <- NULL
  lon_max <- NULL
  lat_min <- NULL
  lat_max <- NULL
}

# Normalize optional blanks to NULL
start_date <- as_null_if_blank(start_date)
end_date   <- as_null_if_blank(end_date)
lon_min    <- as_null_if_blank(lon_min)
lon_max    <- as_null_if_blank(lon_max)
lat_min    <- as_null_if_blank(lat_min)
lat_max    <- as_null_if_blank(lat_max)

# Derive out_dir/out_name from out_result_path (file or folder)
is_csv_target <- !is.null(out_result_path) && grepl("\\.csv$", out_result_path, ignore.case = TRUE)
if (is_csv_target) {
  out_dir  <- dirname(out_result_path)
  out_name <- basename(out_result_path)
} else {
  out_dir  <- out_result_path %||% "data/out"
  out_name <- NULL
}

message("URL:   ", url)
message("START: ", start_date %||% "<full range>")
message("END:   ", end_date %||% "<full range>")
message("OUT:   ", if (is_csv_target) file.path(out_dir, out_name) else paste0(out_dir, " (dir)"))
message("PARAM  ", paste(parameters, collapse=", ")))
message("BBOX   ", paste(c(lon_min, lon_max, lat_min, lat_max), collapse=", "))


# --- Open THREDDS dataset ----------------------------------------------------
message(paste("Opening dataset:", url))
fb_nc <- tryCatch(nc_open(url), error = function(e)
  stop("Could not open THREDDS dataset: ", conditionMessage(e)))
on.exit(try(nc_close(fb_nc), silent = TRUE), add = TRUE)

# --- Time dimension ----------------------------------------------------------
time_raw       <- ncvar_get(fb_nc, "time")
time_converted <- as.POSIXct(time_raw, origin = "1970-01-01", tz = "UTC")

# --- Time index helper -------------------------------------------------------
get_time_index <- function(start_date = NULL, end_date = NULL) {
  if (!is.null(start_date) && is.na(start_date)) start_date <- NULL
  if (!is.null(end_date)   && is.na(end_date))   end_date   <- NULL
  if (xor(is.null(start_date), is.null(end_date)))
    stop("Please specify both start and end date (or neither).")
  if (is.null(start_date)) return(seq_along(time_converted))
  start_date <- as.POSIXct(paste0(start_date, " 00:00:00"), tz = "UTC")
  end_date   <- as.POSIXct(paste0(end_date,   " 23:59:59"), tz = "UTC")
  which(time_converted >= start_date & time_converted <= end_date)
}
time_index <- get_time_index(start_date, end_date)

# --- Variables ---------------------------------------------------------------
# Check which variables are contained in the NetCDF and are valid/available
# (i.e. don't match the exclusion pattern):
all_vars   <- names(fb_nc$var)
message(paste("Variables present in NetCDF:", paste(all_vars, collapse=", ")))
not_contain <- "_qc|latitude|longitude|trajectory_name|time" # exclusion pattern
param_vars <- all_vars[!grepl(not_contain, all_vars)]
message(paste("Variables present in NetCDF, valid for usage:", paste(param_vars, collapse=", ")))

# --- Main extraction ---------------------------------------------------------
df_ferrybox <- function(parameters, param_vars, time_index,
                        lon_min = NULL, lon_max = NULL,
                        lat_min = NULL, lat_max = NULL,
                        save_csv = FALSE, out_dir = NULL, out_name = NULL) {

  # Check if variables passed by users are present in the netcdf and valid/available:
  invalid_params <- setdiff(parameters, param_vars)
  if (length(invalid_params)) {
    stop("Invalid parameter(s): ", paste(invalid_params, collapse = ", "),
         "\nAvailable: ", paste(param_vars, collapse = ", "))
  }

  lat_all <- ncvar_get(fb_nc, "latitude")
  lon_all <- ncvar_get(fb_nc, "longitude")
  lat  <- lat_all[time_index]
  lon  <- lon_all[time_index]
  time <- time_converted[time_index]

  if (all(is.null(c(lon_min, lon_max, lat_min, lat_max)))) {
    lon_min <- min(lon); lon_max <- max(lon)
    lat_min <- min(lat); lat_max <- max(lat)
    message("No bounding box specified – returning full area for selected time range.")
  }

  if (lon_min < min(lon) | lon_max > max(lon) |
      lat_min < min(lat) | lat_max > max(lat)) {
    stop("Coordinates outside data range.\nlon: ", min(lon), "–", max(lon),
         "\nlat: ", min(lat), "–", max(lat))
  }

  read_param <- function(param) {
    x  <- ncvar_get(fb_nc, param)[time_index]
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
    ) |>
      dplyr::filter(!is.na(value))
  }

  df_combined <- purrr::map(parameters, read_param) |>
    dplyr::bind_rows() |>
    dplyr::mutate(
      year   = lubridate::year(datetime),
      month  = lubridate::month(datetime),
      day    = lubridate::day(datetime),
      hour   = lubridate::hour(datetime),
      minute = lubridate::minute(datetime)
    ) |>
    dplyr::filter(dplyr::between(longitude, lon_min, lon_max),
                  dplyr::between(latitude,  lat_min,  lat_max))

  # --- Save as CSV if requested ---------------------------------------------
  if (isTRUE(save_csv)) {
    message("Saving CSV...")

    # If the user passed nothing, create default storage location:
    if (is.null(out_dir) || out_dir == "") {
      out_dir <- "data/out"
      message(paste("No result directory was passed, using:", out_dir))
      if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
      param_tag <- gsub("[^A-Za-z0-9_-]+", "-", paste(parameters, collapse = "_"))
      stamp     <- format(Sys.time(), "%Y%m%d_%H%M%S")
      file_name <- out_name %||% sprintf("ferrybox_%s_%s.csv", param_tag, stamp)
      message(paste("No result directory was passed, using file name:", file_name))
      file_path <- file.path(out_dir, file_name)

    # If the user passed directory and file name, use them:
    } else if (grepl("\\.csv$", out_dir, ignore.case = TRUE)) {
      file_path     <- out_dir
      message(paste("Result directory and filename was passed:", file_path))
      dir_to_create <- dirname(file_path)
      if (!dir.exists(dir_to_create)) dir.create(dir_to_create, recursive = TRUE, showWarnings = FALSE)

    # If the user provided dir only, use it plus default file name:
    } else {
      message(paste("Result directory was passed:", out_dir))
      if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
      param_tag <- gsub("[^A-Za-z0-9_-]+", "-", paste(parameters, collapse = "_"))
      stamp     <- format(Sys.time(), "%Y%m%d_%H%M%S")
      file_name <- out_name %||% sprintf("ferrybox_%s_%s.csv", param_tag, stamp)
      message(paste("Using file name:", file_name))
      file_path <- file.path(out_dir, file_name)
    }
    message("Saving CSV to: ", file_path)
    utils::write.csv(df_combined, file_path, row.names = FALSE)
    message("Saved CSV:     ", file_path)
    attr(df_combined, "saved_csv_path") <- file_path
  } else {
    message("To save as CSV, set save_csv=TRUE.")
  }

  df_combined
}

# --- Example run (works both in RStudio and CLI) -----------------------------
df_all <- df_ferrybox(
  parameters = parameters,
  param_vars = param_vars,
  time_index = time_index,
  lon_min    = lon_min,
  lon_max    = lon_max,
  lat_min    = lat_min,
  lat_max    = lat_max,
  save_csv   = TRUE,
  out_dir    = out_dir,
  out_name   = out_name
)


# --- Close dataset ---
nc_close(fb_nc)




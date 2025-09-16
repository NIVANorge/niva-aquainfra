# Rscript for loading data for Ferrybox data for FerryBox on MS Color Fantasy (Oslo-Kiel) from netcdf and setting time index
# https://thredds.niva.no/thredds/catalog/subcatalogs/ferryboxes.html?dataset=no.niva:af11ba01-dfe3-4432-b9d2-4e6fd10714db
# load (and install if necessary) the required packages
# 
# List of required packages 
required_packages <- c("dplyr","tidyverse", 
                       "tidyr","ncdf4")

# Install missing packages
new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
if(length(new_packages)) install.packages(new_packages)
# Load all packages
lapply(required_packages, library, character.only = TRUE)

# Link to website where data is extracted
# needs to be an url adress
url <- "https://thredds.niva.no/thredds/dodsC/datasets/nrt/color_fantasy.nc"
fb_nc <- nc_open(url)
print(fb_nc)
# Extract time and convert to POSIXct
time_raw <- ncvar_get(fb_nc, "time")
time_converted <- as.POSIXct(time_raw, origin = "1970-01-01", tz = "UTC")

# Filter for given time period
## Input start date period as "YYYY-MM-DD"
get_time_index <- function(start_date = NULL, end_date = NULL) {
  
  # If only one date is given, stop and return error
  if (!is.null(start_date) & is.null(end_date)) {
    stop("Insert end date.")
  }
  if (is.null(start_date) & !is.null(end_date)) {
    stop("Insert start date.")
  }
  
  # If no date is given, return full time range
  if (is.null(start_date) & is.null(end_date)) {
    message("No date input detected – returning full time range.")
    return(seq_along(time_converted))
  }
  
  # Convert text to POSIXct
  start_date <- as.POSIXct(paste0(start_date, " 00:00:00"), tz = "UTC")
  end_date   <- as.POSIXct(paste0(end_date, " 23:59:59"), tz = "UTC")
  
  # Filter time
  time_index <- which(time_converted >= start_date & time_converted <= end_date)
  
  return(time_index)
}

# Use function to filter for desired time period - Function will only extract data where data is available
# i.e. attempting to extract data until lets say 2026, will only result in data until most recent available date
time_index <- get_time_index("2023-01-01", "2023-12-31")

# Check available parameter in dataframe
all_vars <- names(fb_nc$var)
param_vars <- all_vars[!grepl("_qc|latitude|longitude|trajectory_name|time", all_vars)]
print(param_vars)
# Available parameters are "temperature"    "salinity"       "oxygen_sat"     "chlorophyll"    "turbidity"      "fdom"  



## Create function that extracts data for specified parameters from the netcdf file
df_ferrybox <- function(parameters, time_index, lon_min = NULL, lon_max = NULL,
                        lat_min = NULL,lat_max = NULL ,save_csv = NULL) {
  # Validate input parameters
  invalid_params <- setdiff(parameters, param_vars)
  if (length(invalid_params) > 0) {
    stop("Invalid parameter(s): ", paste(invalid_params, collapse = ", "),
         "\nAvailable parameters are: ", paste(param_vars, collapse = ", "))
  }
  
  # Extract shared time and position data
  time <- time_converted[time_index]
  lat  <- ncvar_get(fb_nc, "latitude",  start = min(time_index), count = length(time_index))
  lon  <- ncvar_get(fb_nc, "longitude", start = min(time_index), count = length(time_index))
  
  if(is.null(lon_min) & is.null(lon_max) & is.null(lat_min) & is.null(lat_max)) {
    message("No input coordinates - returning data for entire boundary area")
    lon_min <- min(lon)
    lon_max <- max(lon)
    lat_min <- min(lat)
    lat_max <- max(lat)
  }
  if(lon_min < min(lon) | lon_max > max(lon) | lat_min < min(lat) | lat_max > max(lat)) {
    stop("Chosen coordinates are out of boundary. Please set coordinates between:\n ",
         "longitude: ", paste(min(lon)), "-", paste(max(lon)),"\n",
         "latitude: ",paste(min(lat)), "-", paste(max(lat)))
  }
  
  
  df_list <- list()
  
  for (param in parameters) {
    data <- ncvar_get(fb_nc, param, start = min(time_index), count = length(time_index))
    
    df <- data.frame(
      datetime  = time,
      latitude  = lat,
      longitude = lon,
      value     = data
    )
    
    unit <- ncatt_get(fb_nc, param, "units")$value
    df$unit <- unit
    attr(df, "unit") <- unit
    
    # keep parameters
    df$parameter <- param
    attr(df, "parameter") <- param
    # parameter_long <- ncatt_get(fb_nc, param, "long_name")$value
    
    df[df == -999] <- NA
    is.nan.df <- function(x) do.call(cbind, lapply(x, is.nan))
    df[is.nan.df(df)] <- NA
    df <- dplyr::filter(df, !is.na(value))
    
    df_list[[param]] <- df
  }
  
  df_combined <- dplyr::bind_rows(df_list) |>
    dplyr::mutate(
      year   = lubridate::year(datetime),
      month  = lubridate::month(datetime),
      day    = lubridate::day(datetime),
      hour   = lubridate::hour(datetime),
      minute = lubridate::minute(datetime),
      datetime = lubridate::make_date(year, month, day)
    ) %>%
    filter(longitude >= lon_min,
           longitude <= lon_max,
           latitude >= lat_min,
           latitude <= lat_max)
  
  # --- Save as CSV if the users enters yes ---
  should_save <- isTRUE(save_csv) || (is.character(save_csv) && grepl("^\\s*yes\\s*$", save_csv, ignore.case = TRUE))
  if (should_save) {
    # robust finder til Downloads
    get_downloads_path <- function() {
      sys_name <- Sys.info()[["sysname"]]
      if (identical(sys_name, "Windows")) {
        return(file.path(Sys.getenv("USERPROFILE"), "Downloads"))
      } else {
        return(file.path(Sys.getenv("HOME"), "Downloads"))
      }
    }
    downloads_dir <- get_downloads_path()
    
    # creates filename with parameters and timestamp
    param_tag <- paste(parameters, collapse = "_")
    # Charachters allowed
    param_tag <- gsub("[^A-Za-z0-9_-]+", "-", param_tag)
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    file_name <- sprintf("ferrybox_%s_%s.csv", param_tag, timestamp)
    file_path <- file.path(downloads_dir, file_name)
    
    # Skriv filen
    utils::write.csv(df_combined, file = file_path, row.names = FALSE)
    message("Data saved as CSV in: ", file_path)
    
    # Returns path
    attr(df_combined, "saved_csv_path") <- file_path
  } else {
    message('Wish to save as CSV file, enter save_csv = "Yes" eller TRUE.')
  }
  
  return(df_combined)
}


df_all <- df_ferrybox(parameters = c("temperature", "salinity", "oxygen_sat", "chlorophyll", "turbidity", "fdom"), time_index = time_index,
                      save_csv = "no") 
# test with unavailable parameter
df_all <- df_ferrybox(parameters = c("salinity","Nitrogen"), time_index = time_index)


# close file
nc_close(fb_nc)

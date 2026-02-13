
# Join ferrybox and station measurements by time and parameter
library(dplyr)
library(lubridate)

as_posixct_safe <- function(x, tz = "UTC") {
  if (inherits(x, "POSIXct")) return(x)
  if (inherits(x, "Date"))   return(as.POSIXct(x, tz = tz))
  if (is.character(x)) {
    out <- parse_date_time(x, orders = c("Y-m-d HMS", "Y-m-d HM", "Y-m-d"), tz = tz)
    return(out)
  }
  stop("Unsupported time class: ", paste(class(x), collapse = "/"))
}

join_x_y <- function(df_x,
                     df_y,
                     parameter_x,
                     parameter_y,
                     station_col,
                     station_ID,
                     time_col_x,
                     time_col_y,
                     tz = "UTC") {
  
  stopifnot(is.data.frame(df_x), is.data.frame(df_y), is.character(parameter_x), is.character(parameter_y),
            is.character(time_col_x),is.character(time_col_y))
  
  # station_col check if null, na or empty
  if (is.null(station_col) || is.na(station_col) ||  !nzchar(station_col)) {
    stop(
      "Provide station column name \n",
      "Available columns: ", paste(names(df_y), collapse = ", ")
    )
  }
  
  # Proceed, and create station values with unique stations
  station_vals <- unique(df_y[[station_col]])
  
  if(is.null(station_ID) || is.na(station_ID) || !nzchar(station_ID)){
    stop(
      "Provide unique station ID \n",
      "Available station IDs: ", paste(station_vals, collapse = ", ")
    )
  }
  
  df_y <- df_y %>%filter(.data[[station_col]] == station_ID)

  # Lowercase all col
  station_col <- tolower(station_col)
  time_col_x  <- tolower(time_col_x)
  time_col_y  <- tolower(time_col_y)
  
  col_names_x <- tolower(names(df_x))
  col_names_y <- tolower(names(df_y))
  
  x_map <- setNames(names(df_x), tolower(names(df_x)))
  y_map <- setNames(names(df_y), tolower(names(df_y)))
  
  # Check time columns
  if (!(time_col_x %in% col_names_x) || !(time_col_y %in% col_names_y)) {
    stop(
      "Time column is missing from at least one dataframe.\n",
      "df_x columns: ", paste(col_names_x, collapse = ", "), "\n",
      "df_y columns: ", paste(col_names_y, collapse = ", ")
    )
  }
  
  # Check other columns
  req_x <- c("latitude", "longitude", time_col_x, "value", "parameter")
  req_y <- c(station_col, time_col_y, "value", "parameter")
  
  miss_x <- setdiff(req_x, col_names_x)
  miss_y <- setdiff(req_y, col_names_y)
  
  if (length(miss_x) > 0 || length(miss_y) > 0) {
    stop(
      "Missing columns:\n",
      "df_x: ", paste(miss_x, collapse = ", "), "\n",
      "df_y: ", paste(miss_y, collapse = ", ")
    )
  }
  
  # Create POSIXct time column
  df_x <- df_x %>%
    mutate(time = as_posixct_safe(.data[[ x_map[[time_col_x]] ]], tz = tz))
  
  df_y <- df_y %>%
    mutate(time = as_posixct_safe(.data[[ y_map[[time_col_y]] ]], tz = tz))
  
  if (any(is.na(df_x$time))) {
    stop("Could not parse time in df_x. Example: ", df_x[[ x_map[[time_col_x]] ]][1])
  }
  if (any(is.na(df_y$time))) {
    stop("Could not parse time in df_y. Example: ", df_y[[ y_map[[time_col_y]] ]][1])
  }
  
  # Summarise data
  data_x <- df_x %>%
    filter(parameter == parameter_x) %>%
    mutate(year = year(time), month = month(time), day = day(time)) %>%
    group_by(year, month, day, latitude, longitude, parameter) %>%
    summarise(value_x = mean(value, na.rm = TRUE), .groups = "drop") %>%
    rename("parameter_x" = "parameter")
  
  data_y <- df_y %>%
    filter(parameter == parameter_y,
           .data[[ y_map[[station_col]] ]] == station_ID) %>%
    mutate(year = year(time), month = month(time), day = day(time)) %>%
    group_by(year, month, day, .data[[ y_map[[station_col]] ]], parameter) %>%
    summarise(value_y = mean(value, na.rm = TRUE), .groups = "drop") %>%
    rename("parameter_y" = "parameter")
  
  # Join dataframes by day
  df_comb <- left_join(
    data_x,
    data_y,
    by = c("year", "month", "day")
  ) %>%
    filter(!is.na(value_x),
           !is.na(value_y))
  
  df_comb
}

# Args command for reading input
args <- commandArgs(trailingOnly = TRUE)
message("R Command line args: ", paste(args, collapse = " | "))

if (length(args) < 9) {
  stop("Provide path file for data_x, data_y, parameter_x, parameter_y, station_col, station_ID, time_col_x, time_col_y and save_path")
}

input_path_x  <- args[1] #csv input path
input_path_y  <- args[2] #csv input path
parameter_x <- args[3] # Charachter 
parameter_y <- args[4] # Charachter 
station_col <- args[5] # Charachter 
station_ID <- args[6] # Charachter 
time_col_x <- args[7] # Charachter 
time_col_y <- args[8] # Charachter 
save_path  <- args[9] #path to save, can inlude name of file, if not default name is used "joined.csv"


if (startsWith(input_path_x, 'http')) {
  message('Input CSV provided as URL')
} else {
  if (!file.exists(input_path_x)) stop("Input CSV not found: ", input_path_x)
}

message("Reading input CSV: ", input_path_x)

df_x <- readr::read_csv(input_path_x, show_col_types = FALSE)
df_y <- readr::read_csv(input_path_y, show_col_types = FALSE)




df_joined <- join_x_y(
  df_x = df_x,
  df_y = df_y,
  parameter_x = parameter_x,
  parameter_y = parameter_y ,
  station_col  = station_col,
  station_ID =  station_ID,
  time_col_x   = time_col_x,
  time_col_y   = time_col_y,
  tz = "UTC"
)




# If .csv name is pased in save_path using that as saving name else default "joined.csv" is used. 
if (grepl("\\.csv$", save_path, ignore.case = TRUE)) {
  file_path <- save_path
  dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
} else {
  dir.create(save_path, recursive = TRUE, showWarnings = FALSE)
  file_path <- file.path(save_path, "joined.csv")
}

message(paste0('Writing result to CSV file: ', file_path, ' (may take a little while if the data is big)...'))

utils::write.table(
  df_joined,
  file = file_path,
  sep = ",",
  row.names = FALSE,
  col.names = TRUE,
  quote = TRUE,
  append = FALSE
)

message(paste0('Writing result to CSV file... done'))

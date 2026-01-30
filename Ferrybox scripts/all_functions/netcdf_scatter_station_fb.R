
library(dplyr)
library(ggplot2)
library(lubridate)
library(sf)



`%||%` <- function(x, y) {
  if (is.null(x)) return(y)
  if (length(x) == 0) return(y)
  x
}
scatter_transect_vs_station <- function(
    data_x,
    data_y,
    parameter_x,
    parameter_y,
    
    # --- column mappings (non-expert friendly) ---
    x_cols = list(
      time = NULL,          # e.g. "datetime" OR "date" (optional if y/m/d provided)
      year = "year",
      month = "month",
      day = "day",
      lat = "latitude",
      lon = "longitude",
      value = "value",
      parameter = "parameter",
      unit = "unit"
    ),
    y_cols = list(
      time = NULL,          # e.g. "datetime" OR "date" (optional if y/m/d provided)
      year = "year",
      month = "month",
      day = "day",
      value = "value",
      parameter = "parameter",
      unit = "unit",
      station = "station"
    ),
    
    # --- date filters ---
    date_start = NULL,
    date_end   = NULL,
    tz = "UTC",
    
    # --- spatial filters (transect only) ---
    lat_range = NULL,            # c(min,max)
    waterbodies = NULL,          # sf polygons
    waterbody_ids = NULL,        # vector
    waterbody_id_col = NULL,     # column name in waterbodies
    
    # --- temporal harmonisation ---
    time_unit = "day",           # "minute","hour","day"
    agg_fun = mean,
    
    # --- optional station filter ---
    station_name = NULL,
    
    # --- plot ---
    add_lm = TRUE
) {
  # ---- deps ----
  stopifnot(is.data.frame(data_x), is.data.frame(data_y))
  
  if(!is.null(lat_range) & !is.null(waterbody_ids)) {
    stop("ERROR: Provide either latitudal filtration condition or waterbody ID to aggregate across")
  }
  if(is.null(lat_range) & is.null(waterbody_ids)){
    stop("ERROR: Please provide spatial filtration condition as Lat_range or waterbody ID")
  }

  # ---- helper: safe column getter ----
  get_col <- function(df, col) {
    if (is.null(col)) return(NULL)
    if (!col %in% names(df)) stop("Missing column: '", col, "'. Available: ", paste(names(df), collapse = ", "))
    df[[col]]
  }
  
  # ---- helper: build datetime from time OR y/m/d ----
  standardise_time <- function(df, cols, dataset_name = "data") {
    nms <- names(df)
    
    # allow time to be specified; if not, try common names
    time_col <- cols$time
    if (is.null(time_col)) {
      if ("datetime" %in% nms) time_col <- "datetime"
      else if ("date" %in% nms) time_col <- "date"
    }
    
    has_time <- !is.null(time_col) && time_col %in% nms
    has_ymd  <- all(c(cols$year, cols$month, cols$day) %in% nms)
    
    if (!has_time && !has_ymd) {
      stop(dataset_name, " must contain either a time column (date/datetime) OR year+month+day.")
    }
    
    if (has_time) {
      x <- df[[time_col]]
      
      # accept Date/POSIXct/character
      if (inherits(x, "POSIXct")) {
        dt <- x
      } else if (inherits(x, "Date")) {
        dt <- as.POSIXct(x, tz = tz)
      } else {
        # try parse (strict-ish)
        dt <- suppressWarnings(as.POSIXct(x, tz = tz))
      }
      
      if (all(is.na(dt))) {
        stop(dataset_name, ": could not parse time column '", time_col, "'. ",
             "Provide ISO format (e.g. 2023-01-31 13:10) or supply year/month/day.")
      }
      
      df$datetime <- dt
      return(df)
    }
    
    # y/m/d path
    yr <- get_col(df, cols$year)
    mo <- get_col(df, cols$month)
    da <- get_col(df, cols$day)
    
    dt_str <- sprintf("%04d-%02d-%02d", as.integer(yr), as.integer(mo), as.integer(da))
    dt <- as.POSIXct(dt_str, format = "%Y-%m-%d", tz = tz)
    
    if (any(is.na(dt))) {
      bad <- unique(dt_str[is.na(dt)])
      stop(dataset_name, ": invalid year/month/day values. Example bad dates: ",
           paste(head(bad, 5), collapse = ", "))
    }
    
    df$datetime <- dt
    df
  }
  
  # ---- helper: validate required columns for x/y ----
  assert_cols <- function(df, cols, required, dataset_name) {
    missing <- setdiff(required, names(df))
    if (length(missing) > 0) {
      stop(dataset_name, " missing required columns: ", paste(missing, collapse = ", "),
           ". Available: ", paste(names(df), collapse = ", "))
    }
  }
  
  # ---- standardise time + validate ----
  data_x <- standardise_time(data_x, x_cols, "data_x")
  data_y <- standardise_time(data_y, y_cols, "data_y")
  
  # require mapping cols
  assert_cols(data_x, x_cols, c(x_cols$lat, x_cols$lon, x_cols$value, x_cols$parameter), "data_x")
  assert_cols(data_y, y_cols, c(y_cols$value, y_cols$parameter), "data_y")
  if (!is.null(station_name)) assert_cols(data_y, y_cols, c(y_cols$station), "data_y")
  
  # ---- rename into internal standard names ----
  x <- data_x %>%
    transmute(
      datetime = datetime,
      latitude = .data[[x_cols$lat]],
      longitude = .data[[x_cols$lon]],
      value = .data[[x_cols$value]],
      parameter = .data[[x_cols$parameter]],
      unit = if (!is.null(x_cols$unit) && x_cols$unit %in% names(data_x)) .data[[x_cols$unit]] else NA_character_
    )
  
  y <- data_y %>%
    transmute(
      datetime = datetime,
      value = .data[[y_cols$value]],
      parameter = .data[[y_cols$parameter]],
      unit = if (!is.null(y_cols$unit) && y_cols$unit %in% names(data_y)) .data[[y_cols$unit]] else NA_character_,
      station = if (!is.null(y_cols$station) && y_cols$station %in% names(data_y)) .data[[y_cols$station]] else NA_character_
    )
  
  # ---- filter by parameters ----
  x <- x %>% filter(parameter == parameter_x)
  y <- y %>% filter(parameter == parameter_y)
  
  if (nrow(x) == 0) stop("No rows in data_x after filtering parameter_x = '", parameter_x, "'.")
  if (nrow(y) == 0) stop("No rows in data_y after filtering parameter_y = '", parameter_y, "'.")
  
  # ---- optional station filter ----
  if (!is.null(station_name)) {
    if (!"station" %in% names(y)) stop("station_name provided but station column missing in data_y mapping.")
    y <- y %>% filter(station == station_name)
    if (nrow(y) == 0) stop("No rows left in data_y after filtering station = '", station_name, "'.")
  }
  
  # ---- date filters ----
  if (!is.null(date_start)) {
    ds <- as.POSIXct(date_start, tz = tz)
    if (is.na(ds)) stop("date_start could not be parsed.")
    x <- x %>% filter(datetime >= ds)
    y <- y %>% filter(datetime >= ds)
  }
  if (!is.null(date_end)) {
    de <- as.POSIXct(date_end, tz = tz)
    if (is.na(de)) stop("date_end could not be parsed.")
    x <- x %>% filter(datetime <= de)
    y <- y %>% filter(datetime <= de)
  }
  
  # ---- spatial filter: latitude window ----
  if (!is.null(lat_range)) {
    if (length(lat_range) != 2) stop("lat_range must be length 2, e.g. c(58.9, 59.1)")
    x <- x %>% filter(latitude >= min(lat_range), latitude <= max(lat_range))
    if (nrow(x) == 0) stop("No rows left in data_x after lat_range filter.")
  }
  
  # ---- spatial filter: waterbody intersection ----
  if (!is.null(waterbodies) || !is.null(waterbody_ids) || !is.null(waterbody_id_col)) {
    if (is.null(waterbodies) || is.null(waterbody_ids) || is.null(waterbody_id_col)) {
      stop("To filter by waterbodies you must provide waterbodies, waterbody_ids, and waterbody_id_col.")
    }
    if (!inherits(waterbodies, "sf")) stop("waterbodies must be an sf object.")
    if (!waterbody_id_col %in% names(waterbodies)) stop("waterbody_id_col not found in waterbodies.")
    
    x_sf <- st_as_sf(x, coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)
    x_joined <- st_join(x_sf, waterbodies[, waterbody_id_col, drop = FALSE], left = FALSE)
    
    x <- x_joined %>%
      st_drop_geometry() %>%
      filter(.data[[waterbody_id_col]] %in% waterbody_ids)
    
    if (nrow(x) == 0) stop("No rows left in data_x after waterbody filter.")
  }
  
  # ---- harmonise time + aggregate ----
  x_agg <- x %>%
    mutate(time_key = floor_date(datetime, unit = time_unit)) %>%
    group_by(time_key) %>%
    summarise(
      x_value = agg_fun(value, na.rm = TRUE),
      x_n = sum(!is.na(value)),
      .groups = "drop"
    )
  
  y_agg <- y %>%
    mutate(time_key = floor_date(datetime, unit = time_unit)) %>%
    group_by(time_key) %>%
    summarise(
      y_value = agg_fun(value, na.rm = TRUE),
      y_n = sum(!is.na(value)),
      .groups = "drop"
    )
  
  joined <- inner_join(x_agg, y_agg, by = "time_key") %>%
    filter(is.finite(x_value), is.finite(y_value))
  
  if (nrow(joined) < 10) stop("Too few joined points after filtering/joining (n = ", nrow(joined), ").")
  
  # ---- stats + plot ----
  cor_val <- suppressWarnings(cor(joined$x_value, joined$y_value, use = "complete.obs"))
  
  #Plot waterbody ID if given else plot with transect
  if(is.null(lat_range)) {
    p <- ggplot(joined, aes(x = x_value, y = y_value)) +
      geom_point() +
      labs(
        x = paste0(waterbody_ids," data for ", parameter_x),
        y = paste0(station_name," data for ", parameter_y),
        title = "Water body vs Station scatter (joined by time)"
      )
    
    if (add_lm) p <- p + geom_smooth(method = "lm", se = TRUE)
  } else{   
    p <- ggplot(joined, aes(x = x_value, y = y_value)) +
      geom_point() +
      labs(
        x = paste0("Transect ", paste(lat_range, collapse = "-"),"N° data for ", parameter_x),
        y = paste0(station_name," data for ", parameter_y),
        title = "Transect vs Station scatter (joined by time)"
      )
    
    if (add_lm) p <- p + geom_smooth(method = "lm", se = TRUE)
  }
  
 
  list(
    data = joined,
    plot = p,
    stats = list(n = nrow(joined), cor = cor_val)
  )
}


args <- commandArgs(trailingOnly = TRUE)
message("R Command line args: ", paste(args, collapse = " | "))

if (length(args) < 2) {
  stop("Provide input path to csv and output path.")
}
input_fb_file  <- args[1] # csv file with fb data
input_river_file   <- args[2] #csv file with river names and location
parameter_x <- args[3] # parameter to plot on x-axis
parameter_y <- args[4] # parameter to plot on y-axis

save_png_path <- args[5] # output folder path


date_start <- if(length(args)>= 6) as_null_if_blank(args[6]) else NULL
date_end <- if(length(args)>= 7) as_null_if_blank(args[7]) else NULL

lat_range <- if(length(args)>= 8) as_null_if_blank(args[8]) else NULL

station_name <- if(length(args)>= 9) as_null_if_blank(args[9]) else NULL






res <- scatter_transect_vs_station(
  data_x = FB2023,
  data_y = logger2023,
  parameter_x = "temperature",
  parameter_y = "temp_water_avg",
  lat_range = c(58.9, 59.1),   # Filter by lat
  #waterbodies = vanntype,          # sf polygons
  #waterbody_ids = "Nordre og Søndre Søster",        # vector
  #waterbody_id_col = "Vannfore_1",     # column name in waterbodies
  time_unit = "day",
  station_name = "Baterod",
  y_cols = list(time = "datetime", value = "value", parameter = "parameter", station = "station_name")
)

res$plot



res <- scatter_transect_vs_station(
  data_x = FB2023,
  data_y = logger2023,
  parameter_x = "temperature",
  parameter_y = "temp_water_avg",
  x_cols = list(year="year", month="month", day="day", lat="latitude", lon="longitude", value="value", parameter="parameter"),
  y_cols = list(time="datetime", value="value", parameter="parameter", station="station_name"),
  lat_range = c(58.9, 59.1)
)

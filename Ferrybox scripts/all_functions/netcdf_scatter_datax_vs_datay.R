
library(dplyr)
library(ggplot2)
library(lubridate)
library(sf)
library(readr)

as_null_if_blank <- function(x) {
  if (is.null(x)) return(NULL)
  x <- trimws(x)
  if (!nzchar(x) || tolower(x) == "null") NULL else x
}

`%||%` <- function(x, y) {
  if (is.null(x)) return(y)
  if (length(x) == 0) return(y)
  x
}

scatter_from_joined <- function(
    df_joined,
    waterbodies = NULL,
    waterbody_id_col = NULL,
    waterbody_ids = NULL,
    lat_range = NULL,
    tz = "UTC",
    agg_fun = mean,
    add_lm = TRUE
) {
  stopifnot(is.data.frame(df_joined))
  
  req <- c("year", "month", "day", "value_x", "value_y")
  miss <- setdiff(req, names(df_joined))
  if (length(miss) > 0) {
    stop("df_joined is missing: ", paste(miss, collapse = ", "))
  }
  
  has_coords <- all(c("latitude", "longitude") %in% names(df_joined))
  
  using_waterbodies <- !is.null(waterbodies) || !is.null(waterbody_ids) || !is.null(waterbody_id_col)
  
  if (using_waterbodies) {
    if (is.null(waterbodies) || is.null(waterbody_ids) || is.null(waterbody_id_col)) {
      stop("If using waterbodies you must provide: waterbodies, waterbody_ids, waterbody_id_col.")
    }
    if (!inherits(waterbodies, "sf")) {
      stop("waterbodies must be an sf object.")
    }
    if (!(waterbody_id_col %in% names(waterbodies))) {
      stop("waterbody_id_col not found in waterbodies.")
    }
    if (!has_coords) {
      stop("To filter by waterbodies, df_joined must have latitude and longitude.")
    }
  } else {
    if (is.null(lat_range)) {
      stop("Provide lat_range (e.g. c(59.0, 59.3)) when waterbodies is not provided.")
    }
    if (length(lat_range) != 2) {
      stop("lat_range must be length 2.")
    }
    if (!has_coords) {
      stop("To filter by lat_range, df_joined must have latitude and longitude.")
    }
  }
  
  df <- df_joined %>%
    mutate(
      date = as.Date(sprintf("%04d-%02d-%02d", year, month, day)),
      date = as.POSIXct(date, tz = tz)
    )
  
  if (!using_waterbodies) {
    df <- df %>%
      filter(.data$latitude >= min(lat_range), .data$latitude <= max(lat_range))
    if (nrow(df) == 0) stop("No rows left after lat_range filtering.")
  } else {
    pts <- sf::st_as_sf(df, coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)
    keep <- sf::st_join(pts, waterbodies[, waterbody_id_col, drop = FALSE], left = FALSE)
    df <- keep %>%
      sf::st_drop_geometry() %>%
      filter(.data[[waterbody_id_col]] %in% waterbody_ids)
    if (nrow(df) == 0) stop("No rows left after waterbody filtering.")
  }
  
  daily <- df %>%
    group_by(.data$date) %>%
    summarise(
      x_value = agg_fun(.data$value_x, na.rm = TRUE),
      y_value = agg_fun(.data$value_y, na.rm = TRUE),
      n_pairs = sum(is.finite(.data$value_x) & is.finite(.data$value_y)),
      .groups = "drop"
    ) %>%
    filter(is.finite(.data$x_value), is.finite(.data$y_value))
  
  if (nrow(daily) < 3) {
    stop("Too few points after filtering/aggregation (n = ", nrow(daily), ").")
  }
  
  px <- if ("parameter_x" %in% names(df_joined)) unique(na.omit(df_joined$parameter_x)) else "x"
  py <- if ("parameter_y" %in% names(df_joined)) unique(na.omit(df_joined$parameter_y)) else "y"
  px <- if (length(px) == 1) px else "x"
  py <- if (length(py) == 1) py else "y"
  
  cor_val <- suppressWarnings(stats::cor(daily$x_value, daily$y_value, use = "complete.obs"))
  
  title_txt <- if (using_waterbodies) {
    paste0("Waterbody vs station (", paste(waterbody_ids, collapse = ", "), ")")
  } else {
    paste0("Transect vs station (lat ", paste(range(lat_range), collapse = "–"), ")")
  }
  
  p <- ggplot(daily, aes(x = x_value, y = y_value)) +
    geom_point() +
    labs(
      x = paste0(px, " (value_x)"),
      y = paste0(py, " (value_y)"),
      title = title_txt,
      subtitle = paste0("n=", nrow(daily), ", cor=", round(cor_val, 3))
    )
  
  if (add_lm) {
    p <- p + geom_smooth(method = "lm", se = TRUE)
  }
  
  list(
    data = daily,
    plot = p,
    stats = list(n = nrow(daily), cor = cor_val)
  )
}

resolve_spatial_input_path <- function(input_path) {
  if (is.null(input_path)) return(NULL)
  
  if (!(startsWith(input_path, "http") || file.exists(input_path))) {
    stop("Spatial input must be NULL, a valid file path, or a valid URL.")
  }
  
  resolved_path <- input_path
  
  if (startsWith(input_path, "http") && endsWith(tolower(input_path), "zip")) {
    message("DEBUG: Downloading ZIP: ", input_path)
    
    temp_zip <- tempfile(fileext = ".zip")
    extract_dir <- tempfile()
    dir.create(extract_dir)
    
    download.file(input_path, temp_zip, mode = "wb")
    unzip(temp_zip, exdir = extract_dir)
    
    files <- list.files(extract_dir, recursive = TRUE, full.names = TRUE)
    shp_files <- files[grepl("\\.shp$", files, ignore.case = TRUE)]
    geojson_files <- files[grepl("\\.(geojson|json)$", files, ignore.case = TRUE)]
    spatial_files <- c(shp_files, geojson_files)
    
    message("DEBUG: Extracted files:")
    print(list.files(extract_dir, recursive = TRUE))
    
    if (length(spatial_files) == 0) {
      stop("No .shp or .geojson/.json file found in ZIP.")
    }
    
    if (length(spatial_files) > 1) {
      stop(
        "ZIP contains multiple spatial files. This script requires exactly one spatial file inside the ZIP.\n",
        "Available files: ", paste(basename(spatial_files), collapse = ", ")
      )
    }
    
    resolved_path <- spatial_files[1]
    message("DEBUG: Selected spatial file: ", resolved_path)
    
  } else if (startsWith(input_path, "http") && endsWith(tolower(input_path), "shp")) {
    stop("Remote shapefile must be provided as ZIP.")
  }
  
  resolved_path
}


read_study_area <- function(path_to_study_area, layer_input) {
  lyr_info <- sf::st_layers(path_to_study_area)
  available_layers <- paste(lyr_info$name, collapse = ", ")
  
  if (is.null(layer_input)) {
    stop(paste0(
      "input_study_area was provided, so study_area_layer is required. ",
      "Available layers: ", available_layers
    ))
  }
  
  if (!(layer_input %in% lyr_info$name)) {
    stop(paste0(
      "Input layer name does not exist.",
      " Requested layer: ", layer_input, ".",
      " Available layers: ", available_layers
    ))
  }
  
  shp <- sf::st_read(path_to_study_area, layer = layer_input, quiet = TRUE)
  
  # Repair invalid geometries from source data (e.g. duplicate vertices)
  sf::sf_use_s2(FALSE)
  shp <- sf::st_make_valid(shp)
  sf::sf_use_s2(TRUE)
  
  shp
}


# -------------------------------------------------------------------
# CLI args
# -------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
message("R Command line args: ", paste(args, collapse = " | "))

if (length(args) < 2) {
  stop("Provide source (URL/CSV) and path to save.")
}

joined_df_input_path          <- args[1]
save_path           <- args[2]
waterbodies_path    <- if (length(args) >= 3) as_null_if_blank(args[3]) else NULL
waterbody_ids       <- if (length(args) >= 4) as_null_if_blank(args[4]) else NULL
waterbody_id_col    <- if (length(args) >= 5) as_null_if_blank(args[5]) else NULL
lat_range_min       <- if (length(args) >= 6) as_null_if_blank(args[6]) else NULL
lat_range_max       <- if (length(args) >= 7) as_null_if_blank(args[7]) else NULL
study_area_layer    <- if (length(args) >= 8) as_null_if_blank(args[8]) else NULL

# Numbers are passed as strings to this script from python/docker. And if they are
# passed as numbers, the function "as_null_if_blank()" converts them to characters,
# so we convert (back) to numeric:
if (!is.null(lat_range_min)) {
  lat_range_min <- as.numeric(lat_range_min)
}
if (!is.null(lat_range_max)) {
  lat_range_max <- as.numeric(lat_range_max)
}

# Check file existance (unless passed as URL):
if (startsWith(joined_df_input_path, "http")) {
  message("Input CSV provided as URL.")
} else if (!file.exists(joined_df_input_path)) {
  stop("Input CSV not found: ", joined_df_input_path)
}

# Make a vector from latitude range
lat_range <- if (!is.null(lat_range_min) && !is.null(lat_range_max)) {
  c(lat_range_min, lat_range_max)
} else {
  NULL
}


message("Reading input CSV: ", joined_df_input_path)
df_joined <- readr::read_csv(joined_df_input_path, show_col_types = FALSE)

if (!is.null(waterbody_ids) && length(waterbody_ids) == 1) {
  waterbody_ids <- strsplit(waterbody_ids, ",")[[1]]
  waterbody_ids <- trimws(waterbody_ids)
}
# -------------------------------------------------------------------
# Read waterbodies (optional)
# -------------------------------------------------------------------

waterbody_shp <- NULL
if (!is.null(waterbodies_path)) {
  input_path <- resolve_spatial_input_path(waterbodies_path)  # was: resolve_study_area_path
  
  message("DEBUG: Reading spatial data: ", input_path)
  
  waterbody_shp <- read_study_area(
    path_to_study_area = input_path,
    layer_input = study_area_layer
  )
  
  message("DEBUG: st_read resulted in class: ", paste(class(waterbody_shp), collapse = ", "))
}


# -------------------------------------------------------------------
# Run analysis
# -------------------------------------------------------------------
scatter_fb_stat <- scatter_from_joined(
  df_joined = df_joined,
  waterbodies = waterbody_shp,
  waterbody_id_col = waterbody_id_col,
  waterbody_ids = waterbody_ids,
  lat_range = lat_range,
  tz = "UTC",
  agg_fun = mean,
  add_lm = TRUE
)

# Show plot in interactive sessions
if (interactive()) {
  print(scatter_fb_stat$plot)
}

# -------------------------------------------------------------------
# Save PNG
# -------------------------------------------------------------------
if (grepl("\\.png$", save_path, ignore.case = TRUE)) {
  file_path <- save_path
  dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
} else {
  dir.create(save_path, recursive = TRUE, showWarnings = FALSE)
  file_path <- file.path(save_path, "scatter.png")
}

message("Saving PNG to: ", file_path)
ggsave(
  filename = file_path,
  plot = scatter_fb_stat$plot,
  width = 18,
  height = 22,
  units = "cm",
  dpi = 300,
  bg = "white"
)

message("Saving PNG... done")

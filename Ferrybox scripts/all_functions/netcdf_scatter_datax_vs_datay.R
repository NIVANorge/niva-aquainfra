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

# Pick the single unique non-NA value of a column, or a fallback if the column
# is missing / not unique. Used to derive parameter names and units for labels.
pick_unique <- function(df, col, fallback = NA_character_) {
  if (!col %in% names(df)) return(fallback)
  vals <- unique(stats::na.omit(df[[col]]))
  if (length(vals) == 1) as.character(vals) else fallback
}

# Build an axis label like "Temperature (°C)" — falls back gracefully when the
# parameter name or unit is unavailable.
axis_label <- function(param, unit, default_param) {
  p <- if (!is.na(param) && nzchar(param)) param else default_param
  if (!is.na(unit) && nzchar(unit)) paste0(p, " (", unit, ")") else p
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
  
  # ------------------------------------------------------------------
  # Decide method robustly.
  # Waterbody filtering requires ALL THREE of: waterbodies, ids, id_col.
  # If any is missing we do NOT treat it as "using waterbodies" — we fall
  # back to latitude range. This makes "NULL"/blank inputs behave sensibly
  # regardless of which fields the user leaves empty.
  # ------------------------------------------------------------------
  using_waterbodies <- !is.null(waterbodies) &&
    !is.null(waterbody_ids) &&
    !is.null(waterbody_id_col)
  
  # If the user supplied SOME but not all waterbody inputs, be explicit rather
  # than silently switching method.
  partial_wb <- (!is.null(waterbodies) || !is.null(waterbody_ids) || !is.null(waterbody_id_col)) &&
    !using_waterbodies
  if (partial_wb) {
    stop(
      "Incomplete waterbody inputs. To filter by waterbodies you must provide ALL of: ",
      "waterbody file, waterbody IDs, and waterbody ID column. ",
      "To filter by latitude instead, leave all three empty (or NULL) and provide latitude_min/max."
    )
  }
  
  if (using_waterbodies) {
    if (!inherits(waterbodies, "sf")) {
      stop("waterbodies must be an sf object.")
    }
    if (!(waterbody_id_col %in% names(waterbodies))) {
      stop("waterbody_id_col '", waterbody_id_col, "' not found in waterbodies. ",
           "Available columns: ", paste(names(waterbodies), collapse = ", "))
    }
    if (!has_coords) {
      stop("To filter by waterbodies, df_joined must have latitude and longitude.")
    }
  } else {
    if (is.null(lat_range)) {
      stop("No filtering method provided. Either supply waterbody inputs, ",
           "or provide latitude_min and latitude_max.")
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
  
  # ------------------------------------------------------------------
  # Derive parameter names and units for axis labels.
  # Units are optional: if unit_x / unit_y columns exist they are used,
  # otherwise the axis shows just the parameter name.
  # ------------------------------------------------------------------
  px   <- pick_unique(df_joined, "parameter_x", "value_x")
  py   <- pick_unique(df_joined, "parameter_y", "value_y")
  ux   <- pick_unique(df_joined, "unit_x", NA_character_)
  uy   <- pick_unique(df_joined, "unit_y", NA_character_)
  
  x_lab <- axis_label(px, ux, "value_x")
  y_lab <- axis_label(py, uy, "value_y")
  
  # ------------------------------------------------------------------
  # Regression statistics for annotation.
  # ------------------------------------------------------------------
  cor_val <- suppressWarnings(stats::cor(daily$x_value, daily$y_value, use = "complete.obs"))
  
  fit      <- stats::lm(y_value ~ x_value, data = daily)
  coefs    <- stats::coef(fit)
  intercept <- coefs[[1]]
  slope     <- coefs[[2]]
  r2        <- summary(fit)$r.squared
  
  eq_txt <- sprintf(
    "y = %.3g \u00b7 x %s %.3g",
    slope,
    ifelse(intercept >= 0, "+", "\u2212"),
    abs(intercept)
  )
  stats_txt <- sprintf("%s   |   R\u00b2 = %.3f   |   r = %.3f   |   n = %d",
                       eq_txt, r2, cor_val, nrow(daily))
  
  # Method description for subtitle.
  method_txt <- if (using_waterbodies) {
    paste0("Aggregated over waterbodies: ", paste(waterbody_ids, collapse = ", "))
  } else {
    paste0("Aggregated over latitude ", sprintf("%.2f", min(lat_range)),
           "\u2013", sprintf("%.2f", max(lat_range)), " \u00b0N")
  }
  
  date_span <- paste0(format(min(df$date), "%Y-%m-%d"), " to ",
                      format(max(df$date), "%Y-%m-%d"))
  
  # ------------------------------------------------------------------
  # Build plot.
  # ------------------------------------------------------------------
  p <- ggplot(daily, aes(x = x_value, y = y_value)) +
    { if (add_lm) geom_smooth(method = "lm", formula = y ~ x, se = TRUE,
                              color = "#2c7fb8", fill = "#a6bddb", alpha = 0.3,
                              linewidth = 0.9) } +
    geom_point(aes(size = n_pairs), color = "#08306b", alpha = 0.65) +
    scale_size_continuous(name = "Ferrybox obs. \nper day", range = c(1.5, 5)) +
    labs(
      title    = paste0("Daily avg. FerryBox vs logger: ", px, " vs ", py),
      subtitle = paste0(method_txt, "\nPeriod: ", date_span),
      x        = x_lab,
      y        = y_lab,
      caption  = stats_txt
    ) +
    theme_bw(base_size = 13) +
    theme(
      plot.title      = element_text(face = "bold", size = 16),
      plot.subtitle   = element_text(size = 11, color = "grey30"),
      plot.caption    = element_text(size = 11, hjust = 0, color = "grey20",
                                     margin = margin(t = 10)),
      axis.title      = element_text(face = "bold"),
      axis.text       = element_text(color = "grey20"),
      panel.grid.minor = element_blank(),
      legend.position = "bottom",
      plot.margin     = margin(15, 15, 15, 15)
    )
  
  list(
    data  = daily,
    plot  = p,
    stats = list(n = nrow(daily), cor = cor_val, r2 = r2,
                 slope = slope, intercept = intercept)
  )
}

resolve_spatial_input_path <- function(input_path) {
  if (is.null(input_path)) return(NULL)
  
  is_url <- startsWith(input_path, "http")
  if (!(is_url || file.exists(input_path))) {
    stop("Spatial input must be NULL, a valid file path, or a valid URL.")
  }
  
  local_file <- input_path
  if (is_url) {
    local_file <- tempfile()
    download.file(input_path, local_file, mode = "wb")
  }
  
  # Determine type from CONTENT, not extension (Galaxy gives .dat names)
  con <- file(local_file, "rb")
  magic <- readBin(con, "raw", n = 4)
  close(con)
  is_zip <- length(magic) >= 2 &&
    magic[1] == as.raw(0x50) && magic[2] == as.raw(0x4B)  # "PK"
  
  if (is_zip) {
    extract_dir <- tempfile(); dir.create(extract_dir)
    unzip(local_file, exdir = extract_dir)
    files <- list.files(extract_dir, recursive = TRUE, full.names = TRUE)
    spatial_files <- files[grepl("\\.(shp|geojson|json)$", files, ignore.case = TRUE)]
    if (length(spatial_files) == 0) stop("No .shp or .geojson/.json file found in ZIP.")
    if (length(spatial_files) > 1) stop("ZIP contains multiple spatial files: ",
                                        paste(basename(spatial_files), collapse = ", "))
    return(spatial_files[1])
  }
  
  # Otherwise: geojson/json content (also when file is named .dat)
  first <- readLines(local_file, n = 50, warn = FALSE)
  if (any(grepl("FeatureCollection|\"type\"\\s*:", first))) {
    geojson_path <- tempfile(fileext = ".geojson")
    file.copy(local_file, geojson_path, overwrite = TRUE)
    return(geojson_path)
  }
  
  local_file
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

joined_df_input_path <- args[1]
save_path            <- args[2]
waterbodies_path     <- if (length(args) >= 3) as_null_if_blank(args[3]) else NULL
waterbody_ids        <- if (length(args) >= 4) as_null_if_blank(args[4]) else NULL
waterbody_id_col     <- if (length(args) >= 5) as_null_if_blank(args[5]) else NULL
lat_range_min        <- if (length(args) >= 6) as_null_if_blank(args[6]) else NULL
lat_range_max        <- if (length(args) >= 7) as_null_if_blank(args[7]) else NULL
study_area_layer     <- if (length(args) >= 8) as_null_if_blank(args[8]) else NULL

# Numbers are passed as strings from python/docker; convert (back) to numeric.
if (!is.null(lat_range_min)) lat_range_min <- as.numeric(lat_range_min)
if (!is.null(lat_range_max)) lat_range_max <- as.numeric(lat_range_max)

# Check file existence (unless passed as URL):
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

# Split comma-separated waterbody ids into a vector (handles the string that
# python/docker passes). If the field was NULL/blank this stays NULL.
if (!is.null(waterbody_ids) && length(waterbody_ids) == 1) {
  waterbody_ids <- strsplit(waterbody_ids, ",")[[1]]
  waterbody_ids <- trimws(waterbody_ids)
  waterbody_ids <- waterbody_ids[nzchar(waterbody_ids)]        # drop empty pieces
  if (length(waterbody_ids) == 0) waterbody_ids <- NULL        # all-empty -> NULL
}

# -------------------------------------------------------------------
# Read waterbodies (optional)
# -------------------------------------------------------------------
waterbody_shp <- NULL
if (!is.null(waterbodies_path)) {
  input_path <- resolve_spatial_input_path(waterbodies_path)
  
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
  df_joined        = df_joined,
  waterbodies      = waterbody_shp,
  waterbody_id_col = waterbody_id_col,
  waterbody_ids    = waterbody_ids,
  lat_range        = lat_range,
  tz               = "UTC",
  agg_fun          = mean,
  add_lm           = TRUE
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
  plot   = scatter_fb_stat$plot,
  width  = 22,
  height = 18,
  units  = "cm",
  dpi    = 300,
  bg     = "white"
)

message("Saving PNG... done")

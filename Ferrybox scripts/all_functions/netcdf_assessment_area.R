library(tidyverse)
library(sf)
library(readr)
library(rnaturalearth)
library(ggplot2)
library(rnaturalearthdata)
library(ggspatial)

`%||%` <- function(x, y) if (is.null(x) || all(is.na(x))) y else x

as_null_if_blank <- function(x) {
  if (is.null(x)) return(NULL)
  if (!is.atomic(x) && !is.character(x)) return(x)
  if (length(x) == 0) return(NULL)
  if (length(x) == 1 && is.na(x)) return(NULL)
  if (is.character(x)) {
    x <- trimws(x)
    if (!nzchar(x) || tolower(x) == "null") return(NULL)
  }
  x
}

standardise_lonlat <- function(df) {
  stopifnot(is.data.frame(df))
  
  nms <- names(df)
  nms_low <- tolower(nms)
  
  lon_hits <- nms[nms_low %in% c("lon", "long", "longitude", "x")]
  lat_hits <- nms[nms_low %in% c("lat", "latitude", "y")]
  
  if (length(lon_hits) == 0 || length(lat_hits) == 0) {
    stop(
      "Valid header names for coordinates: x/y, lon/lat, long/lat, longitude/latitude. Found: ",
      paste(names(df), collapse = ", ")
    )
  }
  
  pick <- function(hits, pref) {
    for (p in pref) {
      if (p %in% tolower(hits)) return(hits[tolower(hits) == p][1])
    }
    hits[1]
  }
  
  lon_col <- pick(lon_hits, c("longitude", "lon", "long", "x"))
  lat_col <- pick(lat_hits, c("latitude", "lat", "y"))
  
  df %>%
    rename(
      longitude = all_of(lon_col),
      latitude  = all_of(lat_col)
    )
}

assessment_plot <- function(
  river_df,
  study_area,
  data_fb,
  world_sf,
  river_label_col
) {
  stopifnot(!is.null(data_fb), !is.null(world_sf))
  
  fb_clean <- standardise_lonlat(data_fb) %>%
    filter(is.finite(longitude), is.finite(latitude)) %>%
    distinct(longitude, latitude, .keep_all = TRUE)
  
  if (nrow(fb_clean) == 0) {
    stop("No FerryBox rows left after filtering coordinates.")
  }
  
  fb_sf_pts <- st_as_sf(
    fb_clean,
    coords = c("longitude", "latitude"),
    crs = 4326,
    remove = FALSE
  )
  
  river_sf <- NULL
  if (!is.null(river_df)) {
    river_clean <- standardise_lonlat(river_df) %>%
      filter(is.finite(longitude), is.finite(latitude)) %>%
      distinct(longitude, latitude, .keep_all = TRUE)
    
    if (nrow(river_clean) == 0) {
      warning("river_df provided, but no valid coordinate rows after cleaning.")
    } else {
      river_sf <- st_as_sf(
        river_clean,
        coords = c("longitude", "latitude"),
        crs = 4326,
        remove = FALSE
      )
      
      if (!is.null(river_label_col) && !river_label_col %in% names(river_sf)) {
        if ("station_name" %in% names(river_sf)) {
          river_label_col <- "station_name"
        }
      }
    }
  }
  
  water_sf <- NULL
  if (!is.null(study_area)) {
    if (!inherits(study_area, "sf")) stop("study_area must be an sf object.")
    water_sf <- study_area
  }
  
  bbox_all <- sf::st_bbox(fb_sf_pts)
  
  ggplot() +
    geom_sf(data = world_sf, fill = "grey92", color = "grey60") +
    { if (!is.null(water_sf)) geom_sf(data = water_sf, fill = "lightblue", alpha = 0.6) } +
    geom_sf(data = fb_sf_pts, aes(color = "FerryBox track"), linewidth = 1.2, alpha = 0.4) +
    { if (!is.null(river_sf)) geom_sf(data = river_sf, aes(color = "River outlet"), size = 4) } +
    {
      if (!is.null(river_sf) &&
          !is.null(river_label_col) &&
          river_label_col %in% names(river_sf)) {
        geom_sf_text(
          data = river_sf,
          aes(label = .data[[river_label_col]], geometry = geometry),
          stat = "sf_coordinates",
          size = 5,
          nudge_y = 0.1
        )
      }
    } +
    ggspatial::annotation_scale(
      location = "bl",
      width_hint = 0.25,
      text_cex = 1.2,
      line_width = 0.8
    ) +
    ggspatial::annotation_north_arrow(
      location = "bl",
      which_north = "true",
      style = ggspatial::north_arrow_fancy_orienteering(text_size = 10),
      pad_y = grid::unit(1, "cm")
    ) +
    coord_sf(
      xlim = c(bbox_all["xmin"] - 1, bbox_all["xmax"] + 1),
      ylim = c(bbox_all["ymin"] - 0.5, bbox_all["ymax"] + 0.5),
      expand = FALSE
    ) +
    labs(
      title = "Assessment area",
      color = NULL
    ) +
    theme_minimal() +
    theme(
      panel.grid = element_blank(),
      legend.position = "bottom",
      axis.text.x = element_text(size = 16, angle = 60, face = "bold"),
      axis.text.y = element_text(size = 16, face = "bold"),
      legend.text = element_text(size = 16, face = "bold"),
      plot.title = element_text(size = 20, face = "bold")
    )
}

resolve_study_area_path <- function(input_study_area) {
  if (is.null(input_study_area)) return(NULL)
  
  if (!(startsWith(input_study_area, "http") || file.exists(input_study_area))) {
    stop("input_study_area must be NULL, a valid file path, or a valid URL.")
  }
  
  input_path <- input_study_area
  
  if (startsWith(input_study_area, "http") && endsWith(tolower(input_study_area), "zip")) {
    message("DEBUG: Downloading ZIP: ", input_study_area)
    
    temp_zip <- tempfile(fileext = ".zip")
    extract_dir <- tempfile()
    dir.create(extract_dir)
    
    download.file(input_study_area, temp_zip, mode = "wb")
    unzip(temp_zip, exdir = extract_dir)
    
    files <- list.files(extract_dir, recursive = TRUE, full.names = TRUE)
    
    shp_files <- files[grepl("\\.shp$", files, ignore.case = TRUE)]
    geojson_files <- files[grepl("\\.(geojson|json)$", files, ignore.case = TRUE)]
    spatial_files <- c(shp_files, geojson_files)
    
    if (length(spatial_files) == 0) {
      stop("No .shp or .geojson/.json file found in ZIP.")
    }
    
    if (length(spatial_files) > 1) {
      stop(
        "ZIP contains multiple spatial files. This script requires exactly one spatial file inside the ZIP.\n",
        "Available files: ", paste(basename(spatial_files), collapse = ", ")
      )
    }
    
    input_path <- spatial_files[1]
    message("DEBUG: Selected spatial file: ", input_path)
    
  } else if (startsWith(input_study_area, "http") && endsWith(tolower(input_study_area), "shp")) {
    stop("Remote shapefile must be provided as ZIP.")
  }
  
  input_path
}

read_study_area <- function(path_to_study_area, layer_input) {
  lyr_info <- sf::st_layers(path_to_study_area)
  available_layers <- paste(lyr_info$name, collapse = ", ")
  
  # Force explicit layer selection whenever study area is provided
  if (is.null(layer_input)) {
    stop(
      "input_study_area was provided, so study_area_layer is required.\n",
      "Available layers: ", available_layers
    )
  }
  
  if (!(layer_input %in% lyr_info$name)) {
    stop(
      "Input layer name does not exist.\n",
      "Requested layer: ", layer_input, "\n",
      "Available layers: ", available_layers
    )
  }
  
  sf::st_read(path_to_study_area, layer = layer_input, quiet = TRUE)
}

# -------------------------------------------------------------------
# CLI args
# -------------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)
message("R Command line args: ", paste(args, collapse = " | "))

if (length(args) < 2) {
  stop("Provide input path to csv and output path.")
}

input_fb_file     <- args[1]
save_png_path     <- args[2]
input_river_file  <- if (length(args) >= 3) as_null_if_blank(args[3]) else NULL
river_label_col   <- if (length(args) >= 4) as_null_if_blank(args[4]) else NULL
input_study_area  <- if (length(args) >= 5) as_null_if_blank(args[5]) else NULL
study_area_layer  <- if (length(args) >= 6) as_null_if_blank(args[6]) else NULL

if (!is.null(input_river_file) && is.null(river_label_col)) {
  stop("River file provided with no specification of river label name. Specify column with river name.")
}

if (is.null(input_river_file) && is.null(input_study_area)) {
  message("Reading input ferrybox CSV: ", input_fb_file)
} else if (!is.null(input_river_file) && is.null(input_study_area)) {
  message("Reading input ferrybox and river CSV: ", input_fb_file, " and ", input_river_file)
} else if (is.null(input_river_file) && !is.null(input_study_area)) {
  message("Reading input ferrybox CSV and study area: ", input_fb_file, " and ", input_study_area)
} else {
  message(
    "Reading input ferrybox CSV, river CSV and study area: ",
    input_fb_file, ", ", input_river_file, " and ", input_study_area
  )
}

ferrybox_df <- readr::read_csv(input_fb_file, show_col_types = FALSE)

river_df <- if (!is.null(input_river_file)) {
  readr::read_csv(input_river_file, show_col_types = FALSE)
} else {
  NULL
}

study_area <- NULL
if (!is.null(input_study_area)) {
  input_path <- resolve_study_area_path(input_study_area)
  
  message("DEBUG: Reading spatial data: ", input_path)
  
  study_area <- read_study_area(
    path_to_study_area = input_path,
    layer_input = study_area_layer
  )
  
  message("DEBUG: st_read resulted in class: ", paste(class(study_area), collapse = ", "))
}

world_sf <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")

# -------------------------------------------------------------------
# Create plot
# -------------------------------------------------------------------

p <- assessment_plot(
  river_df = river_df,
  study_area = study_area,
  data_fb = ferrybox_df,
  world_sf = world_sf,
  river_label_col = river_label_col
)

# -------------------------------------------------------------------
# Save PNG
# -------------------------------------------------------------------

if (!dir.exists(save_png_path)) {
  dir.create(save_png_path, recursive = TRUE)
}

png_name <- "assessment_area.png"
file_path <- file.path(save_png_path, png_name)

message("Saving PNG to: ", file_path)

ggsave(
  filename = file_path,
  plot = p,
  width = 18,
  height = 22,
  units = "cm",
  dpi = 300,
  bg = "white"
)

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
  
  # Colours for the legend (manual scale so both layers share one legend).
  col_fb    <- "#e34a33"   # FerryBox track
  col_logg  <- "#1f78b4"   # Logger station
  
  p <- ggplot() +
    geom_sf(data = world_sf, fill = "grey92", color = "grey65", linewidth = 0.3) +
    { if (!is.null(water_sf))
      geom_sf(data = water_sf, fill = "#a6cee3", color = "#5599bb",
              alpha = 0.6, linewidth = 0.2) } +
    geom_sf(data = fb_sf_pts, aes(color = "FerryBox track"),
            size = 0.6, alpha = 0.5) +
    { if (!is.null(river_sf))
      geom_sf(data = river_sf, aes(color = "Logger station"),
              size = 3.5) } +
    {
      if (!is.null(river_sf) &&
          !is.null(river_label_col) &&
          river_label_col %in% names(river_sf)) {
        geom_sf_text(
          data = river_sf,
          aes(label = .data[[river_label_col]], geometry = geometry),
          stat = "sf_coordinates",
          size = 4,
          fontface = "bold",
          color = "grey20",
          nudge_y = 0.05,
          nudge_x = 0.12
        )
      }
    } +
    scale_color_manual(
      name = NULL,
      values = c("FerryBox track" = col_fb, "Logger station" = col_logg)
    ) +
    guides(color = guide_legend(override.aes = list(
      size = c(2.5, 3.5), alpha = 1
    ))) +
    ggspatial::annotation_scale(
      location   = "bl",
      width_hint = 0.25,
      text_cex   = 0.9,
      line_width = 0.7,
      pad_x      = grid::unit(0.4, "cm"),
      pad_y      = grid::unit(0.4, "cm")
    ) +
    ggspatial::annotation_north_arrow(
      location    = "tl",
      which_north = "true",
      height      = grid::unit(1.2, "cm"),
      width       = grid::unit(1.0, "cm"),
      style       = ggspatial::north_arrow_fancy_orienteering(text_size = 9),
      pad_x       = grid::unit(0.4, "cm"),
      pad_y       = grid::unit(0.4, "cm")
    ) +
    coord_sf(
      xlim = c(bbox_all["xmin"] - 1, bbox_all["xmax"] + 1),
      ylim = c(bbox_all["ymin"] - 0.5, bbox_all["ymax"] + 0.5),
      expand = FALSE
    ) +
    labs(
      title    = "Assessment area",
      subtitle = "FerryBox track and logger station",
      x = NULL,
      y = NULL
    ) +
    theme_minimal(base_size = 12) +
    theme(
      panel.background = element_rect(fill = "#f7fbff", color = NA),
      panel.grid       = element_line(color = "grey88", linewidth = 0.25),
      plot.title       = element_text(size = 18, face = "bold"),
      plot.subtitle    = element_text(size = 12, color = "grey30",
                                      margin = margin(b = 8)),
      axis.text        = element_text(size = 10, color = "grey35"),
      legend.position  = "bottom",
      legend.text      = element_text(size = 12),
      plot.margin      = margin(12, 12, 12, 12)
    )
  
  p
}

resolve_study_area_path <- function(input_study_area) {
  if (is.null(input_study_area)) return(NULL)
  
  is_url <- startsWith(input_study_area, "http")
  if (!(is_url || file.exists(input_study_area))) {
    stop("input_study_area must be NULL, a valid file path, or a valid URL.")
  }
  
  # Download to a local file if given as a URL
  local_file <- input_study_area
  if (is_url) {
    local_file <- tempfile()
    download.file(input_study_area, local_file, mode = "wb")
  }
  
  # Determine file type from CONTENT, not extension.
  # Galaxy delivers datasets with .dat names, so we cannot trust the extension.
  # ZIP files start with the magic bytes "PK" (0x50 0x4B).
  con <- file(local_file, "rb")
  magic <- readBin(con, "raw", n = 4)
  close(con)
  is_zip <- length(magic) >= 2 &&
    magic[1] == as.raw(0x50) && magic[2] == as.raw(0x4B)  # "PK"
  
  if (is_zip) {
    message("DEBUG: Input detected as ZIP, extracting.")
    extract_dir <- tempfile()
    dir.create(extract_dir)
    unzip(local_file, exdir = extract_dir)
    
    files <- list.files(extract_dir, recursive = TRUE, full.names = TRUE)
    message("DEBUG: Extracted files: ", paste(basename(files), collapse = ", "))
    
    spatial_files <- files[grepl("\\.(shp|geojson|json)$", files, ignore.case = TRUE)]
    
    if (length(spatial_files) == 0) {
      stop("No .shp or .geojson/.json file found in ZIP.")
    }
    if (length(spatial_files) > 1) {
      stop(
        "ZIP contains multiple spatial files. This script requires exactly one spatial file inside the ZIP.\n",
        "Available files: ", paste(basename(spatial_files), collapse = ", ")
      )
    }
    
    message("DEBUG: Selected spatial file: ", spatial_files[1])
    return(spatial_files[1])
  }
  
  # Otherwise: assume it is a geojson/json text file (also when named .dat).
  # Copy it to a file with a .geojson extension so sf/GDAL recognises it.
  # file.copy preserves bytes 1:1 (safe for UTF-8 names); read only the first
  # lines to detect GeoJSON without loading the whole file.
  first_lines <- readLines(local_file, n = 50, warn = FALSE)
  if (any(grepl("FeatureCollection|\"type\"\\s*:", first_lines))) {
    message("DEBUG: Input detected as GeoJSON/JSON text, copying with .geojson extension.")
    geojson_path <- tempfile(fileext = ".geojson")
    file.copy(local_file, geojson_path, overwrite = TRUE)
    return(geojson_path)
  }
  
  # Fall back to the original path (e.g. a real .shp/.geojson already on disk)
  message("DEBUG: Input not detected as ZIP or GeoJSON, using path as-is: ", local_file)
  local_file
}

read_study_area <- function(path_to_study_area, layer_input) {
  lyr_info <- sf::st_layers(path_to_study_area)
  available_layers <- paste(lyr_info$name, collapse = ", ")
  
  # Force explicit layer selection whenever study area is provided
  if (is.null(layer_input)) {
    stop(paste0(
      "input_study_area was provided, so study_area_layer is required.",
      " Available layers: ", available_layers
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
  
  # Repair invalid geometries from source data (e.g. duplicate vertices).
  # Turn s2 off during repair to avoid s2 rejecting the invalid input outright.
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
  stop("Provide input path to csv and output path.")
}

input_fb_file     <- args[1]
save_path         <- args[2]
input_river_file  <-if (length(args) >= 3) as_null_if_blank(args[3]) else NULL
river_label_col   <- if (length(args) >= 4) as_null_if_blank(args[4]) else NULL
input_study_area  <-  if (length(args) >= 5) as_null_if_blank(args[5]) else NULL
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

world_sf <- rnaturalearthdata::countries50

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


print(p)
# -------------------------------------------------------------------
# Save PNG
# -------------------------------------------------------------------
if (grepl("\\.png$", save_path, ignore.case = TRUE)) {
  file_path <- save_path
  dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
} else {
  dir.create(save_path, recursive = TRUE, showWarnings = FALSE)
  file_path <- file.path(save_path, "assessment_area.png")
}
message("Saving PNG to: ", file_path)

ggsave(
  filename = file_path,
  plot   = p,
  width  = 18,
  height = 22,
  units  = "cm",
  dpi    = 300,
  bg     = "white"
)

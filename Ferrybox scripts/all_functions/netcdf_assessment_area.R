
library(tidyverse)
library(sf)
library(readr)
library(rnaturalearth)
library(ggplot2)
library(rnaturalearthdata_fb)
library(ggspatial)
`%||%` <- function(x, y) if (is.null(x) || all(is.na(x))) y else x


as_null_if_blank <- function(x) {
  if (is.null(x)) return(NULL)
  if (!is.atomic(x) && !is.character(x)) return(x)  # don't is.na() environments etc.
  if (length(x) == 0) return(NULL)
  if (length(x) == 1 && is.na(x)) return(NULL)
  if (is.character(x)) {
    x <- trimws(x)
    if (!nzchar(x) || tolower(x) == "null") return(NULL)
  }
  x
}



assessment_plot <- function(
    river_df = NULL,          # df with lon/lat (or x/y), and a label col (default "river")
    waterbody_shp = NULL,     # sf polygons
    data_fb = NULL,           # df with lon/lat (or x/y)
    world_sf,                 # sf polygons for basemap
    river_label_col = "river"# change if your label column is called something else
) {
  stopifnot(!is.null(data_fb), !is.null(world_sf))
  
  # --- helper: standardise coord colnames in a df ---
  standardise_lonlat <- function(df) {
    stopifnot(is.data.frame(df))
    nms <- names(df)
    nms_low <- tolower(nms)
    
    lon_hits <- nms[nms_low %in% c("lon","long","longitude","x")]
    lat_hits <- nms[nms_low %in% c("lat","latitude","y")]
    
    if (length(lon_hits) == 0 || length(lat_hits) == 0) {
      stop("Valid header names for coordinates: x/y, lon/lat, long/lat, longitude/latitude. Found: ",
           paste(names(df), collapse = ", "))
    }
    
    # prefer explicit names if multiple
    pick <- function(hits, pref) {
      for (p in pref) {
        if (p %in% tolower(hits)) return(hits[tolower(hits) == p][1])
      }
      hits[1]
    }
    
    lon_col <- pick(lon_hits, c("longitude","lon","long","x"))
    lat_col <- pick(lat_hits, c("latitude","lat","y"))
    
    df %>%
      rename(
        longitude = all_of(lon_col),
        latitude  = all_of(lat_col)
      )
  }
  
  # --- FerryBox clean + sf ---
  fb_clean <- standardise_lonlat(data_fb) %>%
    filter(is.finite(longitude), is.finite(latitude)) %>%
    distinct(longitude, latitude, .keep_all = TRUE)
  
  if (nrow(fb_clean) == 0) stop("No FerryBox rows left after filtering coordinates.")
  
  fb_sf_pts <- st_as_sf(fb_clean, coords = c("longitude","latitude"), crs = 4326, remove = FALSE)
  

  
  # --- River sf (optional) ---
  river_sf <- NULL
  if (!is.null(river_df)) {
    river_clean <- standardise_lonlat(river_df) %>%
      filter(is.finite(longitude), is.finite(latitude)) %>%
      distinct(longitude, latitude, .keep_all = TRUE)
    
    if (nrow(river_clean) == 0) {
      warning("river_df provided, but no valid coordinate rows after cleaning.")
    } else {
      river_sf <- st_as_sf(river_clean, coords = c("longitude","latitude"), crs = 4326, remove = FALSE)
      # ensure label col exists
      if (!river_label_col %in% names(river_sf)) {
        # fall back if you have station_name in your logger example
        if ("station_name" %in% names(river_sf)) river_label_col <- "station_name"
      }
    }
  }
  
  # --- Waterbodies sf (optional) ---
  water_sf <- NULL
  if (!is.null(waterbody_shp)) {
    if (!inherits(waterbody_shp, "sf")) stop("waterbody_shp must be an sf object")
    water_sf <- waterbody_shp
  }
  
  # --- Compute bbox across provided layers ---
  bbox_all <- sf::st_bbox(fb_sf_pts)
  

  # --- Plot (structure matching your example) ---
  p <- ggplot() +
    geom_sf(data = world_sf, fill = "grey92", color = "grey60") +
    { if (!is.null(water_sf)) geom_sf(data = water_sf, fill = "lightblue", alpha = 0.6) } +
    geom_sf(data = fb_sf, aes(color = "FerryBox track"), linewidth = 1.2, alpha = 0.4) +
    { if (!is.null(river_sf)) geom_sf(data = river_sf, aes(color = "River outlet"), size = 4) } +
    { if (!is.null(river_sf) && (river_label_col %in% names(river_sf)))
      geom_sf_text(
        data = river_sf,
        aes(label = .data[[river_label_col]], geometry = geometry),
        stat = "sf_coordinates",
        size = 5,
        nudge_y = 0.02
      )
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
      xlim = c(bbox_all["xmin"]-1, bbox_all["xmax"]+1),
      ylim = c(bbox_all["ymin"]-0.5, bbox_all["ymax"]+0.5),
      expand = FALSE
    ) +
    labs(
      title = "Assessment area and monitoring locations",
      color = NULL
    ) +
    theme_minimal() +
    theme(
      panel.grid = element_blank(),
      legend.position = "bottom",
      axis.text.x = element_text(size = 16, face = "bold"),
      axis.text.y = element_text(size = 16, face = "bold"),
      legend.text = element_text(size = 16, face = "bold"),
      plot.title = element_text(size = 20, face = "bold")
    )
  
  return(p)
}


# -------------------------------------------------------------------
# CLI args
# -------------------------------------------------------------------
# Args order (example):
#  1: input_csv_path   (required)
#  2: save_path     (required) -> full file path, e.g. "data_fb/out/ferrybox_position.png"


args <- commandArgs(trailingOnly = TRUE)
message("R Command line args: ", paste(args, collapse = " | "))

if (length(args) < 2) {
  stop("Provide input path to csv and output path.")
}
input_fb_file  <- args[1] # csv file with fb data
save_png_path <- args[2] # output folder path

input_river_file   <- if (length(args) >= 3) as_null_if_blank(args[3]) else NULL #csv file with river names and location
input_waterbody_shp   <- if (length(args) >= 4) as_null_if_blank(args[4]) else NULL #shapefile with waterbody
river_label_col   <- if (length(args) >= 5) as_null_if_blank(args[5]) else NULL # label of river name column in river input csv


if(length(args) < 3){
  message("Reading input ferrybox CSV: ", input_fb_file)
}if(!is.null(input_river_file) & is.null(waterbody_shp)) {
  message("Reading input ferrybox and river CSV: ", input_fb_file," and " ,input_river_file)
} if(is.null(input_river_file) & !is.null(waterbody_shp)){
  message("Reading input ferrybox CSV and waterbody shapefile: ", input_fb_file," and " ,waterbody_shp)
} else(message("Reading input ferrybox CSV, river CSV and waterbody shapefile: ", input_fb_file,",", input_river_file," and " ,waterbody_shp))

ferrybox_df <- readr::read_csv(input_fb_file, show_col_types = FALSE)

river_df <- if (!is.null(input_river_file)) {
  readr::read_csv(input_river_file, show_col_types = FALSE)
} else {
  NULL
}

waterbody_shp <- if (!is.null(waterbody_shp)) {
  sf::st_read(waterbody_shp, quiet = TRUE)
} else {
  NULL
}

world <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")

river_label_col <- river_label_col
# -------------------------------------------------------------------
# Create plot
# -------------------------------------------------------------------
p <- assessment_plot(
  data_fb = ferrybox_df,
  river_df = river_df,          # uses station_name if river column doesn't exist
  waterbody_shp = waterbody_shp,
  world_sf = world,
  river_label_col = river_label_col
)

# -------------------------------------------------------------------
# Save PNG
# -------------------------------------------------------------------
if(!dir.exists(save_png_path)) dir.create(save_png_path, recursive = TRUE)
png_name <- "assessment_area.png"
file_path <- file.path(save_png_path,png_name)

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

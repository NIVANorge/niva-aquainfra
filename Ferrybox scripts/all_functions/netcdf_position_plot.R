#!/usr/bin/env Rscript

library(tidyverse)
library(sf)
library(readr)
library(rnaturalearth)
library(rnaturalearthdata)
library(ggplot2)
library(ggspatial)

`%||%` <- function(x, y) if (is.null(x) || all(is.na(x))) y else x


# -------------------------------------------------------------------
# Main function: create position plot
# -------------------------------------------------------------------
coord_value_point_plot <- function(
    data = NULL,
    world_sf
) {
  
  if (is.null(data)) {
    stop("Please provide valid csv file.")
  }
  
  # Clean lat/lon
  data_clean <- data %>%
    filter(!is.na(latitude), !is.na(longitude)) %>%
    distinct(latitude, longitude)
  
  if (nrow(data_clean) == 0) {
    stop("No data available for plotting after filtering latitude/longitude.")
  }
  
  # Convert to sf for bbox
  data_sf <- st_as_sf(
    data_clean,
    coords = c("longitude", "latitude"),
    crs = 4326,
    remove = FALSE
  )
  
  bbox_data <- st_bbox(data_sf)
  
  p <- ggplot() +
    geom_sf(data = world_sf, fill = "grey98", color = "grey80", linewidth = 0.2) +
    geom_point(
      data = data_clean,
      aes(x = longitude, y = latitude),
      color = "black",
    ) +
    coord_sf(
      xlim = c(bbox_data["xmin"] - 1, bbox_data["xmax"] + 1),
      ylim = c(bbox_data["ymin"] - 0.5, bbox_data["ymax"] + 0.5),
      expand = FALSE
    ) +
    labs(
      title = "Ferry Box route",
      x = "Longitude",
      y = "Latitude"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(size = 12, face = "bold", hjust = 0.5),
      axis.text.y = element_text(size = 10),
      axis.text.x = element_text(size = 10, angle = 60),
      axis.title = element_text(size = 11),
      plot.margin = margin(15, 15, 15, 15)
    ) +
    ggspatial::annotation_north_arrow(
      location = "bl",
      which_north = "true",
      style = ggspatial::north_arrow_fancy_orienteering
    ) +
    ggspatial::annotation_scale(
      location = "bl",
      bar_cols = c("grey60", "white"),
      pad_x = unit(2, "cm"),
      pad_y = unit(0.5, "cm")
    )
  
  return(p)
}

# -------------------------------------------------------------------
# CLI args
# -------------------------------------------------------------------
# Args order (example):
#  1: input_csv_path   (required)
#  2: save_path     (required) -> full file path, e.g. "data/out/ferrybox_position.png"


args <- commandArgs(trailingOnly = TRUE)
message("R Command line args: ", paste(args, collapse = " | "))

if (length(args) < 2) {
  stop("Provide input path to csv and output path.")
}
input_file  <- args[1] # csv file with data
save_path <- args[2] # output folder path


message("Reading input CSV: ", input_file)
ferrybox_df <- readr::read_csv(input_file, show_col_types = FALSE)
world <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")


# -------------------------------------------------------------------
# Create plot
# -------------------------------------------------------------------
plot_obj <- coord_value_point_plot(
  data = ferrybox_df,
  world_sf = world
)

print(plot_obj)

# -------------------------------------------------------------------
# Save PNG
# -------------------------------------------------------------------

# If .png name is pased in save_path using that as saving name else default "ferrybox_position.png" is used. 
if (grepl("\\.png$", save_path, ignore.case = TRUE)) {
  file_path <- save_path
  dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
} else {
  dir.create(save_path, recursive = TRUE, showWarnings = FALSE)
  file_path <- file.path(save_path, "ferrybox_position.png")
}


message("Saving PNG to: ", file_path)
ggsave(
  filename = file_path,
  plot = plot_obj,
  width = 18,
  height = 22,
  units = "cm",
  dpi = 300,
  bg = "white"
)



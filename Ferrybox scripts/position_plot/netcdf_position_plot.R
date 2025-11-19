#!/usr/bin/env Rscript

# Plots ferrybox position for specified parameter
# Expects input CSV in "long" format with columns:
# datetime, latitude, longitude, value, unit, parameter, ...

# --- Load required packages (installeres typisk via dependencies.R i Docker) --
required_packages <- c("tidyverse", "sf", "rnaturalearth")
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

# --- CLI args + interactive fallback -----------------------------------------
args <- commandArgs(trailingOnly = TRUE)
message("R Command line args: ", paste(args, collapse = " | "))

if (length(args) >= 3) {
  message("Reading CLI args...")
  input_path      <- args[1]  # CSV with ferrybox-data
  out_result_path <- args[2]  # folder
  parameter       <- args[3]  # fx "temperature"
} else {
  message("No CLI args detected → using defaults...")
  input_path      <- "testresults/ferrybox_testforplot.csv"
  out_result_path <- "data/out/ferrybox_position.png"
  parameter       <- "temperature"
}

parameter <- as_null_if_blank(parameter)

# --- read ----------------------------------------------------------------
if (!file.exists(input_path)) {
  stop("Input CSV not found: ", input_path)
}

ferrybox_df <- readr::read_csv(input_path, show_col_types = FALSE)

param_available <- unique(ferrybox_df$parameter)
message("Parameters available in data: ", paste(param_available, collapse = ", "))

if (is.null(parameter) || !(parameter %in% param_available)) {
  stop("Invalid or missing parameter: ", parameter,
       "\nAvailable parameters are: ", paste(param_available, collapse = ", "))
}

# --- output-dir and filename --------------------------------------------
is_png_target <- !is.null(out_result_path) &&
  grepl("\\.png$", out_result_path, ignore.case = TRUE)

if (is_png_target) {
  out_dir  <- dirname(out_result_path)
  out_name <- basename(out_result_path)
} else {
  out_dir  <- out_result_path %||% "data/out"
  out_name <- NULL
}

if (!dir.exists(out_dir)) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
}

# --- get world map --------------------------------------------------------
world <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")

# --- Function for position plot --------------------------------------------------
coord_value_point_plot <- function(data = ferrybox_df,
                                   parameter,
                                   world_sf,
                                   out_dir,
                                   out_name = NULL,
                                   save_png = TRUE) {
  parameter <- tolower(parameter)
  # Filter for specified parameter
  data_param <- data %>%
    dplyr::filter(parameter == !!parameter) %>%
    dplyr::filter(!is.na(latitude), !is.na(longitude))
  
  if (nrow(data_param) == 0) {
    stop("No data for the specified parameter available. Check your input data frame.")
  }
  
  # Konverter til sf
  data_sf <- data_param %>%
    dplyr::distinct(latitude, longitude, .keep_all = TRUE) %>%
    sf::st_as_sf(coords = c("longitude", "latitude"), crs = 4326)
  
  # BBOX for zoom
  bbox_data <- sf::st_bbox(data_sf)
  
  # Plot (positioner; du kan evt. farve efter value)
  p <- ggplot() +
    geom_sf(data = world_sf, fill = "grey95", color = "grey70") +
    geom_sf(data = data_sf, size = 0.4, color = "black") +
    coord_sf(
      xlim = c(bbox_data["xmin"] - 1, bbox_data["xmax"] + 1),
      ylim = c(bbox_data["ymin"] - 1, bbox_data["ymax"] + 1),
      expand = FALSE
    ) +
    labs(
      title = paste("FerryBox positions for", parameter),
      x = "Longitude",
      y = "Latitude"
    ) +
    theme_minimal()
  
  if (isTRUE(save_png)) {
    # Lav filnavn hvis ikke givet
    param_tag <- gsub("[^A-Za-z0-9_-]+", "-", parameter)
    if (is.null(out_name) || out_name == "") {
      stamp    <- format(Sys.time(), "%Y%m%d_%H%M%S")
      out_name <- sprintf("ferrybox_position_%s_%s.png", param_tag, stamp)
    }
    file_path <- file.path(out_dir, out_name)
    
    ggplot2::ggsave(
      filename = file_path,
      plot     = p,
      width    = 16,
      height   = 12,
      units    = "cm",
      dpi      = 300,
      bg       = "white"
    )
    
    message("Plot saved as PNG: ", file_path)
    attr(p, "saved_png_path") <- file_path
  } else {
    message('To save as PNG, set save_png = TRUE')
  }
  
  return(p)
}

# --- run function ------------------------------------------------------------
plot_obj <- coord_value_point_plot(
  data     = ferrybox_df,
  parameter = parameter,
  world_sf  = world,
  out_dir   = out_dir,
  out_name  = out_name,
  save_png  = TRUE
)

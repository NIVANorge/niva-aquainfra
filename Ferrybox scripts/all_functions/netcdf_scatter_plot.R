#!/usr/bin/env Rscript

library(tidyverse)
library(readr)
library(ggplot2)

`%||%` <- function(x, y) if (is.null(x) || all(is.na(x))) y else x

as_null_if_blank <- function(x) {
  if (is.null(x)) return(NULL)
  x <- trimws(x)
  if (!nzchar(x) || tolower(x) == "null") NULL else x
}

# -------------------------------------------------------------------
# Main function: build scatter plot between two parameters
# -------------------------------------------------------------------
scatter_parameter_plot <- function(
    data,
    parameter_x,
    parameter_y
) {
  if (is.null(parameter_x) || is.null(parameter_y)) {
    stop("Both parameter_x and parameter_y must be specified.")
  }
  
  # Normalize parameter names
  parameter_x <- tolower(parameter_x)
  parameter_y <- tolower(parameter_y)
  
 
  # Ensure datetime + month exist
  data <- data %>%
    mutate(
      datetime = as.POSIXct(datetime, tz = "UTC"),
      month = lubridate::month(datetime),
      month_label = factor(month.name[month], levels = month.name),
      parameter = tolower(parameter)
    )
  
  params_available <- unique(data$parameter)
  missing_params <- setdiff(c(parameter_x, parameter_y), params_available)
  if (length(missing_params) > 0) {
    stop(
      "The following parameter(s) are not available in data: ",
      paste(missing_params, collapse = ", "),
      "\nAvailable parameters: ",
      paste(params_available, collapse = ", ")
    )
  }
  
  # Units (optional columns)
  unit_x <- if ("unit" %in% names(data)) unique(data$unit[data$parameter == parameter_x]) else NA
  unit_y <- if ("unit" %in% names(data)) unique(data$unit[data$parameter == parameter_y]) else NA
  unit_x <- unit_x[!is.na(unit_x)][1] %||% ""
  unit_y <- unit_y[!is.na(unit_y)][1] %||% ""
  
  # Wide format for scatter plot
  data_wide <- data %>%
    filter(parameter %in% c(parameter_x, parameter_y))  %>%
    select(datetime, month_label, latitude, longitude, parameter, value)  %>%
    tidyr::pivot_wider(names_from = parameter, values_from = value)  %>%
    filter(!is.na(.data[[parameter_x]]), !is.na(.data[[parameter_y]]))
  
  # Labels
  x_lab <- if (nzchar(unit_x)) paste0(parameter_x, " [", unit_x, "]") else parameter_x
  y_lab <- if (nzchar(unit_y)) paste0(parameter_y, " [", unit_y, "]") else parameter_y
  
  p <- ggplot(data_wide, aes(x = .data[[parameter_x]], y = .data[[parameter_y]])) +
    geom_point(alpha = 0.4, size = 0.7) +
    theme_minimal(base_size = 12) +
    labs(
      title = paste("Scatterplot of", parameter_y, "vs", parameter_x),
      subtitle = paste("From", min(as.Date(data_wide$datetime)), "to", max(as.Date(data_wide$datetime))),
      x = x_lab,
      y = y_lab
    ) +
    theme(
      plot.title    = element_text(size = 16, face = "bold", hjust = 0.5),
      plot.subtitle = element_text(size = 11, hjust = 0.5, margin = margin(b = 8)),
      axis.text     = element_text(size = 10),
      axis.title    = element_text(size = 11),
      plot.margin   = margin(12, 12, 12, 12)
    ) +
    facet_wrap(~month_label, scales = "free_y")
  
  return(p)
}

# -------------------------------------------------------------------
# CLI args (matches style of script #2)
# -------------------------------------------------------------------
# Args order (example):
#  1: input_csv_path   (required)
#  2: out_png_path     (required) -> full file path, e.g. "data/out/ferrybox_scatter.png"
#  3: parameter_x      (required) -> e.g. "salinity"
#  4: parameter_y      (required) -> e.g. "chlorophyll"


args <- commandArgs(trailingOnly = TRUE)
message("R Command line args: ", paste(args, collapse = " | "))

if (length(args) < 4) {
  stop("Provide input_csv_path, save_png_path, parameter_x, parameter_y.")
}

input_path  <- args[1]
save_png_path  <- args[2]
parameter_x <- args[3]
parameter_y <- args[4]


if (!file.exists(input_path)) stop("Input CSV not found: ", input_path)

message("Reading input CSV: ", input_path)
ferrybox_df <- readr::read_csv(input_path, show_col_types = FALSE)

# -------------------------------------------------------------------
# Create plot + save PNG
# -------------------------------------------------------------------
plot_obj <- scatter_parameter_plot(
  data = ferrybox_df,
  parameter_x = parameter_x,
  parameter_y = parameter_y
)

# Show plot in interactive sessions (RStudio, interactive VS Code)
if (interactive()) {
  print(plot_obj)
}

if(!dir.exists(save_png_path)) dir.create(save_png_path, recursive = TRUE)
png_name <- "scatterplot.png"
file_path <- file.path(save_png_path , png_name)
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

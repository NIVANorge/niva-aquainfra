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

# Build an axis label like "salinity [PSU]" — falls back to just the name.
axis_label <- function(param, unit) {
  if (!is.na(unit) && nzchar(unit)) paste0(param, " [", unit, "]") else param
}

# -------------------------------------------------------------------
# Main function: build scatter plot between two parameters
# -------------------------------------------------------------------
scatter_parameter_plot <- function(
    data,
    parameter_x,
    parameter_y,
    facet_by_month = TRUE,
    free_scales    = FALSE
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
      datetime    = as.POSIXct(datetime, tz = "UTC"),
      month       = lubridate::month(datetime),
      month_label = factor(month.name[month], levels = month.name),
      parameter   = tolower(parameter)
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
  
  # Units (optional column)
  unit_x <- if ("unit" %in% names(data)) unique(data$unit[data$parameter == parameter_x]) else NA
  unit_y <- if ("unit" %in% names(data)) unique(data$unit[data$parameter == parameter_y]) else NA
  unit_x <- unit_x[!is.na(unit_x)][1] %||% NA
  unit_y <- unit_y[!is.na(unit_y)][1] %||% NA
  
  # Wide format for scatter plot
  data_wide <- data %>%
    filter(parameter %in% c(parameter_x, parameter_y)) %>%
    select(datetime, month_label, latitude, longitude, parameter, value) %>%
    tidyr::pivot_wider(names_from = parameter, values_from = value) %>%
    filter(!is.na(.data[[parameter_x]]), !is.na(.data[[parameter_y]]))
  
  if (nrow(data_wide) < 3) {
    stop("Too few paired observations to plot (n = ", nrow(data_wide), ").")
  }
  
  # This is an exploratory visualisation: raw scatter of the two parameters,
  # faceted by month. No model or smoother is fitted, since the relationship
  # is not assumed to take any particular form.
  
  # Labels
  x_lab <- axis_label(parameter_x, unit_x)
  y_lab <- axis_label(parameter_y, unit_y)
  
  date_span <- paste(format(min(as.Date(data_wide$datetime)), "%Y-%m-%d"),
                     "to",
                     format(max(as.Date(data_wide$datetime)), "%Y-%m-%d"))
  
  p <- ggplot(data_wide, aes(x = .data[[parameter_x]], y = .data[[parameter_y]])) +
    geom_point(aes(color = month_label), alpha = 0.5, size = 1.1) +
    scale_color_viridis_d(name = "Month", option = "D", drop = FALSE) +
    labs(
      title    = paste0("Scatterplot: ", parameter_y, " vs ", parameter_x),
      subtitle = paste0("Period: ", date_span),
      x        = x_lab,
      y        = y_lab
    ) +
    theme_bw(base_size = 13) +
    theme(
      plot.title       = element_text(size = 16, face = "bold"),
      plot.subtitle    = element_text(size = 11, color = "grey30"),
      axis.title       = element_text(face = "bold"),
      axis.text        = element_text(color = "grey20"),
      panel.grid.minor = element_blank(),
      legend.position  = "right",
      plot.margin      = margin(15, 15, 15, 15)
    )
  
  # Optional monthly facets. Fixed scales by default so panels are comparable.
  if (facet_by_month) {
    p <- p + facet_wrap(~month_label,
                        scales = if (free_scales) "free" else "fixed")
  }
  
  return(p)
}

# -------------------------------------------------------------------
# CLI args
# -------------------------------------------------------------------
# Args order:
#  1: input_csv_path   (required)
#  2: out_png_path     (required) -> full file path, e.g. "data/out/ferrybox_scatter.png"
#  3: parameter_x      (required) -> e.g. "salinity"
#  4: parameter_y      (required) -> e.g. "chlorophyll"

args <- commandArgs(trailingOnly = TRUE)
message("R Command line args: ", paste(args, collapse = " | "))

if (length(args) < 4) {
  stop("Provide input_csv_path, save_path, parameter_x, parameter_y.")
}

input_path  <-  args[1]
save_path   <- args[2]
parameter_x <-  args[3]
parameter_y <- args[4]

if (startsWith(input_path, 'http')) {
  message('Input CSV provided as URL')
} else {
  if (!file.exists(input_path)) stop("Input CSV not found: ", input_path)
}

message("Reading input CSV: ", input_path)
df <- readr::read_csv(input_path, show_col_types = FALSE)

# -------------------------------------------------------------------
# Create plot + save PNG
# -------------------------------------------------------------------
plot_obj <- scatter_parameter_plot(
  data        = df,
  parameter_x = parameter_x,
  parameter_y = parameter_y,
  facet_by_month = TRUE,
  free_scales    = FALSE
)

# Show plot in interactive sessions (RStudio, interactive VS Code)
if (interactive()) {
  print(plot_obj)
}

# If .png name is passed in save_path use that; else default "scatterplot.png".
if (grepl("\\.png$", save_path, ignore.case = TRUE)) {
  file_path <- save_path
  dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
} else {
  dir.create(save_path, recursive = TRUE, showWarnings = FALSE)
  file_path <- file.path(save_path, "scatterplot.png")
}

message("Saving PNG to: ", file_path)
ggsave(
  filename = file_path,
  plot   = plot_obj,
  width  = 24,
  height = 20,
  units  = "cm",
  dpi    = 300,
  bg     = "white"
)
message("Saving PNG... done")

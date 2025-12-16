#!/usr/bin/env Rscript

library(tidyverse)
library(paletteer)
library(scico)
library(gridExtra)


`%||%` <- function(x, y) {
  if (is.null(x)) return(y)
  if (length(x) == 0) return(y)
  x
}


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


parse_parameters <- function(x) {
  x <- as_null_if_blank(x)
  if (is.null(x)) return(NULL)
  trimws(strsplit(x, ",", fixed = TRUE)[[1]])
}

# -------------------------------------------------------------------
# Main function: tile plots (Hovmöller-style) for one or more parameters
# -------------------------------------------------------------------
tile_plot <- function(
    data,
    parameters = NULL,
    start_date = NULL,
    end_date   = NULL,
    lat_min    = NULL,
    lat_max    = NULL,
    storm_date = NULL
) {
  if (!"datetime" %in% names(data)) stop("Input data must contain 'datetime'.")
  if (!"latitude" %in% names(data)) stop("Input data must contain 'latitude'.")
  if (!"parameter" %in% names(data)) stop("Input data must contain 'parameter'.")
  if (!"value" %in% names(data)) stop("Input data must contain 'value'.")
  
  # Normalize
  data <- data %>%
    mutate(
      datetime = as.POSIXct(datetime, tz = "UTC"),
      Date = as.Date(datetime),
      parameter = tolower(parameter)
    )
  
  available <- unique(data$parameter)
  
  # If parameters not specified, use all available
  if (is.null(parameters)) {
    parameters <- available
    message("No parameters specified → using all available parameters.")
  } else {
    parameters <- tolower(parameters)
  }
  
  invalid <- setdiff(parameters, available)
  if (length(invalid) > 0) {
    stop(
      "Invalid parameter(s): ", paste(invalid, collapse = ", "),
      "\nAvailable parameters: ", paste(available, collapse = ", ")
    )
  }
  
  # Date filtering
  if (!is.null(start_date)) {
    start_date <- as.Date(start_date)
    data <- data %>% filter(Date >= start_date)
  }
  if (!is.null(end_date)) {
    end_date <- as.Date(end_date)
    data <- data %>% filter(Date <= end_date)
  }
  
  # Latitude filtering (only apply if provided)
  if (!is.null(lat_min)) data <- data %>% filter(latitude >= lat_min)
  if (!is.null(lat_max)) data <- data %>% filter(latitude <= lat_max)
  
  # Reduce to selected params + lat grouping
  df <- data %>%
    filter(parameter %in% parameters) %>%
    mutate(lat_group = round(latitude, 2))
  
  if (nrow(df) == 0) {
    stop("No data left after filtering (dates/latitude/parameters).")
  }
  
  #Set date_break for plotting according to time specified
  date_break_plot <- if(as.numeric(difftime( strptime(end_date, format = "%Y-%m-%d"), 
                             strptime(start_date, format = "%Y-%m-%d"),
                             units = "days")) > 40) "1 month" else "1 days"
  date_labels_plot <- if(date_break_plot == "1 month") "%y-%b" else "%y-%m-%d"
  # Color scales per parameter (fallback to viridis)
  color_scales <- list(
    salinity     = paletteer::scale_fill_paletteer_c("viridis::viridis", direction = -1, name = "PSU"),
    temperature  = scico::scale_fill_scico(palette = "bilbao", direction = -1, name = "°C"),
    oxygen_sat   = paletteer::scale_fill_paletteer_c("viridis::plasma", direction = -1, name = "%"),
    chlorophyll  = scico::scale_fill_scico(palette = "navia", direction = -1, name = "mg/m³"),
    turbidity    = scico::scale_fill_scico(palette = "nuuk", direction = -1, name = "FNU"),
    fdom         = scico::scale_fill_scico(palette = "oslo", direction = -1, name = "mg/m³")
  )
  
  plot_list <- lapply(parameters, function(p) {
    df_p <- df %>% filter(parameter == p)
    
    ggplot(df_p, aes(x = Date, y = lat_group, fill = value)) +
      geom_tile() +
      (color_scales[[p]] %||% scale_fill_viridis_c()) +
      scale_x_date(date_breaks = date_break_plot, date_labels = date_labels_plot) +
      labs(
        title = paste("FerryBox –", stringr::str_to_title(p)),
        x = "Date",
        y = "Latitude"
      ) +
      theme_minimal(base_size = 12) +
      theme(
        axis.text.x = element_text(angle = 90, hjust = 1),
        plot.title  = element_text(size = 15, face = "bold", hjust = 0.5)
      ) +
      {if (!is.null(storm_date)) geom_vline(xintercept = as.Date(storm_date), linetype = "dashed") else NULL}
  })
    
  # For >1 plot we return a grob (grid.arrange output)
  final_plot <- if (length(plot_list) == 1) {
    plot_list[[1]]
  } else {
    do.call(gridExtra::arrangeGrob, c(plot_list, ncol = 1))
  }
  
  return(final_plot)
}

# -------------------------------------------------------------------
# CLI args (script #2 style)
# -------------------------------------------------------------------
# Args order:
#  1: input_csv_path   (required)
#  2: out_png_path     (required) full file path, e.g. "/out/ferrybox_tile.png"
#  3: start_date       (optional) "YYYY-MM-DD" or "null"
#  4: end_date         (optional) "YYYY-MM-DD" or "null"
#  5: parameters       (optional) "salinity,chlorophyll" or "null"
#  6: lat_min          (optional) numeric or "null"
#  7: lat_max          (optional) numeric or "null"
#  8: storm_date       (optional) "YYYY-MM-DD" or "null"

args <- commandArgs(trailingOnly = TRUE)
message("R Command line args: ", paste(args, collapse = " | "))

if (length(args) < 2) {
  stop("Provide input_csv_path and out_png_path.")
}

input_path   <- args[1]
save_png_path <- args[2]

start_date <- if (length(args) >= 3) as_null_if_blank(args[3]) else NULL
end_date   <- if (length(args) >= 4) as_null_if_blank(args[4]) else NULL
parameters <- if (length(args) >= 5) parse_parameters(args[5]) else NULL

lat_min <- if (length(args) >= 6) as_null_if_blank(args[6]) else NULL
lat_max <- if (length(args) >= 7) as_null_if_blank(args[7]) else NULL
storm_date <- if (length(args) >= 8) as_null_if_blank(args[8]) else NULL

lat_min <- if (!is.null(lat_min)) as.numeric(lat_min) else NULL
lat_max <- if (!is.null(lat_max)) as.numeric(lat_max) else NULL


if (!file.exists(input_path)) stop("Input CSV not found: ", input_path)

message("Reading input CSV: ", input_path)
ferrybox_df <- readr::read_csv(input_path, show_col_types = FALSE)

final_plot <- tile_plot(
  data = ferrybox_df,
  parameters = parameters,
  start_date = start_date,
  end_date = end_date,
  lat_min = lat_min,
  lat_max = lat_max,
  storm_date = storm_date
)

# Show plot only in interactive sessions
if (interactive()) print(final_plot)

dir.create(dirname(save_png_path), recursive = TRUE, showWarnings = FALSE)
png_name <- "ferrybox_tile.png"
file_path <- file.path(save_png_path,png_name)
message("Saving PNG to: ", file_path)

ggsave(
  filename = file_path,
  plot = final_plot,
  width = 18,
  height = 22,
  units = "cm",
  dpi = 300,
  bg = "white"
)


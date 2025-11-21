## Script for plotting ferrybox data loaded from netcdf as tile/hovmoller plots and adding storm date if desired

required_packages <- c("dplyr", "ggplot2", "stringr", "scales","scico","paletteer","gridExtra")
# Install missing packages
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

if (length(args) >= 2) {
  message("Reading CLI args...")
  input_path      <- args[1]  # CSV with ferrybox-data
  out_result_path <- args[2]  # folder
  
  # Optional parameter
  # Optional parameters
  start_date      <- if (length(args) >= 3) as_null_if_blank(args[3]) else NULL # filter for period to plot
  end_date        <- if (length(args) >= 4) as_null_if_blank(args[4]) else NULL # filter for period to plot
  parameters      <- if (length(args) >= 5) as_null_if_blank(args[5]) else NULL # Which parameters to plot 
  lat_min         <- if (length(args) >= 6) as_null_if_blank(args[6]) else NULL # Latitude range to plot
  lat_max         <- if (length(args) >= 7) as_null_if_blank(args[7]) else NULL # Latitude range to plot
  storm_date      <- if (length(args) >= 8) as_null_if_blank(args[8]) else NULL # Optional storm date
  
  # Split/define parameter set:
  if (is.na(parameters)) {
    parameters <- "temperature,salinity,oxygen_sat,chlorophyll,turbidity,fdom"
    message("No parameter set passed, using hardcoded set: ", parameters)
  }
  parameters <- strsplit(parameters, "\\s*,\\s*")[[1]]
  # CLI arguments can only be strings, so converting here:
  if (start_date == "null") start_date <- NULL
  if (end_date == "null") end_date <- NULL
  if (lat_min == "null") lat_min <- NULL
  if (lat_max == "null") lat_max <- NULL
  if (storm_date == "null") storm_date <- NULL
  
} else {
  message("No CLI args detected → using defaults...")
  input_path      <- "testresults/ferrybox_testforplot.csv"
  out_result_path <- "data/out/ferrybox_tile.png"
  start_date      <- NULL
  end_date        <- NULL
  parameters      <- c("temperature", "salinity", "oxygen_sat",
                       "chlorophyll", "turbidity", "fdom")
  lat_min <- NULL
  lat_max <- NULL
  storm_date <- NULL
}

# Normalize optional blanks to NULL
start_date <- as_null_if_blank(start_date)
end_date   <- as_null_if_blank(end_date)
lat_min    <- as_null_if_blank(lat_min)
lat_max    <- as_null_if_blank(lat_max)
storm_date    <- as_null_if_blank(storm_date)



# --- read ----------------------------------------------------------------
if (!file.exists(input_path)) {
  stop("Input CSV not found: ", input_path)
}

ferrybox_df <- readr::read_csv(input_path, show_col_types = FALSE)

param_available <- unique(ferrybox_df$parameter)
message("Parameters available in data: ", paste(param_available, collapse = ", "))



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
tile_plot <- function(
    data,
    parameters,
    start_date = NULL,
    end_date   = NULL,
    lat_min = NULL,
    lat_max = NULL,
    storm_date = NULL,
    out_dir = NULL,
    out_name = NULL,
    save_png = TRUE
) {
 
  # --- Validate parameters ---
  invalid_params <- setdiff(parameters, unique(data$parameter))
  if (length(invalid_params) > 0) {
    stop("Invalid parameter(s): ", paste(invalid_params, collapse = ", "),
         "\nAvailable parameters: ", paste(unique(data$parameter), collapse = ", "))
  }
  
  # --- Validate output directory ---
  if (is.null(out_dir)) out_dir <- "tile_plots"
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  
  # --- Convert datetime & apply date filtering ---
  data <- data %>%
    mutate(Date = as.Date(datetime))
  
  if (!is.null(start_date)) {
    start_date <- as.Date(start_date)
    data <- data %>% filter(Date >= start_date)
  }
  
  if (!is.null(end_date)) {
    end_date <- as.Date(end_date)
    data <- data %>% filter(Date <= end_date)
  }
  
  # --- Filter and transform data ---
  df <- data %>%
    filter(parameter %in% parameters,
           latitude >= lat_min,
           latitude <= lat_max) %>%
    mutate(lat_group = round(latitude, 2))
  
  # --- Define color scales ---
  color_scales <- list(
    salinity     = paletteer::scale_fill_paletteer_c("viridis::viridis", direction = -1, name = "PSU"),
    temperature  = scico::scale_fill_scico(palette = "bilbao", direction = -1, name = "°C"),
    oxygen_sat   = paletteer::scale_fill_paletteer_c("viridis::plasma", direction = -1, name = "%"),
    chlorophyll  = scico::scale_fill_scico(palette = "navia", direction = -1, name = "mg/m³"),
    turbidity    = scico::scale_fill_scico(palette = "nuuk", direction = -1, name = "FNU"),
    fdom         = scico::scale_fill_scico(palette = "oslo", direction = -1, name = "mg/m³")
  )
  
  # --- Create a plot per parameter ---
  plot_list <- lapply(parameters, function(p) {
    
    df_p <- df %>% filter(parameter == p)
    
    p_plot <- ggplot(df_p, aes(x = Date, y = lat_group, fill = value)) +
      geom_tile() +
      (color_scales[[p]] %||% scale_fill_viridis_c()) +
      scale_x_date(date_breaks = "1 month", date_labels = "%y-%b") +
      labs(
        title = paste("FerryBox –", str_to_title(p)),
        x = "Date",
        y = "Latitude"
      ) +
      theme_minimal(base_size = 12) +
      theme(
        axis.text.x = element_text(angle = 90, hjust = 1),
        plot.title  = element_text(size = 15, face = "bold", hjust = 0.5)
      )
    
    # Optional storm indicator
    if (!is.null(storm_date)) {
      p_plot <- p_plot +
        geom_vline(xintercept = as.Date(storm_date),
                   linetype = "dashed", color = "red") +
        annotate("text",
                 x = as.Date(storm_date),
                 y = lat_min,
                 label = "Storm event",
                 angle = 90,
                 vjust = -0.5,
                 size = 3,
                 color = "red")
    }
    
    p_plot
  })
  
  # --- Combine or return single plot ---
  final_plot <- if (length(plot_list) == 1) {
    plot_list[[1]]
  } else {
    do.call(gridExtra::grid.arrange, c(plot_list, ncol = 1))
  }
  
  # --- Save PNG if requested ---
  if (isTRUE(save_png)) {
    
    # Create filename if missing
    if (is.null(out_name) || out_name == "") {
      param_tag <- gsub("[^A-Za-z0-9_-]+", "-", paste(parameters, collapse = "_"))
      stamp     <- format(Sys.time(), "%Y%m%d_%H%M%S")
      out_name  <- sprintf("ferrybox_tile_%s_%s.png", param_tag, stamp)
    }
    
    file_path <- file.path(out_dir, out_name)
    
    ggsave(
      filename = file_path,
      plot     = final_plot,
      width    = 16,
      height   = 12,
      units    = "cm",
      dpi      = 300,
      bg       = "white"
    )
    
    message("Tile plot saved as PNG: ", file_path)
    attr(final_plot, "saved_png_path") <- file_path
  }
  
  final_plot
}

## example usage in R
tile_plot(ferrybox_df, parameters = "salinity", lat_min = 58.9, lat_max = 60, save_png = TRUE)

tile_plot(ferrybox_df,
          parameters = "salinity",
          lat_min = 58.9, lat_max = 60,
          start_date = "2023-07-01",
          end_date = "2023-11-01",
          storm_date = "2023-08-08")



tile_plot(ferrybox_df,
          parameters = c("temperature", "salinity"),
          lat_min = 58.9,
          lat_max = 60,
          storm_date = "2023-08-08",
          save_png = FALSE)

        

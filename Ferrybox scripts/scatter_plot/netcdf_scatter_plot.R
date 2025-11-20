### Scatter parameter plot 

# --- Load required packages (installeres typisk via dependencies.R i Docker) --
required_packages <- c("tidyverse", "ggplot2")
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
  parameters <- if (length(args) >= 3) as_null_if_blank(args[3]) else NULL
  
  # Split/define parameter set:
  if (is.na(parameters)) {
    parameters <- "temperature,salinity,oxygen_sat,chlorophyll,turbidity,fdom"
    message("No parameter set passed, using hardcoded set: ", parameters)
  }
  parameters <- strsplit(parameters, "\\s*,\\s*")[[1]]
  
  
} else {
  message("No CLI args detected → using defaults...")
  input_path      <- "testresults/ferrybox_testforplot.csv"
  out_result_path <- "data/out/ferrybox_scatter.png"
  parameters  <- c("temperature", "salinity", "oxygen_sat",
                   "chlorophyll", "turbidity", "fdom") # using default parameters
}


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

# --- Function for scatter plot --------------------------------------------------
scatter_plot <- function(data        = ferrybox_df,
                         parameter_x = NULL,
                         parameter_y = NULL,
                         out_dir = NULL,
                         out_name    = NULL,
                         save_png    = TRUE) {
  
  # Check input parameters
  if (is.null(parameter_x) || is.null(parameter_y)) {
    stop(
      "Specify two parameters to plot.\nAvailable parameters: ",
      paste(unique(data$parameter), collapse = ", ")
    )
  }
  
  # convert to lower case
  parameter_x <- tolower(parameter_x)
  parameter_y <- tolower(parameter_y)
  
  params_available <- unique(tolower(data$parameter))
  missing_params <- setdiff(c(parameter_x, parameter_y), params_available)
  
  if (length(missing_params) > 0) {
    stop(
      "The following parameter(s) are not available in data: ",
      paste(missing_params, collapse = ", "),
      "\nAvailable parameters: ",
      paste(params_available, collapse = ", ")
    )
  }
  
  #Units for specified parameters
  unit_x <- unique(data[data$parameter == parameter_x,]$unit)
  
  unit_y <- unique(data[data$parameter == parameter_y,]$unit)
  
  # convert data to wide format for scatter plot
  data_wide <- data %>%
    dplyr::mutate(parameter = tolower(parameter),
                  datetime = as.Date(datetime)) %>%
    dplyr::filter(parameter %in% c(parameter_x, parameter_y)) %>%
    dplyr::select(datetime, latitude, longitude, parameter, value) %>%
    tidyr::pivot_wider(
      names_from  = parameter,
      values_from = value
    ) %>%
    dplyr::filter(
      !is.na(.data[[parameter_x]]),
      !is.na(.data[[parameter_y]])
    )
  
  if (nrow(data_wide) == 0) {
    stop("No overlapping data for the chosen parameters (after NA filtering).")
  }
  
  # 
  label_x <- parameter_x
  label_y <- parameter_y
  
  # --- create scatterplot --------------------------------------------------------
  p <- ggplot(data_wide, aes(x = .data[[parameter_x]], y = .data[[parameter_y]])) +
    geom_point(alpha = 0.4, size = 0.7) +
    theme_minimal(base_size = 12) +
    labs(
      title = paste("Scatterplot of", label_y, "vs", label_x),
      subtitle = paste("From", min(data_wide$datetime),"to", max(data_wide$datetime), sep = " "),
      x = paste(label_x , "[",unit_x,"]", sep = ""),
      y = paste(label_y , "[",unit_y,"]", sep = "")
    ) +
    theme(
      plot.title    = element_text(size = 16, face = "bold", hjust = 0.5),
      plot.subtitle = element_text(size = 11, hjust = 0.5, margin = margin(b = 8)),
      axis.text     = element_text(size = 10),
      axis.title    = element_text(size = 11),
      plot.margin   = margin(12, 12, 12, 12)
    )
  
  # --- save as PNG ------------------------------------------------------------
  if (isTRUE(save_png)) {
    # filename if nothing is specified
    if (is.null(out_name) || out_name == "") {
      tag_x  <- gsub("[^A-Za-z0-9_-]+", "-", parameter_x)
      tag_y  <- gsub("[^A-Za-z0-9_-]+", "-", parameter_y)
      stamp  <- format(Sys.time(), "%Y%m%d_%H%M%S")
      out_name <- sprintf("ferrybox_scatter_%s_vs_%s_%s.png", tag_y, tag_x, stamp)
    }
    
    if (!dir.exists(out_dir)) {
      dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
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
    
    message("Scatterplot saved as PNG: ", file_path)
    attr(p, "saved_png_path") <- file_path
  }
  
  return(p)
}



plot_obj <- scatter_plot(
  data        = ferrybox_df,
  parameter_x = "salinity",
  parameter_y = "chlorophyll",
  out_dir     = out_dir,
  out_name    = out_name,
  save_png    = TRUE
)

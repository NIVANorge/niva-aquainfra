coord_value_point_plot <- function(data, parameter,
                                   lon_min = NULL, lon_max,
                                   lat_min = NULL, lat_max,
                                   which_plot = NULL,
                                   save_png = NULL) {
  # --- check for 1 parameter ---
  if (length(parameter) != 1) {
    stop("Enter a single parameter")
  }
  
  # Validate input parameter
  if (!parameter %in% unique(data$parameter)) {
    stop("Invalid parameter: ", parameter,
         "\nAvailable parameters are: ", paste(unique(data$parameter), collapse = ", "))
  }
  
  if (is.null(which_plot)) {
    stop("Choose which coordinate (latitude or longitude) you want to plot")
  }
  
  # Filter data
  data_filtered <- data %>%
    filter(parameter == !!parameter,
           longitude >= lon_min,
           longitude <= lon_max,
           latitude  >= lat_min,
           latitude  <= lat_max) %>%
    mutate(parameter_label = paste0(parameter, " (", unit, ")"))
  
  if (nrow(data_filtered) == 0) {
    stop("No data after filtering. Check your input data frame.")
  }
  
  # --- Create plot ---
  if (grepl("latitude", which_plot, ignore.case = TRUE)) {
    p <- ggplot(data_filtered, aes(x = latitude, y = value)) +
      geom_point(alpha = 0.6) +
      labs(
        title = paste("FerryBox –", parameter),
        x = "Latitude",
        y = paste0(unique(data_filtered$unit))
      ) +
      theme_minimal()
  } else if (grepl("longitude", which_plot, ignore.case = TRUE)) {
    p <- ggplot(data_filtered, aes(x = longitude, y = value)) +
      geom_point(alpha = 0.6) +
      labs(
        title = paste("FerryBox –", parameter),
        x = "Longitude",
        y = paste0(unique(data_filtered$unit))
      ) +
      theme_minimal()
  } else {
    stop("which_plot must contain either 'latitude' or 'longitude'")
  }
  
  # --- Save plot as PNG --- #
  should_save <- isTRUE(save_png) || (is.character(save_png) && grepl("^\\s*yes\\s*$", save_png, ignore.case = TRUE))
  if (should_save) {
    # robust folder to Downloads
    get_downloads_path <- function() {
      sys_name <- Sys.info()[["sysname"]]
      if (identical(sys_name, "Windows")) {
        return(file.path(Sys.getenv("USERPROFILE"), "Downloads"))
      } else {
        return(file.path(Sys.getenv("HOME"), "Downloads"))
      }
    }
    downloads_dir <- get_downloads_path()
    
    # creates filename with parameters and timestamp
    param_tag <- paste(parameter, collapse = "_")
    # Characters allowed
    param_tag <- gsub("[^A-Za-z0-9_-]+", "-", param_tag)
    coords_tag <- which_plot
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    file_name <- sprintf("ferrybox_%s_%s_%s.png",coords_tag, param_tag, timestamp)
    file_path <- file.path(downloads_dir, file_name)
    
    # Write file
    ggplot2::ggsave(
      ggsave(p, width=16, height=12, units="cm", dpi=300,
             filename=file_path))
    message("Plot saved as png: ", file_path)
    
    # Returns path
    attr(p, "saved_png_path") <- file_path
  } else {
    message('Wish to save as PNG file, enter save_png = "Yes" or TRUE.')
  }
  
  return(p)
}


coord_value_point_plot(data =df_all, parameter = "temperature", lat_min = 58.9, lat_max = 60,
                       lon_min = 9.90271, lon_max = 11.94657, which_plot = "latitude", save_png = "yes")


param_available <- unique(df_all$parameter)

time_value_point_plot <- function(data, parameter, lon_min = NULL, lon_max = NULL,
                                  lat_min = NULL, lat_max = NULL, save_png) {
  
  # --- check for 1 parameter ---
  if (length(parameter) != 1) {
    stop("Enter a single parameter")
  }
  
  data <- data %>% mutate(parameter = tolower(parameter))
  
  # Validate inputs parameters
  invalid_param <- setdiff(parameter, unique(data$parameter))
  if (length(invalid_param) > 0) {
    stop("Invalid parameter(s): ", paste(invalid_param, collapse = ", "),
         "\nAvailable parameters are: ", paste(param_available, collapse = ", "))
  }
  
  if(is.null(lon_min) & is.null(lon_max) & is.null(lat_min) & is.null(lat_max)) {
    message("No input coordinates - plotting data for entire boundary area")
    lon_min <- min(data$longitude) 
    lon_max <- max(data$longitude)
    lat_min <- min(data$latitude)
    lat_max <- max(data$latitude)
  }
  
  # Filters data for the specified parameter and latitude range. Time has been filtered previously
  data_filtered <- data %>% filter(parameter == !!parameter,
                                   longitude >= lon_min,
                                   longitude <= lon_max,
                                   latitude >= lat_min,
                                   latitude <= lat_max) 
    #mutate(parameter_label = paste0(parameter, " (", unit, ")"))
  
  #Check if there is any data available
  if (nrow(data_filtered) == 0) {
    stop("No data after filtering. Check your input data frame.")
  }
  
  #-------------- Create parameter plot --------------#
   p <- ggplot(data_filtered, aes(x = datetime, y = value)) +
      geom_point(alpha = 0.6) +
      scale_x_date(date_breaks = "1 month", date_labels = "%Y-%m")+
      labs(
        title = paste("FerryBox –", parameter),
        x = "Date",
        y = paste0(unique(data_filtered$unit))
      ) +
      theme_minimal()+
      theme(axis.text.x = element_text(angle = 45))
  

   # --- Save plot as PNG ---------# 
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
     time_min <- format(min(data_filtered$datetime), "%Y%m%d")
     time_max <- format(max(data_filtered$datetime), "%Y%m%d")
     file_name <- sprintf("ferrybox_%s_%s-%s.png", param_tag, time_min,time_max)
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


time_value_point_plot(df_all, parameter = "salinity", save_png = "no")

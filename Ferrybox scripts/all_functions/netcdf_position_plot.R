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
  parameter <- if (length(args) >= 3) as_null_if_blank(args[3]) else NULL

  # Split/define parameter set:
  if (is.na(parameter)) {
    parameter <- "temperature,salinity,oxygen_sat,chlorophyll,turbidity,fdom"
    message("No parameter set passed, using hardcoded set: ", parameter)
  }
  parameter <- strsplit(parameter, "\\s*,\\s*")[[1]]
  
  
} else {
  message("No CLI args detected → using defaults...")
  input_path      <- "testresults/ferrybox_testforplot.csv"
  out_result_path <- "data/out/ferrybox_position.png"
  parameter  <- c("temperature", "salinity", "oxygen_sat",
                       "chlorophyll", "turbidity", "fdom") # using default parameter
}


# --- read ----------------------------------------------------------------
if (!file.exists(input_path)) {
  stop("Input CSV not found: ", input_path)
}

ferrybox_df <- readr::read_csv(input_path, show_col_types = FALSE)

param_available <- unique(ferrybox_df$parameter)
message("parameter available in data: ", paste(param_available, collapse = ", "))
param_available <- unique(ferrybox_df$parameter) |> as.character()

if (is.null(parameter) || !(parameter %in% param_available)) {
  stop("Invalid or missing parameter: ", parameter,
       "\nAvailable parameter are: ", paste(param_available, collapse = ", "))
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
                                   parameter = NULL,
                                   param_available,
                                   world_sf,
                                   out_dir = NULL,
                                   out_name = NULL,
                                   save_png = TRUE) {

  if(is.null(parameter)){
    parameter <- param_available
    message(paste("Plotting position for:"),"\n",paste(param_available, collapse = ", "))
  } else {
    parameter <- parameter
  }
  
  parameter <- tolower(parameter)
  
  # Filter and check data
  data_clean <- data %>%
    filter(!is.na(latitude), !is.na(longitude))
  
  if (nrow(data_clean) == 0) {
    stop("No data available for plotting.")
  }
  
  # Convert to sf
  data_sf <- st_as_sf(
    data_clean,
    coords = c("longitude", "latitude"),
    crs = 4326,
    remove = FALSE
  )
  
  # Bounding box
  bbox_data <- st_bbox(data_sf)
  
  # --- Plot ---
  p <- ggplot() +
    geom_sf(data = world_sf, fill = "grey98", color = "grey80", size = 0.2) +
    geom_path(data = data_clean,
              aes(x = longitude, y = latitude),
              color = "black", linewidth = 0.4, alpha = 0.8) +
    coord_sf(
      xlim = c(bbox_data["xmin"] - 0.5, bbox_data["xmax"] + 0.5),
      ylim = c(bbox_data["ymin"] - 0.5, bbox_data["ymax"] + 0.5),
      expand = FALSE
    ) +
    labs(
      title = "FerryBox route for:",
      subtitle = paste(parameter, collapse = ", "),
      x = "Longitude",
      y = "Latitude"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(size = 12, face = "bold", hjust = 0.5),
      plot.subtitle = element_text(size = 8, hjust = 0.5, margin = margin(b = 10)),
      axis.text = element_text(size = 10),
      axis.title = element_text(size = 11),
      plot.margin = margin(15, 15, 15, 15)
    )
  
  # --- Save file ---
  if (isTRUE(save_png)) {
    param_tag <- gsub("[^A-Za-z0-9_-]+", "-", parameter)
    
    if (is.null(out_name)) {
      stamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
      out_name <- sprintf("ferrybox_position_%s_%s.png", param_tag, stamp)
    }
    
    file_path <- file.path(out_dir, out_name)
    
    ggsave(
      filename = file_path,
      plot = p,
      width = 18,
      height = 22,
      units = "cm",
      dpi = 300,
      bg = "white"
    )
    
    message("Plot saved: ", file_path)
  }
  
  return(p)
}


# --- run function ------------------------------------------------------------
plot_obj <- coord_value_point_plot(
  data     = ferrybox_df,
  parameter = NULL,
  param_available = param_available,
  world_sf  = world,
  out_dir   = out_dir,
  out_name  = out_name,
  save_png  = TRUE
)

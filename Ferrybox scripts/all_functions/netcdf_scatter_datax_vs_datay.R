
library(dplyr)
library(ggplot2)
library(lubridate)
library(sf)

as_null_if_blank <- function(x) {
  if (is.null(x)) return(NULL)
  x <- trimws(x)
  if (!nzchar(x) || tolower(x) == "null") NULL else x
}


`%||%` <- function(x, y) {
  if (is.null(x)) return(y)
  if (length(x) == 0) return(y)
  x
}

scatter_from_joined <- function(
    df_joined,
    waterbodies,          # sf polygons (optional)
    waterbody_ids,        # vector (optional)
    waterbody_id_col,     # column name in waterbodies (optional)
    lat_range,            # c(min, max) required if no waterbodies
    tz = "UTC",
    agg_fun = mean,
    add_lm = TRUE
) {
  stopifnot(is.data.frame(df_joined))
  
  # --- required cols ---
  req <- c("year", "month", "day", "value_x", "value_y")
  miss <- setdiff(req, names(df_joined))
  if (length(miss) > 0) stop("df_joined is missing: ", paste(miss, collapse = ", "))
  
  has_coords <- all(c("latitude", "longitude") %in% names(df_joined))
  
  # --- enforce spatial rule ---
  using_waterbodies <- !is.null(waterbodies) || !is.null(waterbody_ids) || !is.null(waterbody_id_col)
  if (using_waterbodies) {
    if (is.null(waterbodies) || is.null(waterbody_ids) || is.null(waterbody_id_col)) {
      stop("If using waterbodies you must provide: waterbodies, waterbody_ids, waterbody_id_col.")
    }
    if (!inherits(waterbodies, "sf")) stop("waterbodies must be an sf object.")
    if (!(waterbody_id_col %in% names(waterbodies))) stop("waterbody_id_col not found in waterbodies.")
    if (!has_coords) stop("To filter by waterbodies, df_joined must have latitude and longitude.")
  } else {
    if (is.null(lat_range)) stop("Provide lat_range (e.g. c(59.0, 59.3)) when waterbodies is not provided.")
    if (length(lat_range) != 2) stop("lat_range must be length 2.")
    if (!has_coords) stop("To filter by lat_range, df_joined must have latitude and longitude.")
  }
  
  # --- make a date key ---
  df <- df_joined |>
    dplyr::mutate(
      date = as.Date(sprintf("%04d-%02d-%02d", year, month, day)),
      date = as.POSIXct(date, tz = tz)
    )
  
  # --- spatial filter ---
  if (!using_waterbodies) {
    df <- df |>
      dplyr::filter(.data$latitude >= min(lat_range), .data$latitude <= max(lat_range))
    if (nrow(df) == 0) stop("No rows left after lat_range filtering.")
  } else {
    if (!requireNamespace("sf", quietly = TRUE)) stop("Need sf for waterbodies filtering.")
    pts <- sf::st_as_sf(df, coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)
    keep <- sf::st_join(pts, waterbodies[, waterbody_id_col, drop = FALSE], left = FALSE)
    df <- keep |>
      sf::st_drop_geometry() |>
      dplyr::filter(.data[[waterbody_id_col]] %in% waterbody_ids)
    if (nrow(df) == 0) stop("No rows left after waterbody filtering.")
  }
  
  # --- aggregate across space per day (avoids multiple points per day) ---
  daily <- df |>
    dplyr::group_by(.data$date) |>
    dplyr::summarise(
      x_value = agg_fun(.data$value_x, na.rm = TRUE),
      y_value = agg_fun(.data$value_y, na.rm = TRUE),
      n_pairs = sum(is.finite(.data$value_x) & is.finite(.data$value_y)),
      .groups = "drop"
    ) |>
    dplyr::filter(is.finite(.data$x_value), is.finite(.data$y_value))
  
  if (nrow(daily) < 3) stop("Too few points after filtering/aggregation (n = ", nrow(daily), ").")
  
  # --- labels from df_joined if present ---
  px <- if ("parameter_x" %in% names(df_joined)) unique(na.omit(df_joined$parameter_x)) else "x"
  py <- if ("parameter_y" %in% names(df_joined)) unique(na.omit(df_joined$parameter_y)) else "y"
  px <- if (length(px) == 1) px else "x"
  py <- if (length(py) == 1) py else "y"
  
  cor_val <- suppressWarnings(stats::cor(daily$x_value, daily$y_value, use = "complete.obs"))
  
  title_txt <- if (using_waterbodies) {
    paste0("Waterbody vs station (", paste(waterbody_ids, collapse = ", "), ")")
  } else {
    paste0("Transect vs station (lat ", paste(range(lat_range), collapse = "–"), ")")
  }
  
  p <- ggplot2::ggplot(daily, ggplot2::aes(x = x_value, y = y_value)) +
    ggplot2::geom_point() +
    ggplot2::labs(
      x = paste0(px, " (value_x)"),
      y = paste0(py, " (value_y)"),
      title = title_txt,
      subtitle = paste0("n=", nrow(daily), ", cor=", round(cor_val, 3))
    )
  
  if (add_lm) {
    p <- p + ggplot2::geom_smooth(method = "lm", se = TRUE)
  }
  
  list(
    data = daily,
    plot = p,
    stats = list(n = nrow(daily), cor = cor_val)
  )
}



# Args command for reading input
args <- commandArgs(trailingOnly = TRUE)
message("R Command line args: ", paste(args, collapse = " | "))

if (length(args) < 2) stop("Provide source (URL/CSV) and path to save")

input_path  <- args[1] #csv input path with joined dataframe
save_path  <- args[2 ] #path to PNG, can inlude name of file, if not default name is used "scatter.PNG"

waterbodies_path <- if (length(args) >= 3) as_null_if_blank(args[3]) else NULL
waterbody_ids <- if (length(args) >= 4) as_null_if_blank(args[4]) else NULL
waterbody_id_col <- if (length(args) >= 5) as_null_if_blank(args[5]) else NULL
lat_range <- if (length(args) >= 6) as_null_if_blank(args[6]) else NULL # vector e.g c(58.1,58.2)


if (startsWith(input_path, 'http')) {
  message('Input CSV provided as URL')
} else {
  if (!file.exists(input_path)) stop("Input CSV not found: ", input_path)
}

message("Reading input CSV: ", input_path)
df_joined <- readr::read_csv(input_path, show_col_types = FALSE)

waterbody_shp <- if (!is.null(waterbodies_path)) {
  sf::st_read(waterbodies_path, quiet = TRUE)
} else {
  NULL
}


scatter_fb_stat <- scatter_from_joined(
  df_joined = df_joined,
  waterbodies = waterbody_shp,
  waterbody_ids = waterbody_ids,
  waterbody_id_col = waterbody_id_col,
  lat_range = lat_range,
  tz = "UTC",
  agg_fun = mean,
  add_lm = TRUE
)

scatter_fb_stat$plot


# Save PNG
if (grepl("\\.png$", save_path, ignore.case = TRUE)) {
  file_path <- save_path
  dir.create(dirname(file_path), recursive = TRUE, showWarnings = FALSE)
} else {
  dir.create(save_path, recursive = TRUE, showWarnings = FALSE)
  file_path <- file.path(save_path, "scatter.png")
}

message("Saving PNG to: ", file_path)
ggsave(
  filename = file_path,
  plot = scatter_fb_stat$plot,
  width = 18,
  height = 22,
  units = "cm",
  dpi = 300,
  bg = "white"
)

message("Saving PNG... done")

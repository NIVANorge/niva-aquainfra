# dependencies.R
# Ensure pak is installed
if (!requireNamespace("pak", quietly = TRUE)) {
  install.packages("pak", repos = sprintf(
    "https://r-lib.github.io/p/pak/stable/%s/%s/%s",
    .Platform$pkgType, R.Version()$os, R.Version()$arch
  ))
}

# Install required CRAN packages
pak::pkg_install(c(
  "dplyr",
  "tidyverse",
  "tidyr",
  "ncdf4",
  "ggplot2",
  "sf",
  "rnaturalearth",
  "scales",
  "scico",
  "paletteer",
  "gridExtra"
))

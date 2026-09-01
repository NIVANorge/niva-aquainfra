# Hydrochemistry analysis workflow

This tool performs river and coastal hydrochemistry analysis.

The workflow contains three selectable processing steps:

- **Interpolation** – generates daily river chemistry time series by filling gaps in observed water chemistry data.
- **Flux estimation** – calculates daily, monthly, and annual river constituent fluxes using daily chemistry and discharge data.
- **Trend analysis** – performs Mann–Kendall and Sen's slope trend analysis for river and marine time series.

Users can select which processing step or combination of steps to run.

## User parameters

The tool is controlled through command-line parameters. Users do not need to edit the internal JSON configuration files.

### General parameters

| Parameter | Format | Description |
|---|---|---|
| `--steps` | string | Processing step or comma-separated steps to run: `interpolate`, `fluxes`, `trends`. |
| `--output-dir` | path | Directory where the results of the workflow are written. |

### River interpolation and flux parameters

| Parameter | Format | Description |
|---|---|---|
| `--waterchem` | path or URL | Cleaned river water-chemistry NetCDF input. |
| `--discharge` | path or URL | Cleaned daily river-discharge NetCDF input. |
| `--river-name` | string | Name/identifier assigned to the river in the outputs. |
| `--wc-station-name` | string | Station name used in the water-chemistry input file. |
| `--q-station-name` | string | Station name used in the discharge input file. |
| `--latitude` | float | Latitude of the river station in decimal degrees. |
| `--longitude` | float | Longitude of the river station in decimal degrees. |

### Trend-analysis parameters

| Parameter | Format | Description |
|---|---|---|
| `--trend-frequency` | string | Temporal resolution for trend analysis: `monthly`, `annual`, or `seasonal_by_season`. |
| `--mk-mode` | string | Mann-Kendall test mode: `auto`, `original`, or `seasonal`. |
| `--trend-start` | `YYYY-MM-DD` | Optional start date for the trend-analysis period. |
| `--trend-end` | `YYYY-MM-DD` | Optional end date for the trend-analysis period. |

## Input data

The workflow uses NetCDF (`.nc`) input data.

### River interpolation and flux estimation

Two input datasets are required:

| Input | Format | Description |
|---|---|---|
| Water chemistry | NetCDF (`.nc`) or OPeNDAP URL | Cleaned river water-chemistry observations containing time, station information, chemistry variables, and units. |
| Discharge | NetCDF (`.nc`) | Cleaned daily river-discharge time series in m³/s, containing time and station information. |

When both `interpolate` and `fluxes` are selected, the output from the interpolation step is passed internally to the flux-estimation step. The user does not need to provide the interpolated chemistry file separately.
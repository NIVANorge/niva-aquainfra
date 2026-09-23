# Hydrochemistry analysis workflow

This tool performs river and coastal hydrochemistry analysis for the AquaINFRA workflow.

The workflow contains three selectable processing steps:

- **Interpolation** – generates daily river chemistry time series by filling gaps in observed water-chemistry data.
- **Flux estimation** – calculates daily, monthly, and annual river constituent fluxes using daily chemistry and discharge data.
- **Trend analysis** – performs Mann–Kendall trend tests and Sen's slope estimation for river and marine time series.

Users can run one processing step or combine several steps. All user-configurable settings are supplied through command-line parameters. Users do not need to edit the internal JSON configuration files.


## AquaINFRA/Galaxy entry point

For AquaINFRA/Galaxy integration, use `main_aquainfra.py`.

This script is the user-facing command-line interface. It loads the generic templates in `config/aquainfra/` and overrides them with command-line arguments, so users do not need to edit JSON files.

The original/local workflow entry point and its configuration files have been moved to `../hydrochem_trends_river_oslofjord_preprocessing/river_analysis_workflow/`. They are not required for the AquaINFRA/Galaxy interface. Running the relocated entry point requires updating its imports and configuration paths.



## Processing steps

The workflow is run through:

```bash
python main_aquainfra.py --steps <steps> --output-dir <output_directory> [options]
```

`--steps` accepts one or more comma-separated values:

- `interpolate`
- `fluxes`
- `trends`

### Examples

The following examples provide complete commands for running each processing step with the Glomma test data.

### Interpolation

```bash
python main_aquainfra.py \
  --steps interpolate \
  --output-dir output_aquainfra_glomma_test \
  --waterchem "https://thredds.niva.no/thredds/dodsC/datasets/samples/cleaned_riverchem_40356.nc" \
  --discharge "data/processed/river/Q_daily_mean_Glomma_Solbergfoss_2_605_0_cleaned.nc" \
  --river-name "Glomma" \
  --wc-station-name "Glomma, Sarpsfossen" \
  --q-station-name "Solbergfoss" \
  --latitude 59.27980207 \
  --longitude 11.13411158 \
  --variables "DOC,TOTN,TOTP" \
  --variable-units "DOC=mg/l,TOTN=ug/l,TOTP=ug/l"
```

### Flux estimation

This example uses the interpolated water-chemistry file produced by the interpolation step above.

```bash
python main_aquainfra.py \
  --steps fluxes \
  --output-dir output_aquainfra_glomma_test \
  --interpolated-waterchem "output_aquainfra_glomma_test/daily_estimates/data/daily_water_chemistry_modeled_Glomma.nc" \
  --discharge "data/processed/river/Q_daily_mean_Glomma_Solbergfoss_2_605_0_cleaned.nc" \
  --river-name "Glomma" \
  --q-station-name "Solbergfoss" \
  --latitude 59.27980207 \
  --longitude 11.13411158 \
  --variables "DOC,TOTN,TOTP" \
  --variable-units "DOC=mg/l,TOTN=ug/l,TOTP=ug/l"
```

### Trend analysis

This example uses the daily river-flux output together with the marine input dataset.

```bash
python main_aquainfra.py \
  --steps trends \
  --output-dir output_aquainfra_glomma_test \
  --river-flux-dir "output_aquainfra_glomma_test/fluxes/data/daily" \
  --marine-input "https://thredds.niva.no/thredds/dodsC/datasets/samples/oslofjord_cleaned_standardized_data.nc" \
  --variables "DOC,TOTN,TOTP,SST"
```

The full list of available parameters and their descriptions can always be displayed with:

```bash
python main_aquainfra.py --help
```

### Combined workflow with explicit output paths

Run commands from the `hydrochem_trends_river_oslofjord_use_case` directory. The local discharge dataset must be available at the path shown.

The following smoke test was successfully run for Glomma using DOC and annual trend analysis. It uses one validation repeat to reduce runtime; this is a functional test, not a recommended scientific validation setting.

```bash
python main_aquainfra.py --steps interpolate,fluxes,trends --output-dir output_galaxy_test --waterchem "https://thredds.niva.no/thredds/dodsC/datasets/samples/cleaned_riverchem_40356.nc" --discharge "data/processed/river/Q_daily_mean_Glomma_Solbergfoss_2_605_0_cleaned.nc" --river-name "Glomma" --wc-station-name "Glomma, Sarpsfossen" --q-station-name "Solbergfoss" --latitude 59.27980207 --longitude 11.13411158 --variables "DOC" --variable-units "DOC=mg/l" --validation-repeats 1 --trend-frequency annual --interpolation-output "galaxy_results/chemistry.nc" --daily-flux-output "galaxy_results/daily.nc" --monthly-flux-output "galaxy_results/monthly.nc" --annual-flux-output "galaxy_results/annual.nc" --trend-output "galaxy_results/results.xlsx"
```

The five main results are written under `galaxy_results/`; diagnostics and figures are written under `output_galaxy_test/`.

This single-line command works in Bash and PowerShell. The multiline examples above use Bash `\` continuations; in PowerShell, enter those commands on one line or use PowerShell backticks.

## Main user parameters

### General parameters

| Parameter | Format | Description |
|---|---|---|
| `--steps` | string | Processing step or comma-separated steps: `interpolate`, `fluxes`, `trends`. |
| `--output-dir` | path | Directory where workflow outputs are written. |
| `--variables` | comma-separated strings | Variables to process, for example `DOC,TOTN,TOTP`. If omitted, variables from the internal configuration are used. |
| `--variable-units` | comma-separated `variable=unit` pairs | Units for processed variables, for example `DOC=mg/l,TOTN=ug/l,TOTP=ug/l`. |
| `--non-mass-vars` | comma-separated strings | Variables that should not be interpreted as mass fluxes, for example `Color,UV_Abs_254nm,UV_Abs_410nm`. |

## River interpolation parameters

### Input data and station information

| Parameter | Format | Description |
|---|---|---|
| `--waterchem` | path or URL | Cleaned river water-chemistry NetCDF input. |
| `--discharge` | path or URL | Cleaned daily river-discharge NetCDF input. |
| `--river-name` | string | River name/identifier used in the outputs. |
| `--wc-station-name` | string | Station name used in the water-chemistry input. |
| `--q-station-name` | string | Station name used in the discharge input. |
| `--latitude` | float | River station latitude in decimal degrees. |
| `--longitude` | float | River station longitude in decimal degrees. |
| `--wc-time-col` | string | Name of the time coordinate/column in the water-chemistry input. |
| `--q-time-col` | string | Name of the time coordinate/column in the discharge input. |
| `--wc-station-col` | string | Name of the station coordinate/column in the water-chemistry input. |
| `--q-station-col` | string | Name of the station coordinate/column in the discharge input. |
| `--discharge-var` | string | Name of the discharge variable in the discharge input. |

### Interpolation and validation settings

| Parameter | Format | Description |
|---|---|---|
| `--linear-max-gap` | integer | Maximum gap length in days for linear interpolation. |
| `--gam-n-splines-q` | integer | Number of GAM splines for discharge. |
| `--gam-n-splines-doy` | integer | Number of GAM splines for day of year. |
| `--validation-repeats` | integer | Number of blocked-holdout validation repeats. |
| `--validation-block-size` | integer | Block size used in holdout validation. |
| `--validation-min-observations` | integer | Minimum number of observations required for validation. |
| `--validation-min-points` | integer | Minimum number of validation points required. |
| `--validation-min-coverage` | float | Minimum validation coverage required for a candidate method. |
| `--selection-min-r2` | float | Minimum R-squared required for a candidate interpolation method. |
| `--selection-r2-tolerance` | float | R-squared tolerance when comparing acceptable methods. |
| `--selection-max-bias-fraction` | float | Maximum allowed absolute relative validation bias. |
| `--selection-extreme-ratio-limit` | float | Limit used when evaluating implausible extreme model predictions. |

If these scientific settings are not supplied, the defaults in the internal AquaINFRA configuration are used.

## Flux-estimation parameters

Flux estimation can either use interpolated chemistry generated during the same run or an existing daily interpolated chemistry file.

| Parameter | Format | Description |
|---|---|---|
| `--interpolated-waterchem` | path | Existing daily interpolated river water-chemistry NetCDF. Required when `fluxes` is run without `interpolate`. |
| `--discharge` | path or URL | Daily river-discharge NetCDF input. |
| `--river-name` | string | River name/identifier. |
| `--q-station-name` | string | Station name used in the discharge input. |
| `--latitude` | float | River station latitude in decimal degrees. |
| `--longitude` | float | River station longitude in decimal degrees. |
| `--flux-date-col` | string | Name of the date coordinate/column in the interpolated chemistry input. |
| `--flux-station-col` | string | Name of the station coordinate/column used in flux processing. |
| `--flux-discharge-col` | string | Name of the discharge variable/column used in flux processing. |

When `interpolate` and `fluxes` are selected together, the interpolation output is passed automatically to the flux-estimation step.

## Trend-analysis parameters

Trend analysis can use river flux data, marine chemistry data, or both.

### Inputs and trend settings

| Parameter | Format | Description |
|---|---|---|
| `--river-flux-dir` | path | Folder containing daily river-flux NetCDF files when trends are run without the flux step. |
| `--marine-input` | path or URL | Marine chemistry NetCDF file or OPeNDAP URL. |
| `--trend-frequency` | string | `config`, `monthly`, `annual`, or `seasonal_by_season`. |
| `--mk-mode` | string | Mann–Kendall mode: `auto`, `original`, or `seasonal`. |
| `--trend-start-year` | integer | Optional first year of the trend-analysis period. |
| `--trend-end-year` | integer | Optional last year of the trend-analysis period. |
| `--trend-require-full-period` / `--no-trend-require-full-period` | flag | Require or do not require the complete requested trend period. |
| `--trend-alpha` | float | Significance level for the Mann–Kendall test. |
| `--trend-min-points-monthly` | integer | Minimum number of points for monthly trend analysis. |
| `--trend-min-points-annual` | integer | Minimum number of points for annual trend analysis. |
| `--trend-min-points-seasonal` | integer | Minimum number of points for seasonal trend analysis. |

### River aggregation settings

River flux values are aggregated by **summation**.

| Parameter | Format | Description |
|---|---|---|
| `--river-min-obs-monthly` | integer | Minimum number of daily river values required to create a monthly value. |
| `--river-min-obs-annual` | integer | Minimum number of daily river values required to create an annual value. |
| `--river-min-obs-seasonal` | integer | Minimum number of daily river values required to create a seasonal value. |

### Marine settings

Marine chemistry values are aggregated using the **median**.

| Parameter | Format | Description |
|---|---|---|
| `--marine-depth-min` | float | Minimum marine sampling depth in metres. |
| `--marine-depth-max` | float | Maximum marine sampling depth in metres. |
| `--marine-min-obs-monthly` | integer | Minimum number of marine observations required to create a monthly value. |
| `--marine-min-obs-annual` | integer | Minimum number of marine observations required to create an annual value. |
| `--marine-min-obs-seasonal` | integer | Minimum number of marine observations required to create a seasonal value. |
| `--marine-require-all-seasons` / `--no-marine-require-all-seasons` | flag | Require or do not require all seasons when creating annual marine values. |
| `--marine-min-seasons-per-year` | integer | Minimum number of seasons required to create an annual marine value. |
| `--marine-station-dim` | string | Name of the station dimension in the marine input. |
| `--marine-station-coord` | string | Name of the station-name coordinate in the marine input. |
| `--marine-depth-dim` | string | Name of the depth dimension/coordinate in the marine input. |

The default AquaINFRA configuration selects marine measurements from 0–10 m depth unless the depth limits are overridden through the command line.

## Dataset metadata

Metadata for generated NetCDF datasets can be supplied through command-line parameters.

| Parameter | Description |
|---|---|
| `--creator-name` | Name of the person or organisation responsible for the dataset. |
| `--creator-email` | Contact email for the dataset creator. |
| `--creator-institution` | Institution responsible for creating the dataset. |
| `--creator-url` | URL identifying the dataset creator. |
| `--data-owner` | Person or organisation owning the dataset. |
| `--institution` | Institution associated with the generated dataset. |
| `--institution-short-name` | Short name or acronym of the institution. |
| `--publisher-name` | Name of the dataset publisher. |
| `--publisher-email` | Contact email for the dataset publisher. |
| `--publisher-institution` | Institution responsible for publishing the dataset. |
| `--publisher-url` | URL of the dataset publisher. |
| `--project` | Project associated with the generated dataset. |
| `--license` | License applying to the generated dataset. |
| `--naming-authority` | Naming authority used for generated dataset identifiers. |
| `--keywords` | Comma-separated keywords describing the generated dataset. |

The internal AquaINFRA JSON files provide generic defaults and are not intended to be edited by users.

## Input data

The workflow uses NetCDF (`.nc`) datasets and, where supported, OPeNDAP URLs.

### River interpolation

Two inputs are required:

| Input | Description |
|---|---|
| Water chemistry | River water-chemistry observations containing time, station information, and the selected chemistry variables. |
| Discharge | Daily river-discharge time series containing time, station information, and discharge. |

### Flux estimation

Flux estimation requires daily interpolated chemistry and daily discharge. If interpolation is run in the same workflow execution, the interpolated chemistry output is used automatically.

### Trend analysis

Trend analysis accepts:

| Input | Description |
|---|---|
| River daily fluxes | Daily river-flux NetCDF files produced by the flux-estimation step. |
| Marine chemistry | Marine chemistry dataset containing station, time, depth, and chemistry variables. |

River and marine trend sources may be analysed together.

## Output data

By default, results are written below the directory specified with `--output-dir`.

The following optional arguments specify exact paths, including filenames, for the main result files:

| Argument | Main output |
|---|---|
| `--interpolation-output` | Daily modelled water-chemistry NetCDF |
| `--daily-flux-output` | Daily flux NetCDF |
| `--monthly-flux-output` | Monthly flux NetCDF |
| `--annual-flux-output` | Annual flux NetCDF |
| `--trend-output` | Combined trend Excel workbook |

Relative paths supplied through these five arguments are resolved from the current working directory. Parent directories are created automatically.

Each selected main output is written directly to its specified path. Omitted arguments retain the default output paths and filenames. Diagnostics and figures remain under `--output-dir`.

When steps are combined, downstream steps automatically use the actual output paths returned by the preceding steps.

Use a distinct path for each output, separate from all input files. Output-path arguments apply only when the corresponding processing step is selected.

### Interpolation

For each river, interpolation produces:

| Output | Format | Description |
|---|---|---|
| Daily modelled water chemistry | NetCDF (`.nc`) | Daily chemistry time series with observations preserved and missing values filled using the selected interpolation and fallback methods. |
| Validation metrics | CSV (`.csv`) | Validation statistics for candidate interpolation methods. |
| Final-series completeness | CSV (`.csv`) | Completeness information for the final daily chemistry series. |
| All-method figures | PNG (`.png`) | Comparison of candidate interpolation methods. |
| Selected-method figures | PNG (`.png`) | Observations and final selected daily estimates. |
| Validation figures | PNG (`.png`) | Withheld observations compared with model predictions. |

The main NetCDF output follows the naming convention:

```text
daily_water_chemistry_modeled_<river>.nc
```

### Flux estimation

For each river, flux estimation produces:

| Output | Format | Description |
|---|---|---|
| Daily fluxes | NetCDF (`.nc`) | Daily constituent flux estimates. |
| Monthly fluxes | NetCDF (`.nc`) | Monthly values aggregated from daily estimates. |
| Annual fluxes | NetCDF (`.nc`) | Annual values aggregated from daily estimates. |
| Flux figures | PNG (`.png`) | Diagnostic daily, monthly, and annual time-series figures. |

The NetCDF outputs follow the naming conventions:

```text
daily_water_chemistry_fluxes_<river>.nc
monthly_water_chemistry_fluxes_<river>.nc
annual_water_chemistry_fluxes_<river>.nc
```

### Trend analysis

Trend analysis produces:

| Output | Format | Description |
|---|---|---|
| Combined trend results | Excel (`.xlsx`) | Mann–Kendall results and Sen's slope estimates. |
| Station trend figures | PNG (`.png`) | Trend results for individual river or marine stations. |
| Trend-matrix figures | PNG (`.png`) | Summary figures comparing trends among stations. |

The main tabular output is:

```text
mk_results.xlsx
```

Figures are organised by temporal frequency. Seasonal analyses additionally produce results for individual seasons.

## Python dependencies

The required Python packages are listed in `requirements.txt`:

```text
numpy
pandas
xarray
matplotlib
scipy
scikit-learn
pygam
pymannkendall
netCDF4
openpyxl
```

Install them with:

```bash
pip install -r requirements.txt
```

## Internal configuration

The AquaINFRA command-line interface uses internal generic configuration templates located under:

```text
config/aquainfra/
```

These files contain workflow defaults. **Users do not need to edit these JSON files.**

Command-line parameters supplied to `main_aquainfra.py` override the corresponding internal configuration values.

## Authors and contact

Developed by Areti Balkoni and Leah Jackson-Blake for the AquaINFRA hydrochemistry workflow.

For technical issues, please use the repository's GitHub Issues page.
For other questions, contact: areti.balkoni@niva.no
import math
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

plt.style.use("ggplot")


def load_netcdf_to_dataframe(filepath, time_vars=("sample_date", "time")):
    """
    Loads a list of NetCDF files into pandas DataFrames.

    Parameters:
        filepath: Path to NetCDF files.
        time_vars: Column names to convert to datetime.

    Returns:
        list of tuples: Each tuple contains the filename and its corresponding DataFrame.
    """
    ds = xr.open_dataset(filepath)
    df = ds.to_dataframe().reset_index()

    for time_var in time_vars:
        if time_var in df.columns:
            df[time_var] = pd.to_datetime(df[time_var], errors="coerce")


    return df


def process_river_df(
    df,
    time_col_name,
    station_rename_map,
    date_col="date",
    station_col="station_name",
    standard_station_col="river_name",
):
    """
    Processes a DataFrame by:
    - Renaming the time column to a consistent name
    - Converting the date column to datetime
    - Normalizing timestamps to keep only the date part
    - Renaming station names using the provided mapping
    - Renaming the station column to a standard name (e.g. 'river_name')

    Parameters:
        df: dataframe.
        time_col_name: Name of the time column to convert and rename.
        station_rename_map: Dict to standardize station names.
        date_col: New name for the time column (default: "date").
        station_col: Name of the original station column (default: "station_name").
        standard_station_col: Final name for the renamed station column (default: "river_name").

    Returns:
        List of tuples: (filename, processed DataFrame)
    """
    if time_col_name in df.columns:
        df = df.rename(columns={time_col_name: date_col})
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df[date_col] = df[date_col].dt.normalize()

    if station_col in df.columns:
        df[station_col] = df[station_col].replace(station_rename_map)
        df = df.rename(columns={station_col: standard_station_col})

    return df


def merge_daily_river_data(
    wc_df, q_df, station_name, date_col="date", discharge_col="discharge", drop_cols=False
):
    """
    Merge water chemistry and discharge data per river into daily time series,
    handling duplicates by aggregating (mean over duplicate days).

    Parameters:
        wc_df: df for water chemistry.
        q_df: df for discharge.
        station_name: Station name.
        date_col: Name of the date column to align on (default: 'date').
        discharge_col: Name of the discharge column (default: 'discharge').
        drop_cols: List of column names to drop from water chemistry data before merging.
                   Set to False to skip dropping any columns.

    Returns:
        List of (station_name, merged DataFrame) tuples.
    """
    q_df = q_df.drop_duplicates(subset=date_col)

    full_dates = pd.date_range(start=q_df[date_col].min(), end=q_df[date_col].max(), freq='D')
    full_df = pd.DataFrame({date_col: full_dates})

    merged = pd.merge(full_df, q_df[[date_col, discharge_col]], on=date_col, how='left')

    if isinstance(drop_cols, list):
        wc_df = wc_df.drop(columns=drop_cols, errors='ignore')

    merged = pd.merge(merged, wc_df, on=date_col, how='left')

    numeric_cols = merged.select_dtypes(include='number').columns.tolist()
    agg_df = merged.groupby(date_col)[numeric_cols].mean().reset_index()

    columns_order = [date_col, discharge_col] + [
        col for col in agg_df.columns if col not in [date_col, discharge_col]
    ]
    agg_df = agg_df[columns_order]

    return agg_df


def extract_units_from_netcdf(netcdf_path, skip_vars=["date", "latitude", "longitude", "river_name"]):
    """
    Extracts units from a NetCDF file and returns a DataFrame like pars_metadata_df.
    """
    ds = xr.open_dataset(netcdf_path)
    metadata = []

    for var in ds.data_vars:
        if var in skip_vars:
            continue
        unit = ds[var].attrs.get("units", "unknown")
        metadata.append({"parameter_name": var, "unit": unit})

    return pd.DataFrame(metadata)


def compute_fluxes(df, param_unit_map, discharge_col="discharge", keep_cols=["date"]):
    """
    Compute daily fluxes for a river dataframe.

    - Concentration units are handled based on `param_unit_map`.
    - Discharge must be in m³/s (converted internally).
    - Output includes flux variables, date, and river name.

    Parameters:
        df: DataFrame with daily data (must include discharge and concentration variables)
        param_unit_map: dict mapping variable names to their units
        discharge_col: name of the discharge column
        keep_cols: additional columns to retain (like date, river_name)

    Returns:
        DataFrame with fluxes and metadata columns.
    """
    q_m3_per_day = df[discharge_col] * 86400  # m³/s to m³/day
    flux_df = pd.DataFrame(index=df.index)

    for var in df.columns:
        if var in keep_cols or var == discharge_col:
            continue
        if var not in param_unit_map:
            print(f"Skipping {var}: unit not found in metadata.")
            continue

        unit = param_unit_map[var]

        # Convert to kg/m³
        if unit.endswith(("mg/l", "mg/l C", "mg Pt/l")):
            conc_kg_m3 = df[var] * 1e-3
        elif unit.endswith(("µg/l", "μg/l", "µg/l P")):
            conc_kg_m3 = df[var] * 1e-6
        elif unit.endswith("Abs/cm"):
            conc_kg_m3 = df[var]  # use raw values -> they’re not in tonnes/day!!!!!!
        else:
            print(f"Unknown unit for {var}: {unit}. Skipping.")
            continue

        flux = conc_kg_m3 * q_m3_per_day / 1000  # tonnes/day or Abs·m³/day
        flux_df[var] = flux

        # Add metadata columns
    for col in keep_cols:
        flux_df[col] = df[col]

    return flux_df


def plot_daily_fluxes(
    df,
    station_name,
    variables_to_plot=None,
    cols=3,
    figsize=(15, 12),
    non_mass_flux_vars=None,
    save_path=None,
):
    """
    Plot daily fluxes for a specific river station using line plots.

    Parameters:
        df: df with daily flux data.
        station_name: Name of the station to plot.
        variables_to_plot: Specific variable names to include. If None, plots all available.
        cols: Number of columns in the subplot grid.
        figsize: Overall figure size in inches (width, height).
        non_mass_flux_vars: List of variable names that should not be labeled as "tonnes/day".
        save_path: If provided, saves the figure to this file path.

    Returns:
        None. Saves and/or displays a matplotlib figure.
    """
    if non_mass_flux_vars is None:
        non_mass_flux_vars = ["Color", "UV_Abs_254nm", "UV_Abs_410nm"]

    df = df.copy().sort_values("date")

    flux_vars = df.select_dtypes(include='number').columns.difference(["discharge"])
    if variables_to_plot:
        flux_vars = [v for v in variables_to_plot if v in flux_vars]
    else:
        flux_vars = [v for v in flux_vars if not df[v].isna().all()]

    rows = math.ceil(len(flux_vars) / cols)
    fig_height = 3 * rows
    fig_width = 5 * cols

    fig, axes = plt.subplots(rows, cols, figsize=(fig_width, fig_height), sharex=False)
    axes = axes.flatten()

    for i, var in enumerate(flux_vars):
        axes[i].plot(df["date"], df[var])
        axes[i].set_title(var)
        axes[i].tick_params(axis='x', labelrotation=45)

        if var not in non_mass_flux_vars:
            axes[i].set_ylabel("tonnes/day")

        axes[i].grid(True)

    for j in range(len(flux_vars), len(axes)):
        axes[j].axis('off')

    fig.suptitle(f"{station_name} – Daily Fluxes", fontsize=16)
    fig.tight_layout(rect=[0, 0, 1, 0.96])

    if save_path:
        plt.savefig(save_path, dpi=300)
        print(f"Saved plot to: {save_path}")

    plt.show()


def plot_monthly_fluxes(
    df,
    station_name,
    variables_to_plot=None,
    cols=3,
    figsize=(15, 12),
    non_mass_flux_vars=None,
    save_path=None,
):
    """
    Plot monthly fluxes for a specific river station using line plots.

    Parameters:
        df: df with monthly flux data.
        station_name: Name of the station to plot.
        variables_to_plot: Specific variable names to include. If None, plots all available.
        cols: Number of columns in the subplot grid.
        figsize: Overall figure size in inches (width, height).
        non_mass_flux_vars: List of variables that should not be labeled as "tonnes/month".
        save_path: If provided, saves the figure to this file path.

    Returns:
        None. Saves and/or displays a matplotlib figure.
    """
    if non_mass_flux_vars is None:
        non_mass_flux_vars = ["Color", "UV_Abs_254nm", "UV_Abs_410nm"]

    df = df.copy().sort_values("date")

    flux_vars = df.select_dtypes(include='number').columns.difference(["discharge"])
    if variables_to_plot:
        flux_vars = [v for v in variables_to_plot if v in flux_vars]
    else:
        flux_vars = [v for v in flux_vars if not df[v].isna().all()]

    rows = math.ceil(len(flux_vars) / cols)
    fig_height = 3 * rows
    fig_width = 5 * cols

    fig, axes = plt.subplots(rows, cols, figsize=(fig_width, fig_height), sharex=False)
    axes = axes.flatten()

    for i, var in enumerate(flux_vars):
        axes[i].plot(df.index, df[var], marker="o")
        axes[i].set_title(var)
        axes[i].tick_params(axis='x', labelrotation=45)

        if var not in non_mass_flux_vars:
            axes[i].set_ylabel("tonnes/month")

        axes[i].grid(True)

    for j in range(len(flux_vars), len(axes)):
        axes[j].axis('off')

    fig.suptitle(f"{station_name} – Monthly Fluxes", fontsize=16)
    fig.tight_layout(rect=[0, 0, 1, 0.96])

    if save_path:
        plt.savefig(save_path, dpi=300)
        print(f"Saved plot to: {save_path}")

    plt.show()


def plot_annual_fluxes(
    df,
    station_name,
    variables_to_plot=None,
    cols=3,
    figsize=(15, 12),
    non_mass_flux_vars=None,
    save_path=None,
):
    """
    Plot annual fluxes for a specific river station using line plots.

    Parameters:
        df: List of (station_name, DataFrame) pairs with annual flux data.
        station_name: Name of the station to plot.
        variables_to_plot: Specific variable names to include. If None, plots all.
        cols: Number of columns in the subplot grid.
        figsize: Figure size (width, height) in inches.
        non_mass_flux_vars: List of variable names that should NOT be labeled as "tonnes/year".
        save_path: If provided, saves the figure to this file path.

    Returns:
        None. Saves and/or displays a matplotlib figure.
    """
    # Default non-mass-based variables
    if non_mass_flux_vars is None:
        non_mass_flux_vars = ["Color", "UV_Abs_254nm", "UV_Abs_410nm"]

    df = df.copy().sort_values("year")

    flux_vars = df.select_dtypes(include='number').columns.difference(["year", "discharge"])
    if variables_to_plot:
        flux_vars = [v for v in variables_to_plot if v in flux_vars]
    else:
        flux_vars = [v for v in flux_vars if not df[v].isna().all()]

    rows = math.ceil(len(flux_vars) / cols)
    fig_height = 3 * rows
    fig_width = 5 * cols

    fig, axes = plt.subplots(rows, cols, figsize=(fig_width, fig_height), sharex=False)
    axes = axes.flatten()

    for i, var in enumerate(flux_vars):
        axes[i].plot(df["year"], df[var], marker="o")
        axes[i].set_title(var)
        axes[i].tick_params(axis='x', labelrotation=45)

        if var not in non_mass_flux_vars:
            axes[i].set_ylabel("tonnes/year")

        valid_years = df.loc[df[var].notna(), "year"]
        axes[i].set_xticks(valid_years)
        axes[i].grid(True)

    for j in range(len(flux_vars), len(axes)):
        axes[j].axis('off')

    fig.suptitle(f"{station_name} – Annual Fluxes", fontsize=16)
    fig.tight_layout(rect=[0, 0, 1, 0.96])

    if save_path:
        plt.savefig(save_path, dpi=300)
        print(f"Saved plot to: {save_path}")

    plt.show()


def save_flux_datasets_as_netcdf(
    df,
    station_name,
    output_dir,
    frequency_label,
    river_coords,
    flux_metadata_df,
    standard_name_map,
    var_comments,
    processed_namespace_uuid,
    global_metadata_config,
):
    """
    Save water chemistry flux estimates as NetCDF files, including metadata and CF-compliant structure.

    Parameters:
        df: df containing flux values and metadata columns.
            The DataFrame must have a datetime index (daily, monthly, or annual resolution).
        station_name: Name of the station to plot.
        output_dir: Directory where NetCDF files will be saved (Path or str).
        frequency_label: Frequency label (e.g., "daily", "monthly", "annual").
        river_coords: Dict containing {"lat": float, "lon": float}.
        flux_metadata_df: DataFrame with columns ['parameter_name', 'unit'].
        standard_name_map: Dict mapping variable names to descriptive names.
        var_comments: Dict of comments per variable and station: {var: {station: comment}}.
        processed_namespace_uuid: UUID namespace used for generating persistent dataset UUIDs.
        global_metadata_config: Dict of global metadata following ACDD/CF conventions.

    Returns:
        None. Saves NetCDF files to disk.
    """

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = df.copy()

    # Ensure datetime index
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
        else:
            raise ValueError(f"DataFrame for {station_name} has no datetime index or 'date' column.")

    df.index.name = "date"  # <-- Set name explicitly

    ds = xr.Dataset.from_dataframe(df)
    ds = ds.assign_coords(date=("date", df.index))  # <-- Use 'date' coordinate
    ds["date"].attrs.update({"standard_name": "time", "long_name": "Time of measurement", "axis": "T"})

    # Add lat/lon if available
    if river_coords is not None:
        ds = ds.assign_coords(
            latitude=xr.DataArray(
                river_coords["lat"],
                dims=(),
                attrs={"standard_name": "latitude", "long_name": "Latitude", "units": "degree_north"},
            ),
            longitude=xr.DataArray(
                river_coords["lon"],
                dims=(),
                attrs={"standard_name": "longitude", "long_name": "Longitude", "units": "degree_east"},
            ),
        )
        ds = ds.set_coords(["latitude", "longitude"])

    # River name
    ds["river_name"] = xr.DataArray(station_name, dims=(), attrs={"cf_role": "timeseries_id"})

    # Add variable metadata
    for var in ds.data_vars:
        if var in ["date", "latitude", "longitude", "river_name"]:
            continue
        if var in flux_metadata_df["parameter_name"].values:
            match = flux_metadata_df[flux_metadata_df["parameter_name"] == var].iloc[0]
            ds[var].attrs["units"] = match["unit"]
            ds[var].attrs["long_name"] = standard_name_map.get(var, var)
        if var_comments.get(var) and station_name in var_comments[var]:
            ds[var].attrs["comment"] = var_comments[var][station_name]

    # Metadata
    unique_id = f"no.niva:{uuid.uuid5(processed_namespace_uuid, station_name)}"
    lat = float(ds.latitude.values.item()) if "latitude" in ds.coords else np.nan
    lon = float(ds.longitude.values.item()) if "longitude" in ds.coords else np.nan

    dataset_metadata = global_metadata_config.copy()
    dataset_metadata.update(
        {
            "id": unique_id,
            "title": f"{frequency_label.capitalize()} water chemistry fluxes for river {station_name}",
            "title_no": f"Estimerte {frequency_label} flukser av vannkjemi for elv {station_name}",
            "summary": f"{frequency_label.capitalize()} time series of water chemistry for river {station_name}, estimated by harmonizing observed data and interpolating missing values.",
            "summary_no": f"{frequency_label.capitalize()} tidsserier for vannkjemi ved elv {station_name}, beregnet ved å harmonisere observerte målinger og interpolere manglende verdier.",
            "date_created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "time_coverage_start": str(ds.date.min().values),
            "time_coverage_end": str(ds.date.max().values),
            "geospatial_lat_min": lat,
            "geospatial_lat_max": lat,
            "geospatial_lon_min": lon,
            "geospatial_lon_max": lon,
        }
    )

    if frequency_label == "daily":
        dataset_metadata["processing_level"] = (
            "Estimated daily mass fluxes based on preprocessed water chemistry and discharge time series."
        )
        dataset_metadata["history"] = (
            "Fluxes were estimated using harmonized daily water chemistry concentrations and discharge data. Concentration units were converted to mass per volume where applicable, and fluxes were calculated as mass transport per day."
        )
    else:
        dataset_metadata["processing_level"] = "Aggregated fluxes derived from daily estimates."
        dataset_metadata["history"] = (
            f"Daily fluxes were estimated using water chemistry and discharge data, then aggregated to {frequency_label} totals."
        )

    ds.attrs = {k: str(v) for k, v in dataset_metadata.items()}

    # Encoding: use 'date', not 'time'
    encoding = {
        "date": {
            "dtype": "int32",
            "_FillValue": None,
            "units": "seconds since 1970-01-01 00:00:00",
        },
        "latitude": {"_FillValue": None},
        "longitude": {"_FillValue": None},
    }

    # Add integer encoding for numeric vars
    for var in ds.data_vars:
        if var in ["date", "latitude", "longitude", "river_name"]:
            continue
        if np.issubdtype(ds[var].dtype, np.number):
            encoding[var] = {"dtype": "int32", "_FillValue": -9999}

    fname = f"{frequency_label}_water_chemistry_fluxes_{station_name.lower().replace(' ', '_')}.nc"
    output_path = output_dir / fname
    ds.to_netcdf(output_path, encoding=encoding, format="NETCDF4")
    print(f"Saved: {output_path}")


def flux(cfg: Dict[str, Any]) -> None:
    """
    TODO: ADD documentation here.
    """

    # Path initialization
    wc_interp_data_path = Path(cfg["wc_interp_data_path"])

    q_cleaned_data_path = Path(cfg["q_cleaned_data_path"])

    flux_metadata_df = pd.DataFrame(cfg["flux_metadata"]) ###

    river = cfg["river"]

    processed_namespace_uuid = uuid.UUID(cfg["processed_namespace_uuid"])

    plots_output_dir = Path(cfg["plots_output_dir"])
    plots_output_dir.mkdir(parents=True, exist_ok=True)

    output_dir = Path(cfg["output_dir"])
    os.makedirs(output_dir, exist_ok=True)

    # Load data

    wc_df = load_netcdf_to_dataframe(wc_interp_data_path, time_vars=("date", "time"))
    q_df = load_netcdf_to_dataframe(q_cleaned_data_path, time_vars=("date", "time"))

    # Data preparation

    # Process both types of data
    q_df = process_river_df(
        q_df,
        time_col_name="time",
        station_rename_map=cfg["q_station_rename_map"],
        date_col=cfg["date_col"],
        station_col="station_name",
        standard_station_col=cfg["station_col"],
    )

    wc_df = process_river_df(
        wc_df,
        time_col_name="sample_date",
        station_rename_map=cfg["q_station_rename_map"],
        date_col=cfg["date_col"],
        station_col="station_name",
        standard_station_col=cfg["station_col"],
    )
    # Merge data for easier processing
    merged_df = merge_daily_river_data(
        wc_df, q_df, river, cfg["date_col"], cfg["discharge_col"], drop_cols=cfg["columns_to_drop"]
    )

    # Estimate fluxes

    # daily fluxes

    # Extract parameter units from netCDF
    param_meta_df = extract_units_from_netcdf(wc_interp_data_path)
    param_unit_map = dict(zip(param_meta_df["parameter_name"], param_meta_df["unit"]))

    # Compute fluxes
    df_with_fluxes = compute_fluxes(merged_df, param_unit_map, discharge_col=cfg["discharge_col"])
    print(df_with_fluxes)

    # monthly and annual

    # Ensure 'date' is datetime and set as index
    daily_df = df_with_fluxes.copy()
    daily_df["date"] = pd.to_datetime(daily_df["date"])
    daily_df = daily_df.set_index("date")

    # Monthly flux: require at least 25 valid daily values
    monthly_df = daily_df.resample("M").sum(min_count=25)
    monthly_df["month"] = monthly_df.index.to_period("M").astype(str)

    # Annual flux: require at least 350 valid daily values
    annual_df = daily_df.resample("Y").sum(min_count=350)
    annual_df["year"] = annual_df.index.year

    # Plot
    save_path = plots_output_dir / f"{river.lower()}_daily_fluxes.png"
    plot_daily_fluxes(df=daily_df, station_name=river, save_path=save_path)
    assert False

    save_path = plots_output_dir / f"{river.lower()}_monthly_fluxes.png"
    plot_monthly_fluxes(df=monthly_df, station_name=river, save_path=save_path)

    save_path = plots_output_dir / f"{river.lower()}_annual_fluxes.png"
    plot_annual_fluxes(df=annual_df, station_name=river, save_path=save_path)

    flux_groups = {"daily": daily_df, "monthly": monthly_df, "annual": annual_df}

    # Create datasets and assign metadata and global attributes
    for label, results in flux_groups.items():
        save_flux_datasets_as_netcdf(
            df=results,
            station_name=river,
            output_dir=os.path.join(output_dir, label),
            frequency_label=label,
            river_coords=cfg["river_coords"],
            flux_metadata_df=flux_metadata_df,
            standard_name_map=cfg["standard_name_map"],
            var_comments=" ".join(cfg["var_comments"]),
            processed_namespace_uuid=processed_namespace_uuid,
            global_metadata_config=cfg["global_metadata_config"],
        )

if __name__ == "__main__":
    import json
    with open("flux_config.json","r") as f:
        cfg = json.load(f)
    flux(cfg)
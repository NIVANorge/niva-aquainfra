from __future__ import annotations

import math
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

from src.export_netcdf import export_dataset
# from src.utils import ensure_dirs
from src.utils import (
    ensure_dirs,
    resolve_path,
    netcdf_to_dataframe,
    standardize_time_and_station,
    merge_daily_discharge_and_chemistry,
    save_or_show_plot
)

plt.style.use("ggplot")

# ----------------------------- utils ---------------------------------------
def meta_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return (cfg.get("metadata") or {}).copy()

def render_template(s: Optional[str], ctx: Dict[str, Any]) -> Optional[str]:
    if not s:
        return None
    return s.format(**ctx)

def build_global_attrs_for_flux(
    cfg: Dict[str, Any],
    *,
    station_id: str,
    frequency: str,
) -> Dict[str, str]:
    """
    Build global attributes
    """
    md = meta_cfg(cfg)
    md_tpl = (md.get("templates") or {})
    md_defaults = (md.get("defaults") or {})
    md_timestamps = (md.get("timestamps") or {})

    ctx = {
        "station_id": station_id,
        "frequency": frequency,
        "frequency_cap": frequency.capitalize(),
    }

    base = dict(cfg.get("global_metadata_config", {}) or {})

    for k, v in md_defaults.items():
        base.setdefault(k, v)

    for k in ["title", "title_no", "summary", "summary_no"]:
        if k not in base:
            rendered = render_template(md_tpl.get(k), ctx)
            if rendered:
                base[k] = rendered

    # If user sets date_created, keep it. Otherwise exporter will auto-fill.
    if "date_created" not in base and md_timestamps.get("date_created"):
        base["date_created"] = md_timestamps["date_created"]

    return {k: str(v) for k, v in base.items()}

# ----------------------------- metadata helpers -----------------------------
def extract_units_from_netcdf(netcdf_path: Path, skip_vars=("date", "latitude", "longitude", "river_name")) -> pd.DataFrame:
    """ Read variable units from the *modeled/interpolated* chemistry NetCDF. """

    with xr.open_dataset(netcdf_path) as ds:
        metadata = []
        for var in ds.data_vars:
            if var in skip_vars:
                continue
            unit = ds[var].attrs.get("units", "unknown")
            metadata.append({"parameter_name": var, "unit": unit})
    return pd.DataFrame(metadata)

# ----------------------------- flux computation -----------------------------
def compute_fluxes(
    df: pd.DataFrame,
    *,
    param_unit_map: Dict[str, str],
    discharge_col: str = "discharge",
    keep_cols: list[str] = None,
) -> pd.DataFrame:
    """
    Convert concentration time series + discharge into daily fluxes.

    Assumptions:
    - discharge is in m3/s
    - concentrations are in mg/L or µg/L (or Abs/cm for proxy variables)
    - output is tonnes/day (except for proxy variables which are kept "as is")

    Any variable with unknown units is silently skipped on purpose.
    """
    if keep_cols is None:
        keep_cols = ["date"]

    df = df.copy()
    q_m3_per_day = df[discharge_col] * 86400.0  # m3/s -> m3/day

    flux_df = pd.DataFrame(index=df.index)

    for var in df.columns:
        if var in keep_cols or var == discharge_col:
            continue
        if var not in param_unit_map:
            continue

        unit = str(param_unit_map[var])

        # Concentration to kg/m3
        if unit.endswith(("mg/l", "mg/l C", "mg Pt/l")):
            conc_kg_m3 = df[var] * 1e-3
        elif unit.endswith(("µg/l", "μg/l", "µg/l P")):
            conc_kg_m3 = df[var] * 1e-6
        elif unit.endswith("Abs/cm"):
            # not mass concentration; keep as-is (proxy)
            conc_kg_m3 = df[var]
        else:
            # unknown unit
            continue

        # tonnes/day (kg/m3 * m3/day = kg/day; /1000 = tonnes/day)
        flux_df[var] = conc_kg_m3 * q_m3_per_day / 1000.0

    for col in keep_cols:
        if col in df.columns:
            flux_df[col] = df[col]

    return flux_df

# ----------------------------- plotting -----------------------------
def plot_flux_grid(
    df: pd.DataFrame,
    station_name: str,
    *,
    x,
    title_suffix: str,
    non_mass_vars: set[str],
    y_mass_label: str,
    save_path: Optional[Path] = None,
) -> None:
    flux_vars = df.select_dtypes(include="number").columns.tolist()
    if "year" in flux_vars:
        flux_vars.remove("year")

    cols = 3
    rows = math.ceil(len(flux_vars) / cols) if flux_vars else 1
    fig, axes = plt.subplots(rows, cols, figsize=(5 * cols, 3 * rows), sharex=False)
    axes = np.array(axes).flatten()

    for i, var in enumerate(flux_vars):
        axes[i].plot(x, df[var], marker="o" if title_suffix != "Daily Fluxes" else None)
        axes[i].set_title(var)
        axes[i].tick_params(axis="x", labelrotation=45)
        if var not in non_mass_vars:
            axes[i].set_ylabel(y_mass_label)
        axes[i].grid(True)

    for j in range(len(flux_vars), len(axes)):
        axes[j].axis("off")

    fig.suptitle(f"{station_name} – {title_suffix}", fontsize=16)
    fig.tight_layout(rect=[0, 0, 1, 0.96])

    save_or_show_plot(save_path=save_path, dpi=300)


# ---------------------------- export via export_dataset ----------------------------
def _flux_unit_for_frequency(
    base_unit: str,
    *,
    frequency: str,
    var_name: str,
    non_mass_vars: set[str],
    undefined_unit_label: str = "undefined",
) -> str:
    """
    Decide the unit string to write to the exported NetCDF.

    For mass variables we output tonnes per aggregation period.
    For proxy variables we keep the original unit (often "undefined" or "Abs/cm").
    """

    base_unit = (base_unit or "").strip()

    # Proxy variables: keep their original unit (or "undefined" if missing)
    if var_name in non_mass_vars:
        return base_unit if base_unit else undefined_unit_label

    # If unit is missing/undefined, don't invent a flux unit
    if base_unit.lower() in {"undefined", "unknown", ""}:
        return base_unit if base_unit else undefined_unit_label

    # Mass flux units depend on aggregation frequency
    if frequency == "daily":
        return "tonnes/day"
    if frequency == "monthly":
        return "tonnes/month"
    if frequency == "annual":
        return "tonnes/year"

    return base_unit

def df_to_dataset(
    df: pd.DataFrame,
    *,
    time_name: str,
    river: str,
    river_coords: Optional[dict],
    flux_metadata_df: pd.DataFrame,
    standard_name_map: Dict[str, str],
    var_comments: Dict[str, Any],
    frequency: str,
    non_mass_vars: set[str],
    undefined_unit_label: str = "undefined",
) -> xr.Dataset:
    """
    Convert a pandas DataFrame (daily/monthly/annual) to an xarray Dataset
    with consistent metadata (units, long_name, comments, coords).
    """

    df = df.copy()

    # Ensure we have a proper datetime index in the Dataset
    if time_name in df.columns:
        df[time_name] = pd.to_datetime(df[time_name])
        df = df.set_index(time_name)
    elif not pd.api.types.is_datetime64_any_dtype(df.index):
        raise ValueError(f"Expected '{time_name}' column or datetime index.")

    df.index.name = time_name

    ds = xr.Dataset.from_dataframe(df)

    # Minimal CF-like time coordinate attributes
    ds[time_name].attrs.update({"standard_name": "time", "long_name": "Time of measurement", "axis": "T"})

    # Add static station coordinates (point geometry)
    if river_coords and ("lat" in river_coords) and ("lon" in river_coords):
        ds = ds.assign_coords(
            latitude=xr.DataArray(float(river_coords["lat"]), dims=(), attrs={"standard_name": "latitude", "units": "degree_north"}),
            longitude=xr.DataArray(float(river_coords["lon"]), dims=(), attrs={"standard_name": "longitude", "units": "degree_east"}),
        ).set_coords(["latitude", "longitude"])

    ds["river_name"] = xr.DataArray(river, dims=(), attrs={"cf_role": "timeseries_id"})

    for var in ds.data_vars:
        if var == "river_name":
            continue

        if not flux_metadata_df.empty and var in set(flux_metadata_df["parameter_name"]):
            base_unit = flux_metadata_df.loc[flux_metadata_df["parameter_name"] == var, "unit"].iloc[0]
        else:
            base_unit = ds[var].attrs.get("units", "undefined")

        ds[var].attrs["units"] = _flux_unit_for_frequency(
            str(base_unit),
            frequency=frequency,
            var_name=var,
            non_mass_vars=non_mass_vars,
            undefined_unit_label=undefined_unit_label,
        )

        ds[var].attrs["long_name"] = str(standard_name_map.get(var, var))

        vc = var_comments.get(var)
        if isinstance(vc, dict):
            com = vc.get(river)
            if com:
                ds[var].attrs["comment"] = str(com)
        elif isinstance(vc, str):
            ds[var].attrs["comment"] = vc

    return ds


# ----------------------------- main -----------------------------
def flux(cfg: Dict[str, Any]) -> list[Path]:
    """
    Flux workflow for river:
        1) read interpolated chemistry + cleaned discharge
        2) harmonize station/time columns
        3) merge to daily series (full date coverage based on Q)
        4) compute daily fluxes
        5) aggregate to monthly/annual
        6) plot quick-look figures
        7) export NetCDFs (daily/monthly/annual)

    Returns list of output NetCDF paths.
    """

    wc_path = resolve_path(cfg["wc_interp_data_path"])
    q_path = resolve_path(cfg["q_cleaned_data_path"])

    plots_output_dir = resolve_path(cfg["plots_output_dir"])
    base_output_dir = resolve_path(cfg["output_dir"])
    ensure_dirs(plots_output_dir, base_output_dir)

    river = cfg["river"]
    station_col = cfg.get("station_col", "river_name")
    date_col = cfg.get("date_col", "date")
    discharge_col = cfg.get("discharge_col", "discharge")

    # Export metadata settings
    # processed_namespace_uuid = uuid.UUID(cfg["processed_namespace_uuid"])
    # global_metadata_config = cfg.get("global_metadata_config", {})
    #
    # Export configuration
    export_cfg = cfg.get("export", {})
    engine = export_cfg.get("engine", "netcdf4")
    nc_format = export_cfg.get("format", "NETCDF4")
    time_enc_cfg = export_cfg.get("time", {})
    time_name = time_enc_cfg.get("name", "date")
    filename_template = export_cfg.get("filename_template", "{frequency}_water_chemistry_fluxes_{station_id_or_stem}.nc")
    # id_prefix = export_cfg.get("id_prefix", "no.niva")

    # Variable metadata from config
    flux_metadata_df = pd.DataFrame(cfg.get("flux_metadata", {}))
    standard_name_map = cfg.get("standard_name_map", {})
    var_comments = cfg.get("var_comments", {})
    river_coords = cfg.get("river_coords")

    unit_opt = cfg.get("unit_options", {})
    non_mass_vars = set(unit_opt.get("non_mass_vars", []))
    undefined_unit_label = str(unit_opt.get("undefined_unit_label", "undefined"))

    wc_df_raw = netcdf_to_dataframe(wc_path, time_vars=("date", "sample_date", "time"))
    q_df_raw = netcdf_to_dataframe(q_path, time_vars=("date", "sample_date", "time"))

    q_df = standardize_time_and_station(
        q_df_raw,
        time_col_in="time",
        date_col_out=date_col,
        station_col_in="station_name",
        station_col_out=station_col,
        station_rename_map=cfg.get("q_station_rename_map", {}),
    )

    wc_time_guess = "date" if "date" in wc_df_raw.columns else "sample_date"
    wc_df = standardize_time_and_station(
        wc_df_raw,
        time_col_in=wc_time_guess,
        date_col_out=date_col,
        station_col_in="station_name",
        station_col_out=station_col,
        station_rename_map=cfg.get("q_station_rename_map", {}),
    )

    # Merge
    merged = merge_daily_discharge_and_chemistry(
        wc_df, q_df,
        station_name=river,
        station_col=station_col,
        date_col=date_col,
        discharge_col=discharge_col,
        drop_wc_cols=cfg.get("columns_to_drop", False),
    )

    param_meta_df = extract_units_from_netcdf(wc_path)
    param_unit_map = dict(zip(param_meta_df["parameter_name"], param_meta_df["unit"]))

    daily_flux = compute_fluxes(
        merged,
        param_unit_map=param_unit_map,
        discharge_col=discharge_col,
        keep_cols=[date_col],
    )
    daily_flux[date_col] = pd.to_datetime(daily_flux[date_col])

    # Monthly + annual aggregation
    daily_idx = daily_flux.set_index(date_col)

    # Do not aggregate discharge as a "flux"
    if discharge_col in daily_idx.columns:
        daily_idx_no_q = daily_idx.drop(columns=[discharge_col])
    else:
        daily_idx_no_q = daily_idx

    monthly_flux = daily_idx_no_q.resample("ME").sum(min_count=25)
    annual_flux = daily_idx_no_q.resample("YE").sum(min_count=350)

    # Annual needs year column for plotting
    annual_plot = annual_flux.copy()
    annual_plot["year"] = annual_plot.index.year

    daily_sorted = daily_flux.copy().sort_values(date_col)
    plot_flux_grid(
        daily_sorted,
        river,
        x=daily_sorted[date_col],
        title_suffix="Daily Fluxes",
        non_mass_vars=non_mass_vars,
        y_mass_label="tonnes/day",
        save_path=plots_output_dir / f"{river.lower()}_daily_fluxes.png",
    )

    plot_flux_grid(
        monthly_flux,
        river,
        x=monthly_flux.index,
        title_suffix="Monthly Fluxes",
        non_mass_vars=non_mass_vars,
        y_mass_label="tonnes/month",
        save_path=plots_output_dir / f"{river.lower()}_monthly_fluxes.png",
    )

    annual_plot = annual_flux.copy()
    annual_plot["year"] = annual_plot.index.year
    plot_flux_grid(
        annual_plot,
        river,
        x=annual_plot["year"],
        title_suffix="Annual Fluxes",
        non_mass_vars=non_mass_vars,
        y_mass_label="tonnes/year",
        save_path=plots_output_dir / f"{river.lower()}_annual_fluxes.png",
    )

    outputs: list[Path] = []

    for frequency, df_freq in [
        ("daily", daily_flux.rename(columns={date_col: time_name})),
        ("monthly", monthly_flux.reset_index().rename(columns={date_col: time_name})),
        ("annual", annual_flux.reset_index().rename(columns={date_col: time_name})),
    ]:
        ds = df_to_dataset(
            df_freq,
            time_name=time_name,
            river=river,
            river_coords=river_coords,
            flux_metadata_df=flux_metadata_df,
            standard_name_map=standard_name_map,
            var_comments=var_comments,
            frequency=frequency,
            non_mass_vars=non_mass_vars,
            undefined_unit_label=undefined_unit_label,
        )

        gmeta = build_global_attrs_for_flux(cfg, station_id=river, frequency=frequency)
        # gmeta = dict(global_metadata_config)
        # gmeta.setdefault("title", f"{frequency.capitalize()} water chemistry fluxes for river {river}")
        # gmeta.setdefault("summary", f"{frequency.capitalize()} fluxes derived from daily concentration estimates and discharge.")

        md = meta_cfg(cfg)
        md_id = (md.get("id") or {})

        namespace_uuid = md_id.get("namespace_uuid") or cfg.get("processed_namespace_uuid")
        id_prefix = md_id.get("prefix") or export_cfg.get("id_prefix", "no.niva")

        seed_template = md_id.get("seed_template", "{station_id}:{frequency}")
        id_seed = render_template(seed_template,
                                  {"station_id": river, "frequency": frequency}) or f"{river}:{frequency}"

        filename = filename_template.format(
            frequency=frequency,
            station_id_or_stem=river.lower().replace(" ", "_"),
            station_id=river,
        )

        out_path = export_dataset(
            ds=ds,
            output_dir=(base_output_dir / frequency),
            filename=filename,
            time_name=time_name,
            global_attrs=gmeta,
            # namespace_uuid=str(processed_namespace_uuid),
            # id_prefix=id_prefix,
            # id_seed=f"{river}:{frequency}",
            namespace_uuid=namespace_uuid,
            id_prefix=id_prefix,
            id_seed=id_seed,
            engine=engine,
            nc_format=nc_format,
            time_encoding_cfg={
                "dtype": time_enc_cfg.get("dtype", "int32"),
                "_FillValue": time_enc_cfg.get("_FillValue", None),
                "units": time_enc_cfg.get("units", "seconds since 1970-01-01 00:00:00"),
                "calendar": time_enc_cfg.get("calendar", None),
            },
            var_encoding_overrides=None,
        )
        outputs.append(out_path)

    return outputs

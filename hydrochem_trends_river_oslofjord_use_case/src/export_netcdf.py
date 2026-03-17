from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd
import xarray as xr

# create directories if missing
try:
    from .utils import ensure_dirs
except Exception:
    def ensure_dirs(*paths: Union[str, Path]) -> None:
        """mkdir -p for each given path."""

        for p in paths:
            Path(p).mkdir(parents=True, exist_ok=True)

# ------------------------- small internal helpers -------------------------
def _as_utc_string(ts) -> str:
    """Convert to pandas Timestamp, force UTC, then output ISO 8601 with Z."""

    t = pd.to_datetime(ts, utc=True)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _infer_time_name(ds: xr.Dataset, explicit: Optional[str]) -> Optional[str]:
    """
    Choose a time coordinate name:
    - return None if nothing fits (export will still work but won't set coverage)
    """

    if explicit and explicit in ds.coords:
        return explicit
    for c in ("time", "date", "datetime", "timestamp", "sample_date"):
        if c in ds.coords:
            return c
    return None


def _build_encoding(
    ds: xr.Dataset,
    time_name: Optional[str],
    time_cfg: Optional[Dict[str, Any]],
    var_overrides: Optional[Dict[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    """Create an xarray encoding dict."""

    enc: Dict[str, Any] = {}

    # Time coordinate encoding
    tcfg = time_cfg or {}
    if time_name and time_name in ds.coords:
        enc[time_name] = {
            "dtype": tcfg.get("dtype", "int32"),
            "_FillValue": tcfg.get("_FillValue", None),
            "units": tcfg.get("units", "seconds since 1970-01-01 00:00:00"),
        }
        if tcfg.get("calendar") is not None:
            enc[time_name]["calendar"] = tcfg["calendar"]

    # Leave lat/lon without fill values if present
    if "latitude" in ds.coords:
        enc["latitude"] = {"_FillValue": None}
    if "longitude" in ds.coords:
        enc["longitude"] = {"_FillValue": None}

    # Apply explicit per-variable overrides
    for var, override in (var_overrides or {}).items():
        enc[var] = {**enc.get(var, {}), **override}

    return enc


def _set_default_coord_attrs(ds: xr.Dataset, time_name: Optional[str]) -> xr.Dataset:
    """
    Ensure consistent coordinate metadata across all exported datasets.
    """
    if time_name and time_name in ds.coords:
        ds[time_name].attrs.setdefault("standard_name", "time")
        ds[time_name].attrs.setdefault("long_name", "Time")
        ds[time_name].attrs.setdefault("axis", "T")

    if "latitude" in ds.coords:
        ds["latitude"].attrs.setdefault("standard_name", "latitude")
        ds["latitude"].attrs.setdefault("long_name", "Latitude")
        ds["latitude"].attrs.setdefault("units", "degrees_north")

    if "longitude" in ds.coords:
        ds["longitude"].attrs.setdefault("standard_name", "longitude")
        ds["longitude"].attrs.setdefault("long_name", "Longitude")
        ds["longitude"].attrs.setdefault("units", "degrees_east")

    return ds


def _ensure_timeseries_id(ds: xr.Dataset) -> xr.Dataset:
    """
    If dataset contains a scalar station identifier variable, ensure cf_role=timeseries_id.
    """
    for cand in ("station_id", "station_name", "river_name"):
        if cand in ds.variables and ds[cand].dims == ():
            ds[cand].attrs.setdefault("cf_role", "timeseries_id")
            break
    return ds

def _set_default_station_var_attrs(ds: xr.Dataset) -> xr.Dataset:
    """
    Ensure consistent attrs for scalar station/site identifier variables
    that live in DATA_VARS (not coords).
    """
    defaults = {
        "station_name": {"long_name": "Station name", "units": "1"},
        "station_id": {"long_name": "Station identifier", "units": "1"},
        "station_code": {"long_name": "Station code", "units": "1"},
        "station_type": {"long_name": "Station type", "units": "1"},
        "river_name": {"long_name": "Station name", "units": "1"},
    }

    for v, attrs in defaults.items():
        if v in ds.data_vars and ds[v].dims == ():
            for k, val in attrs.items():
                ds[v].attrs.setdefault(k, val)

    # Ensure cf_role is on a scalar identifier variable
    # Prefer station_name, otherwise fall back to river_name/station_id
    if "station_name" in ds.data_vars and ds["station_name"].dims == ():
        ds["station_name"].attrs.setdefault("cf_role", "timeseries_id")
    elif "river_name" in ds.data_vars and ds["river_name"].dims == ():
        ds["river_name"].attrs.setdefault("cf_role", "timeseries_id")
    elif "station_id" in ds.data_vars and ds["station_id"].dims == ():
        ds["station_id"].attrs.setdefault("cf_role", "timeseries_id")

    return ds

# ------------------------------- public API -------------------------------
def export_dataset(
    ds: xr.Dataset,
    *,
    output_dir: Union[str, Path],
    filename: str,
    time_name: Optional[str],
    global_attrs: Dict[str, Any],
    default_global_attrs: Optional[Dict[str, Any]] = None,
    namespace_uuid: Optional[str] = None,
    id_prefix: Optional[str] = None,
    id_seed: Optional[str] = None,
    engine: str = "netcdf4",
    nc_format: str = "NETCDF4",
    time_encoding_cfg: Optional[Dict[str, Any]] = None,
    var_encoding_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Path:
    """
    Write an xarray.Dataset to NetCDF with consistent metadata and encodings.

    Caller-provided global_attrs override default_global_attrs.
    Coverage & geospatial fields are overwritten to reflect the dataset contents.
    """
    output_dir = Path(output_dir)
    ensure_dirs(output_dir)

    # Copy dataset so we never mutate caller's object
    ds = ds.copy()

    # Determine the time coordinate name (explicit or inferred)
    tn = _infer_time_name(ds, time_name)

    # Ensure consistent coordinate attrs across all exports
    ds = _set_default_coord_attrs(ds, tn)
    ds = _ensure_timeseries_id(ds)
    ds = _set_default_station_var_attrs(ds)

    # Global attributes
    attrs: Dict[str, Any] = dict(default_global_attrs or {})
    attrs.update(global_attrs or {})

    # Stable dataset ID (optional)
    if "id" not in attrs and namespace_uuid and id_prefix and id_seed:
        try:
            ns = uuid.UUID(str(namespace_uuid))
            attrs["id"] = f"{id_prefix}:{uuid.uuid5(ns, str(id_seed))}"
        except Exception:
            pass

    # Always compute/overwrite temporal coverage if we can
    if tn and tn in ds.coords and ds[tn].size > 0:
        attrs["time_coverage_start"] = _as_utc_string(ds[tn].min().values)
        attrs["time_coverage_end"] = _as_utc_string(ds[tn].max().values)

    # Always compute/overwrite geospatial coverage if coords exist
    if "latitude" in ds.coords:
        lat = float(ds.latitude.values)
        attrs["geospatial_lat_min"] = lat
        attrs["geospatial_lat_max"] = lat
    if "longitude" in ds.coords:
        lon = float(ds.longitude.values)
        attrs["geospatial_lon_min"] = lon
        attrs["geospatial_lon_max"] = lon

    # date_created: keep user value if present; otherwise set to now (UTC)
    attrs.setdefault("date_created", pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))

    # Attach final global attributes (NetCDF global attrs are typically strings)
    ds.attrs = {k: str(v) for k, v in attrs.items()}

    # Encoding (time + coords + per-var overrides)
    encoding = _build_encoding(
        ds,
        time_name=tn,
        time_cfg=time_encoding_cfg,
        var_overrides=var_encoding_overrides,
    )

    # Write file
    out_path = (output_dir / filename).resolve()
    ds.to_netcdf(out_path, encoding=encoding, engine=engine, format=nc_format, mode="w")
    print(f"Saved NetCDF: {out_path}")
    return out_path

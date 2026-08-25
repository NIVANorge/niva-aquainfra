from __future__ import annotations

import uuid
import math
from pathlib import Path
from typing import Dict, Any, List, Optional

import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

from src.export_netcdf import export_dataset

plt.style.use("ggplot")

# ------------------------ config utilities ------------------------
def meta_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Get the optional meta config (column-backed or fixed values)."""
    return (cfg.get("meta") or {}).copy()

def autodetect_time_col(df: pd.DataFrame, explicit: Optional[str]) -> str:
    """Pick time column from config if given; else try common names."""
    if explicit and explicit in df.columns:
        return explicit
    for c in ["sample_date", "date", "datetime", "time", "timestamp"]:
        if c in df.columns:
            return c
    raise KeyError("No time column. Set input.time_col or use a common name like 'sample_date'.")

def read_meta_value(df: pd.DataFrame, m: Dict[str, Any], key: str) -> Optional[Any]:
    """Read a single metadata value (from a column or a fixed value)."""
    spec = m.get(key)
    if not spec:
        return None
    if "from_col" in spec:
        col = spec["from_col"]
        if col not in df.columns:
            raise KeyError(f"meta.{key}.from_col='{col}' not found.")
        s = df[col].dropna()
        return s.iloc[0] if not s.empty else None
    if "value" in spec:
        return spec["value"]
    return None

def ensure_one_station_if_possible(df: pd.DataFrame, m: Dict[str, Any], fname: str) -> None:
    """If station_id comes from a column, enforce one station per file."""
    spec = m.get("station_id")
    if not spec or "from_col" not in spec:
        return
    col = spec["from_col"]
    if col not in df.columns:
        raise KeyError(f"meta.station_id.from_col='{col}' not found.")
    uniq = df[col].dropna().unique()
    if len(uniq) != 1:
        raise ValueError(f"Expected 1 station per file; found {len(uniq)} in {fname}: {uniq}")

# --------------------------- plotting ---------------------------
def plot_reconstruction(df: pd.DataFrame, date_col: str, var_orig: str, var_final: str,
                        label_base: str = "", station_label: str = "", units: str = "",
                        output_path: Path | None = None,
                        colors: Dict[str, str] | None = None) -> None:
    colors = colors or {'original': '#1b9e77', 'final': '#d95f02'}
    if var_orig not in df.columns or var_final not in df.columns:
        return
    plt.figure(figsize=(10, 4))
    plt.plot(df[date_col], df[var_orig], label=f'{label_base} (original)',
             color=colors['original'], marker='o', linestyle='none')
    plt.plot(df[date_col], df[var_final], label=f'{label_base} (reconstructed)',
             color=colors['final'], marker='.', linestyle='-', alpha=0.7)
    plt.title(f"{station_label}")
    plt.ylabel(f"{label_base} ({units})" if units else label_base)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
    else:
        plt.show()

def plot_scatter(ax, x, y, xlabel, ylabel, title, unit=None):
    ax.scatter(x, y, alpha=0.5)
    if pd.notna(x).any() and pd.notna(y).any():
        x_min, x_max = float(np.nanmin(x)), float(np.nanmax(x))
        y_min, y_max = float(np.nanmin(y)), float(np.nanmax(y))
        lo, hi = min(x_min, y_min), max(x_max, y_max)
        ax.plot([lo, hi], [lo, hi], 'r--')
    unit_str = f" ({unit})" if unit else ""
    ax.set_xlabel(f"{xlabel}{unit_str}")
    ax.set_ylabel(f"{ylabel}{unit_str}")
    ax.set_title(title)

def plot_lines(ax, df: pd.DataFrame, date_col: str,
               columns: List[str], labels: List[str], title: str, unit: str | None = None):
    for col, label in zip(columns, labels):
        if col in df.columns:
            ax.plot(df[date_col], df[col], label=label)
    ax.legend()
    ax.set_title(title)
    ax.set_ylabel(unit if unit else "")

# ------------------------------ QC -----------------------------------
def detect_outliers(df: pd.DataFrame, station_name: str, filename: str,
                                date_col: str,
                                meta_map: Dict[str, Any],
                                variables: List[str] | None = None,
                                out_cfg: Dict[str, Any] | None = None,
                                fig_dir: Path | None = None) -> pd.DataFrame:
    """Replace quantile-based outliers with NaN and optionally write a QC figure."""

    df = df.copy()

    # quantile thresholds from config
    out_cfg = out_cfg or {"lower_quantile": 0.05, "upper_quantile": 0.95}
    q_low, q_high = float(out_cfg["lower_quantile"]), float(out_cfg["upper_quantile"])

    if variables is None:
        exclude = {date_col}
        for k in ["station_id", "station_code", "station_name", "station_type", "latitude", "longitude"]:
            spec = meta_map.get(k)
            if spec and "from_col" in spec and spec["from_col"] in df.columns:
                exclude.add(spec["from_col"])
        variables = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in exclude]

    variables = [v for v in variables if v in df.columns and not df[v].dropna().empty]
    if not variables:
        return df

    ncols, nrows = 2, math.ceil(len(variables) / 2)
    fig, axs = plt.subplots(nrows=nrows, ncols=ncols, figsize=(14, 4 * nrows), squeeze=False)
    fig.suptitle(f"{station_name} – Outlier Detection (Q{q_low*100:.2f}–Q{q_high*100:.2f})", fontsize=16)

    for idx, var in enumerate(variables):
        row, col = divmod(idx, ncols)
        ax = axs[row][col]
        series = df[var].dropna()
        p_low = series.quantile(q_low)
        p_high = series.quantile(q_high)
        outliers = (df[var] < p_low) | (df[var] > p_high)
        ax.scatter(df[date_col], df[var], label=var, color='gray', alpha=0.7)
        ax.scatter(df.loc[outliers, date_col], df.loc[outliers, var], color='red', label='Outliers')
        ax.set_title(var); ax.set_xlabel("Date"); ax.set_ylabel(var); ax.legend(); ax.grid(True)
        df.loc[outliers, var] = np.nan

    for i in range(len(variables), nrows * ncols):
        row, col = divmod(i, ncols)
        axs[row][col].axis('off')

    if fig_dir:
        fig_dir.mkdir(parents=True, exist_ok=True)
        base_name = Path(filename).stem
        out_png = fig_dir / f"{base_name}_detected_outliers.png"
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.savefig(out_png, dpi=300, bbox_inches='tight')
        plt.close()
    else:
        plt.show()
    return df

# ----------------------- filename id formatter ------------------------
def _fmt_id(val) -> Optional[str]:
    """Format an id value for filenames/attrs (avoid trailing .0)."""
    if val is None:
        return None
    try:
        f = float(val)
        if np.isfinite(f) and f.is_integer():
            return str(int(f))
        return f"{f:g}"
    except Exception:
        s = str(val).strip()
        return s or None

# ------------------------------- main --------------------------------
def preprocess(cfg: Dict[str, Any]) -> List[Path]:
    """ Run the preprocessing pipeline and write a cleaned NetCDF file. """

    # input + output dirs
    inp = cfg["input"]
    explicit_time = inp.get("time_col")

    project_root = Path(__file__).resolve().parents[1]

    in_file = project_root / inp["file"]
    fig_dir = project_root / cfg["paths"]["fig_dir"]
    out_dir = project_root / cfg["paths"]["output_dir"]

    if not in_file.exists():
        raise FileNotFoundError(f"Input file not found: {in_file}")

    # other config
    reconstruction_config: Dict[str, Any] = cfg["reconstruction_config"]
    derivation_config: List[Dict[str, Any]] = cfg["derivation_config"]
    plot_config: List[Dict[str, Any]] = cfg["plot_config"]
    outlier_config: Dict[str, Any] = cfg["outlier_config"]
    pars_meta_df = pd.DataFrame(cfg.get("pars_metadata", []))
    standard_name_map: Dict[str, str] = cfg.get("standard_name_map", {})
    var_comments: Dict[str, str] = cfg.get("var_comments", {})
    global_metadata_config: Dict[str, Any] = cfg["global_metadata_config"]  # may include fixed date_created
    processed_namespace_uuid = uuid.UUID(cfg["processed_namespace_uuid"])
    meta_map = meta_cfg(cfg)

    output_cfg: Dict[str, Any] = cfg.get("output", {})
    time_name: str = output_cfg.get("time_name", "time")

    # load file
    with xr.open_dataset(in_file) as ds_in:
        df = ds_in.to_dataframe().reset_index()

    tcol = autodetect_time_col(df, explicit_time)
    df[tcol] = pd.to_datetime(df[tcol], errors="coerce")
    ensure_one_station_if_possible(df, meta_map, in_file.name)

    # reconstruction + per-var plots
    station_name_val = read_meta_value(df, meta_map, "station_name")
    if station_name_val is None:
        sname_spec = meta_map.get("station_name")
        if sname_spec and "from_col" in sname_spec and sname_spec["from_col"] in df.columns:
            svals = df[sname_spec["from_col"]].dropna().unique()
            station_name_val = svals[0] if len(svals) else None
    station_label = str(station_name_val) if station_name_val is not None else in_file.name

    tmp_cols: List[str] = []
    for var, settings in reconstruction_config.items():
        fallback     = settings.get("fallback")
        compute_from = settings.get("compute_from")
        formula      = settings.get("formula")
        preserve_as  = settings.get("preserve_as", f"{var}_orig")
        calc_temp    = settings.get("calc_temp_var", f"{var}_calc")
        units        = settings.get("units", "")

        if var in df.columns:
            df[preserve_as] = df[var]

        if var in df.columns and fallback and (fallback in df.columns):
            df[var] = df[var].fillna(df[fallback])
            tmp_cols.append(fallback)

        if compute_from and (compute_from in df.columns) and formula:
            df[calc_temp] = eval(formula, {}, {compute_from: df[compute_from]})
            df[var] = df[var].fillna(df[calc_temp]) if var in df.columns else df[calc_temp]
            tmp_cols.extend([compute_from, calc_temp])

        if preserve_as in df.columns and var in df.columns:
            out_png = fig_dir / f"{in_file.stem}_{var}_reconstruction.png"
            plot_reconstruction(df, tcol, preserve_as, var,
                                label_base=var, station_label=str(station_label),
                                units=units, output_path=out_png)
        tmp_cols.append(preserve_as)

    df = df.drop(columns=[c for c in tmp_cols if c in df.columns])

    # derivations + masks + summary figure
    for rule in derivation_config:
        op = rule["operation"]; target = rule.get("target")

        if op == "scale" and target in df.columns:
            df[target] = df[target] * rule["factor"]

        elif op == "sum":
            sources = rule["sources"]
            if all(col in df.columns for col in sources):
                df[target] = df[sources[0]] + df[sources[1]]

        elif op == "fill_from_other":
            if all(col in df.columns for col in rule["requires"]):
                ctx = {col: df[col] for col in df.columns}
                cond = eval(rule["condition"], {}, ctx)
                df.loc[cond, target] = eval(rule["expression"], {}, ctx)

        elif op == "rowwise_sum":
            sources = rule["sources"]
            if all(col in df.columns for col in sources):
                df[target] = df.apply(
                    lambda row: sum(row[col] for col in sources) if all(pd.notna(row[col]) for col in sources) else np.nan,
                    axis=1
                )

        elif op == "difference":
            if all(col in df.columns for col in rule["requires"]):
                a, b = rule["requires"]
                df[target] = df[a] - df[b]

        elif op == "mask_date_before":
            date_column = rule.get("date_column", tcol)
            if (rule.get("file_contains","") in in_file.name) and (target in df.columns) and (date_column in df.columns):
                mask = pd.to_datetime(df[date_column]) < pd.Timestamp(rule["before"])
                df.loc[mask, target] = np.nan

    fig, axs = plt.subplots(3, 2, figsize=(14, 16))
    fig.suptitle(f"Station: {station_label}", fontsize=16)
    plotted = False
    for pc in plot_config:
        if not all(col in df.columns for col in pc["required"]):
            continue
        r, c = pc["subplot"]; plotted = True
        if pc["type"] == "scatter":
            plot_scatter(axs[r, c], df[pc["x"]], df[pc["y"]], pc["xlabel"], pc["ylabel"], pc["title"], unit=pc.get("unit"))
        elif pc["type"] == "line":
            plot_lines(axs[r, c], df, tcol, pc["columns"], pc["labels"], pc["title"], unit=pc.get("unit"))
    axs[2, 1].axis('off')
    if plotted:
        out_png = fig_dir / f"{in_file.stem}_derived_pars.png"
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.savefig(out_png, dpi=300, bbox_inches='tight')
        plt.close()
    else:
        plt.close()

    # outlier pass
    df = detect_outliers(df, station_label, in_file.name,
                                     date_col=tcol, meta_map=meta_map,
                                     out_cfg=outlier_config, fig_dir=fig_dir)

    # build dataset + write
    df = df.copy()
    df[tcol] = pd.to_datetime(df[tcol])

    df = df.set_index(tcol)
    df.index.name = time_name

    lat = read_meta_value(df, meta_map, "latitude")
    lon = read_meta_value(df, meta_map, "longitude")
    station_id = read_meta_value(df, meta_map, "station_id")
    station_code = read_meta_value(df, meta_map, "station_code")
    station_name = read_meta_value(df, meta_map, "station_name")
    station_type = read_meta_value(df, meta_map, "station_type")

    # drop any meta columns before building dataset
    drop_cols = []
    for k in ["latitude","longitude","station_id","station_code","station_name","station_type"]:
        spec = meta_map.get(k)
        if spec and "from_col" in spec and spec["from_col"] in df.columns:
            drop_cols.append(spec["from_col"])
    df_clean = df.drop(columns=drop_cols, errors="ignore")

    ds_out = xr.Dataset.from_dataframe(df_clean)

    ds_out = ds_out.assign_coords(**{time_name: (time_name, df.index)})

    if lat is not None and lon is not None:
        ds_out = ds_out.assign_coords(
            latitude=xr.DataArray(float(lat), dims=()),
            longitude=xr.DataArray(float(lon), dims=()),
        ).set_coords(["latitude", "longitude"])

    # station_id handling
    sid_str = _fmt_id(station_id)
    station_id_is_int = False
    sid_value_for_ds: Optional[Any] = None
    if station_id is not None:
        try:
            f = float(station_id)
            if np.isfinite(f) and f.is_integer():
                station_id_is_int = True
                sid_value_for_ds = int(f)
            else:
                sid_value_for_ds = str(station_id)
        except Exception:
            sid_value_for_ds = str(station_id)

    # station meta vars
    if sid_value_for_ds is not None:
        ds_out["station_id"] = xr.DataArray(
            sid_value_for_ds, dims=(),
            attrs={"long_name": "Station identifier", "units": "1"}
        )

    if station_code is not None:
        ds_out["station_code"] = xr.DataArray(
            str(station_code), dims=(),
            attrs={"long_name": "Station code", "units": "1"}
        )

    if station_name is not None:
        ds_out["station_name"] = xr.DataArray(
            str(station_name), dims=(),
            attrs={"cf_role": "timeseries_id", "long_name": "Station name", "units": "1"}
        )

    if station_type is not None:
        ds_out["station_type"] = xr.DataArray(
            str(station_type), dims=(),
            attrs={"long_name": "Station type", "units": "1"}
        )

    # annotate data vars
    for var in ds_out.data_vars:
        if var in ["station_id", "station_code", "station_name", "station_type"]:
            continue
        if not pars_meta_df.empty:
            match = pars_meta_df[pars_meta_df["parameter_name"] == var]
            if not match.empty:
                row = match.iloc[0]
                ds_out[var].attrs["units"] = str(row["unit"])
                ds_out[var].attrs["parameter_name"] = str(row["parameter_name"])
                ds_out[var].attrs["long_name"] = str(standard_name_map.get(var, row["parameter_name"]))
        if var in var_comments:
            ds_out[var].attrs["comment"] = str(var_comments[var])

    # export via centralized writer
    export_cfg = cfg.get("export", {})
    engine = export_cfg.get("engine", "netcdf4")
    nc_format = export_cfg.get("format", "NETCDF4")
    time_enc_cfg = export_cfg.get("time", {})

    # filename + id seed
    if sid_str:
        filename = export_cfg.get("filename_template", "cleaned_riverchem_{station_id_or_stem}.nc").format(
            station_id_or_stem=sid_str, stem=in_file.stem
        )
        id_seed = sid_str
    else:
        filename = export_cfg.get("filename_template", "{stem}_cleaned.nc").format(
            station_id_or_stem=in_file.stem, stem=in_file.stem
        )
        id_seed = in_file.stem

    # per-variable encoding
    enc_overrides: Dict[str, Dict[str, Any]] = {}
    if "station_id" in ds_out and station_id_is_int:
        enc_overrides["station_id"] = {"dtype": "int32", "_FillValue": -9999}

    # write file
    out_path = export_dataset(
        ds=ds_out,
        output_dir=out_dir,
        filename=filename,
        time_name=time_name,
        global_attrs=global_metadata_config,
        namespace_uuid=str(processed_namespace_uuid),
        id_prefix=export_cfg.get("id_prefix", "no.niva"),
        id_seed=id_seed,
        engine=engine,
        nc_format=nc_format,
        time_encoding_cfg=time_enc_cfg,
        var_encoding_overrides=enc_overrides,
    )

    return [out_path]

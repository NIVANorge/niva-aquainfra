from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
import pymannkendall as mk
from scipy.stats import theilslopes
from src.utils import resolve_path, ensure_dirs


plt.style.use("ggplot")


# # ------------------------- path helpers -------------------------
# def _project_root() -> Path:
#     # src/mk_trend_test.py -> parents[1] = project root if src/ is inside the project
#     return Path(__file__).resolve().parents[1]

#
# def _abs_path(p: str | Path) -> Path:
#     p = Path(p)
#     return p if p.is_absolute() else (_project_root() / p)


# def _ensure_dir(p: Path) -> None:
#     p.mkdir(parents=True, exist_ok=True)


# ------------------------- dataset helpers -------------------------
def _infer_time_name(ds: xr.Dataset) -> Optional[str]:
    for c in ("date", "time", "datetime", "timestamp", "sample_date"):
        if c in ds.coords:
            return c
    for c in ("date", "time"):
        if c in ds.dims:
            return c
    return None


def _list_stations_from_ds(ds: xr.Dataset, station_dim: str, station_coord: Optional[str] = None) -> List[str]:
    if station_coord and station_coord in ds.coords:
        vals = ds[station_coord].values
    elif station_coord and station_coord in ds.data_vars:
        vals = ds[station_coord].values
    elif station_dim in ds.coords:
        vals = ds[station_dim].values
    else:
        n = int(ds.dims.get(station_dim, 0))
        return [f"{station_dim}_{i}" for i in range(n)]

    out: List[str] = []
    for v in vals:
        try:
            if isinstance(v, bytes):
                out.append(v.decode("utf-8"))
            else:
                out.append(str(v))
        except Exception:
            out.append(str(v))
    return out


def _slice_period(s: pd.Series, *, start: Optional[str], end: Optional[str]) -> pd.Series:
    """Slice datetime-indexed Series to [start, end]."""
    if s is None or s.empty:
        return s

    s = s.copy()
    s.index = pd.to_datetime(s.index, errors="coerce")
    s = s[~s.index.isna()].sort_index()

    if start is None and end is None:
        return s

    start_ts = pd.to_datetime(start) if start else None
    end_ts = pd.to_datetime(end) if end else None

    if start_ts is not None and end_ts is not None:
        return s.loc[start_ts:end_ts]
    if start_ts is not None:
        return s.loc[start_ts:]
    return s.loc[:end_ts]

def _find_site_file(folder: Path, site: str) -> Optional[Path]:
    if not folder.exists():
        return None
    site_l = site.lower().replace(" ", "_")
    cands = sorted(folder.glob("*.nc"))

    for f in cands:
        if site_l in f.name.lower():
            return f

    if len(cands) == 1:
        return cands[0]
    return None


def _open_series_and_unit_from_nc(
    nc_path: Path,
    var: str,
    *,
    station: Optional[str] = None,
    station_dim: Optional[str] = None,
    station_coord: Optional[str] = None,
) -> Tuple[Optional[pd.Series], Optional[str]]:
    """
    Load variable `var` from NetCDF into a pandas Series indexed by datetime.
    Also returns ds[var].attrs.get("units") if present.
    """
    ds = xr.open_dataset(nc_path)

    if var not in ds.data_vars:
        return None, None

    time_coord = _infer_time_name(ds)
    if time_coord is None:
        return None, None

    da = ds[var]

    # Multi-station selection (future marine style)
    if station is not None and station_dim is not None and station_dim in da.dims:
        if station_coord and station_coord in ds.coords:
            labels = [str(x) for x in ds[station_coord].values]
            if station in labels:
                da = da.isel({station_dim: labels.index(station)})
            else:
                try:
                    da = da.sel({station_dim: station})
                except Exception:
                    return None, None
        else:
            try:
                da = da.sel({station_dim: station})
            except Exception:
                return None, None

    try:
        t = pd.to_datetime(ds[time_coord].values, errors="coerce")
    except Exception:
        return None, None

    y = pd.Series(da.values, index=pd.DatetimeIndex(t, name=time_coord), name=var).sort_index()
    unit = da.attrs.get("units", None)
    return y, (str(unit) if unit is not None else None)


# ------------------------- unit helpers -------------------------
def _display_unit_for_plot(
    base_unit: Optional[str],
    *,
    frequency: str,
    var: str,
    non_mass_vars: set[str],
    undefined_label: str = "undefined",
) -> str:
    """
    For plotting labels:
      - if base_unit == "tonnes" -> tonnes/<period>
      - non-mass vars: keep base_unit (or undefined_label)
      - if base_unit undefined -> undefined_label
    """
    bu = (base_unit or "").strip()

    if var in non_mass_vars:
        return bu if bu else undefined_label

    if bu.lower() in {"", "undefined", "unknown"}:
        return undefined_label

    if bu == "tonnes":
        if frequency == "monthly":
            return "tonnes/month"
        if frequency == "annual":
            return "tonnes/year"
        if frequency == "daily":
            return "tonnes/day"

    return bu

# ------------------------- results helpers -------------------------
def _period_str(idx: pd.Index) -> str:
    if idx is None or len(idx) == 0:
        return ""
    try:
        a = pd.to_datetime(idx.min()).date()
        b = pd.to_datetime(idx.max()).date()
        return f"{a}-{b}"
    except Exception:
        return f"{idx.min()}-{idx.max()}"


def _mk_trend_label(trend: Any) -> str:
    if trend is None:
        return "no trend"
    return str(trend)


def _x_for_fit(s: pd.Series, frequency: str) -> np.ndarray:
    """
    x scale used for fitting:
      - annual: YEAR integers
      - monthly: matplotlib date numbers
    """
    if frequency == "annual":
        years = pd.to_datetime(s.index).year.astype(float)
        return years.to_numpy()
    dt = pd.to_datetime(s.index).to_pydatetime()
    return mdates.date2num(dt).astype(float)


def _sen_slope_intercept(y: pd.Series, x: np.ndarray) -> tuple[float, float]:
    yv = y.values.astype(float)
    mask = np.isfinite(x) & np.isfinite(yv)
    xv = x[mask].astype(float)
    yv = yv[mask].astype(float)
    if len(yv) < 2:
        return (np.nan, np.nan)
    slope, intercept, *_ = theilslopes(yv, xv, 0.95)
    return float(slope), float(intercept)


def _classify_sen_trend(slope: float, p: float, alpha: float) -> str:
    if not np.isfinite(p) or p > alpha or not np.isfinite(slope):
        return "no trend"
    if slope > 0:
        return "increasing"
    if slope < 0:
        return "decreasing"
    return "no trend"


# ------------------------- MK helper -------------------------
def _mk_test(
    y: pd.Series,
    *,
    frequency: str,
    mk_mode: str,
    alpha: float,
) -> Optional[Dict[str, Any]]:
    y = y.dropna()
    if y.empty:
        return None

    mode = mk_mode
    if mode == "auto":
        mode = "seasonal" if frequency == "monthly" else "original"

    try:
        if mode == "seasonal":
            res = mk.seasonal_test(y.values, period=12, alpha=alpha)
        else:
            res = mk.original_test(y.values, alpha=alpha)
    except Exception as e:
        return {"error": str(e), "mk_mode_used": mode}

    return {"mk_mode_used": mode, "trend": getattr(res, "trend", None), "p": getattr(res, "p", None)}


# ------------------------- plotting: station grid -------------------------
def _plot_station_grid(
    station: str,
    frequency: str,
    series_by_var: Dict[str, pd.Series],
    units_by_var: Dict[str, str],
    mk_df_station: pd.DataFrame,
    out_png: Path,
    *,
    alpha: float,
    ncols: int = 3,
) -> None:
    vars_present = [v for v, s in series_by_var.items() if s is not None and not s.dropna().empty]
    if not vars_present:
        return

    trend_colors = {
        "no trend": "darkgrey",
        "decreasing": "darkcyan",
        "increasing": "darksalmon",
        "insufficient_data": "darkgrey",
        "error": "darkgrey",
    }

    nvars = len(vars_present)
    nrows = math.ceil(nvars / ncols)

    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(6 * ncols, 3.8 * nrows), sharex=False)
    axes = np.array(axes).reshape(-1)

    for i, var in enumerate(vars_present):
        ax = axes[i]
        s = series_by_var[var].dropna()

        ax.scatter(s.index, s.values, s=100, color="dimgrey", alpha=0.9)

        # y label: variable + unit
        unit_lbl = units_by_var.get(var, "undefined")
        ax.set_ylabel(f"{var} [{unit_lbl}]")

        row = mk_df_station[mk_df_station["variable"] == var]

        if not row.empty:
            r = row.iloc[0]
            p = r.get("mk_p_val", np.nan)
            mk_tr = str(r.get("mk_trend", "no trend"))
            sen_tr = str(r.get("sen_trend", "no trend"))
            slope = r.get("sen_slp", np.nan)
            intercept = r.get("sen_incpt", np.nan)

            if np.isfinite(p) and float(p) <= alpha and np.isfinite(slope) and np.isfinite(intercept):
                line_col = trend_colors.get(sen_tr, "darkgrey")
                x = _x_for_fit(s, frequency)
                fitted = float(slope) * x + float(intercept)
                ax.plot(s.index, fitted, color=line_col, linestyle="dashed", linewidth=3)

            ax.set_title(f"MK: {mk_tr} (p={float(p):.3g})", fontsize=10)
        else:
            ax.set_title(var, fontsize=10)

        ax.grid(True)

        if frequency == "annual":
            ax.xaxis.set_major_locator(mdates.YearLocator(base=1))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

        ax.tick_params(axis="x", labelrotation=45)
        ax.tick_params(axis="both", labelsize=10)

        for _, spine in ax.spines.items():
            spine.set_linewidth(1.5)

    for j in range(nvars, len(axes)):
        axes[j].axis("off")

    # title only station name
    fig.suptitle(f"{station}", fontsize=18, y=0.99)
    fig.tight_layout(rect=[0, 0, 1, 0.985])

    # _ensure_dir(out_png.parent)
    ensure_dirs(out_png.parent)
    plt.savefig(out_png, dpi=300, bbox_inches="tight")
    plt.close(fig)


# ------------------------- plotting: trend matrix per variable -------------------------
def _plot_trend_matrix_for_variable(
    df_freq: pd.DataFrame,
    *,
    variable: str,
    frequency: str,
    stations_order: List[str],
    out_png: Path,
    alpha: float,
    non_sig_alpha: float = 0.25,
) -> None:
    """
    Per variable: x=trend category, y=station, scatter marker.
    Color indicates trend; transparency indicates significance.
    """
    if df_freq.empty:
        return

    d = df_freq[df_freq["variable"] == variable].copy()
    if d.empty:
        return

    # Choose what to plot as "trend": we use Sen trend (based on slope sign + MK p)
    # If sen_trend missing, fall back to mk_trend, else "no trend"
    if "sen_trend" not in d.columns:
        d["sen_trend"] = d.get("mk_trend", "no trend")

    # Significance from mk_p_val
    d["significant"] = np.isfinite(d["mk_p_val"].to_numpy()) & (d["mk_p_val"].astype(float) <= float(alpha))

    trend_order = ["decreasing", "no trend", "increasing"]
    x_map = {t: i for i, t in enumerate(trend_order)}

    trend_colors = {
        "no trend": "darkgrey",
        "decreasing": "darkcyan",
        "increasing": "darksalmon",
        "insufficient_data": "darkgrey",
        "error": "darkgrey",
    }

    # y order fixed by stations_order
    y_map = {st: i for i, st in enumerate(stations_order)}

    # build point lists
    xs, ys, cols, alphas = [], [], [], []
    for _, r in d.iterrows():
        st = str(r["station_id"])
        tr = str(r.get("sen_trend", "no trend"))
        if tr not in x_map:
            tr = "no trend"

        xs.append(x_map[tr])
        ys.append(y_map.get(st, np.nan))
        cols.append(trend_colors.get(tr, "darkgrey"))
        alphas.append(1.0 if bool(r["significant"]) else non_sig_alpha)

    # drop points with unknown station
    mask = np.isfinite(np.array(ys, dtype=float))
    xs = np.array(xs, dtype=float)[mask]
    ys = np.array(ys, dtype=float)[mask]
    cols = np.array(cols, dtype=object)[mask]
    alphas = np.array(alphas, dtype=float)[mask]

    if xs.size == 0:
        return

    plt.figure(figsize=(6.5, max(2.5, 0.45 * len(stations_order))))
    ax = plt.gca()

    # Draw each point (so alpha can vary)
    for x, y, c, a in zip(xs, ys, cols, alphas):
        ax.scatter(x, y, s=220, color=c, alpha=float(a), edgecolors="black", linewidths=0.5)

    ax.set_yticks(range(len(stations_order)))
    ax.set_yticklabels(stations_order)
    ax.set_xticks(range(len(trend_order)))
    ax.set_xticklabels(trend_order, rotation=0)

    ax.set_xlim(-0.5, len(trend_order) - 0.5)
    ax.set_ylim(-0.5, len(stations_order) - 0.5)

    ax.grid(True, axis="both", alpha=0.3)
    ax.set_xlabel(" ")
    ax.set_ylabel(" ")
    ax.set_title(f"{variable} – {frequency}")

    for _, spine in ax.spines.items():
        spine.set_linewidth(1.5)

    plt.tight_layout()
    # _ensure_dir(out_png.parent)
    ensure_dirs(out_png.parent)
    plt.savefig(out_png, dpi=300, bbox_inches="tight")
    plt.close()


# ------------------------- IO helpers -------------------------
def _write_station_excel(
    out_xlsx: Path,
    *,
    monthly_df: Optional[pd.DataFrame],
    annual_df: Optional[pd.DataFrame],
) -> None:
    # _ensure_dir(out_xlsx.parent)
    ensure_dirs(out_xlsx.parent)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xl:
        if monthly_df is not None and not monthly_df.empty:
            monthly_df.to_excel(xl, sheet_name="monthly", index=False)
        if annual_df is not None and not annual_df.empty:
            annual_df.to_excel(xl, sheet_name="annual", index=False)


# ------------------------- public API -------------------------
def analyze_trends(
    cfg: Dict[str, Any],
    *,
    frequency: str = "both",  # "annual" | "monthly" | "both"
    mk_mode: str = "auto",    # "auto" | "original" | "seasonal"
    stations: Optional[List[str]] = None,
) -> Dict[str, Path]:
    """
    Reads annual + monthly flux NetCDF and runs MK tests.

    Inputs:
      cfg["variables"] : list[str]
      cfg["inputs"][freq] : {"mode":"folder"|"file", "path": "...", ...}
      cfg["stations"] : list[str]  (or override via stations=...)

    Outputs:
      - Station grid figures:
          <output_dir>/<figures_dir>/<frequency>/<station>.png
      - Trend matrix figures:
          <output_dir>/<figures_dir>/<frequency>/trend_matrix/<variable>.png
      - One Excel per station:
          <output_dir>/<tables_dir>/<station>_mk_results.xlsx
      - One combined Excel for ALL stations (append mode):
          <output_dir>/<tables_dir>/<combined_table_name>
          with two sheets: monthly, annual

    Notes on append mode:
      - If the combined file exists, we read its monthly/annual sheets, append new rows,
        and drop duplicates using a key (default: frequency+station_id+variable+period).
      - If you want a different de-duplication rule, edit `dedupe_cols`.
    """
    vars_to_test: List[str] = cfg.get("variables", [])
    if not vars_to_test:
        raise ValueError("No variables in cfg['variables'].")

    # out_root = _abs_path(cfg["output_dir"])
    out_root = resolve_path(cfg["output_dir"])
    figures_dir = cfg.get("results", {}).get("figures_dir", "figures")
    tables_dir = cfg.get("results", {}).get("tables_dir", "tables")

    # Combined table name from config
    combined_name = cfg.get("results", {}).get("combined_table_name", "mk_results.xlsx")

    trend_opt = cfg.get("trend_options", {})
    alpha = float(trend_opt.get("alpha", 0.05))

    unit_opt = cfg.get("unit_options", {})
    non_mass_vars = set(unit_opt.get("non_mass_vars", []))
    undefined_unit_label = str(unit_opt.get("undefined_unit_label", "undefined"))

    period_cfg = trend_opt.get("period", {}) if isinstance(trend_opt.get("period", {}), dict) else {}
    start_date = period_cfg.get("start") or None
    end_date = period_cfg.get("end") or None

    min_points = trend_opt.get("min_points", {"monthly": 36, "annual": 5})
    min_monthly = int(min_points.get("monthly", 36))
    min_annual = int(min_points.get("annual", 5))

    plot_opt = cfg.get("plot_options", {})
    ncols = int(plot_opt.get("ncols", 3))
    non_sig_alpha = float(plot_opt.get("non_sig_alpha", 0.25))

    # Frequencies
    freq_list: List[str] = []
    if frequency in ("both", "monthly"):
        freq_list.append("monthly")
    if frequency in ("both", "annual"):
        freq_list.append("annual")

    inputs = cfg.get("inputs", {})
    if not inputs:
        raise ValueError("Missing cfg['inputs'].")

    stations_cfg = cfg.get("stations", cfg.get("site_li", []))
    stations_to_use = stations if stations is not None else stations_cfg
    if not stations_to_use:
        raise ValueError("No stations provided (cfg['stations'] empty and no override).")

    # Results per station
    results_by_station: Dict[str, Dict[str, pd.DataFrame]] = {}

    # Collect all rows for combined output
    all_rows: List[Dict[str, Any]] = []

    for freq in freq_list:
        fcfg = inputs.get(freq, {})
        mode = str(fcfg.get("mode", "folder")).lower()
        # src_path = _abs_path(fcfg.get("path", ""))
        src_path = resolve_path(fcfg.get("path", ""))

        if not src_path.exists():
            print(f"[trends] Missing {freq} input: {src_path}")
            continue

        station_dim = fcfg.get("station_dim")
        station_coord = fcfg.get("station_coord")

        stations_local = stations_to_use

        # Multi-station file mode ("marine later")
        if mode == "file" and stations_local == ["all"]:
            with xr.open_dataset(src_path) as ds:
                if not station_dim:
                    raise ValueError(f"inputs.{freq}.station_dim is required when mode='file'")
                stations_local = _list_stations_from_ds(ds, station_dim=str(station_dim), station_coord=station_coord)

        mk_rows_all: List[Dict[str, Any]] = []

        for st in stations_local:
            series_by_var: Dict[str, pd.Series] = {}
            units_by_var: Dict[str, str] = {}

            for var in vars_to_test:
                if mode == "folder":
                    nc_path = _find_site_file(src_path, st)
                    if nc_path is None:
                        continue
                    s, unit = _open_series_and_unit_from_nc(nc_path, var)
                    file_used = str(nc_path)
                else:
                    s, unit = _open_series_and_unit_from_nc(
                        src_path,
                        var,
                        station=st,
                        station_dim=str(station_dim) if station_dim else None,
                        station_coord=station_coord,
                    )
                    file_used = str(src_path)

                if s is None:
                    continue

                s = s.dropna()

                # optional time window filtering
                s = _slice_period(s, start=start_date, end=end_date)

                if s.empty:
                    continue

                series_by_var[var] = s
                units_by_var[var] = _display_unit_for_plot(
                    unit,
                    frequency=freq,
                    var=var,
                    non_mass_vars=non_mass_vars,
                    undefined_label=undefined_unit_label,
                )

                n_valid = int(s.shape[0])
                min_n = min_monthly if freq == "monthly" else min_annual

                # Stats
                first = float(s.iloc[0]) if n_valid else np.nan
                last = float(s.iloc[-1]) if n_valid else np.nan
                mean = float(s.mean()) if n_valid else np.nan
                median = float(s.median()) if n_valid else np.nan
                std_dev = float(s.std()) if n_valid else np.nan
                iqr = float(s.quantile(0.75) - s.quantile(0.25)) if n_valid else np.nan
                period = _period_str(s.index)

                if n_valid < min_n:
                    mk_rows_all.append({
                        "station_id": st,
                        "period": period,
                        "variable": var,
                        "unit_display": units_by_var[var],
                        "n_vals": n_valid,
                        "first": first,
                        "last": last,
                        "mean": mean,
                        "median": median,
                        "std_dev": std_dev,
                        "iqr": iqr,
                        "mk_p_val": np.nan,
                        "mk_trend": "insufficient_data",
                        "sen_slp": np.nan,
                        "sen_incpt": np.nan,
                        "sen_trend": "insufficient_data",
                        "frequency": freq,
                        "mk_mode_used": "n/a",
                        "file": file_used,
                    })
                    continue

                res = _mk_test(s, frequency=freq, mk_mode=mk_mode, alpha=alpha)
                if res is None:
                    continue

                if "error" in res:
                    mk_rows_all.append({
                        "station_id": st,
                        "period": period,
                        "variable": var,
                        "unit_display": units_by_var[var],
                        "n_vals": n_valid,
                        "first": first,
                        "last": last,
                        "mean": mean,
                        "median": median,
                        "std_dev": std_dev,
                        "iqr": iqr,
                        "mk_p_val": np.nan,
                        "mk_trend": "error",
                        "sen_slp": np.nan,
                        "sen_incpt": np.nan,
                        "sen_trend": "error",
                        "frequency": freq,
                        "mk_mode_used": res.get("mk_mode_used"),
                        "error": res.get("error"),
                        "file": file_used,
                    })
                    continue

                pval = res.get("p", np.nan)
                mk_trend = _mk_trend_label(res.get("trend"))

                # Sen slope/intercept on correct x-scale
                x = _x_for_fit(s, freq)
                sen_slp, sen_incpt = _sen_slope_intercept(s, x)
                sen_trend = _classify_sen_trend(sen_slp, float(pval) if np.isfinite(pval) else np.nan, alpha)

                mk_rows_all.append({
                    "station_id": st,
                    "period": period,
                    "variable": var,
                    "unit_display": units_by_var[var],
                    "n_vals": n_valid,
                    "first": first,
                    "last": last,
                    "mean": mean,
                    "median": median,
                    "std_dev": std_dev,
                    "iqr": iqr,
                    "mk_p_val": float(pval) if np.isfinite(pval) else np.nan,
                    "mk_trend": mk_trend,
                    "sen_slp": sen_slp,
                    "sen_incpt": sen_incpt,
                    "sen_trend": sen_trend,
                    "frequency": freq,
                    "mk_mode_used": res.get("mk_mode_used"),
                    "file": file_used,
                })

            # Station grid figure (one per station per freq)
            if series_by_var:
                mk_df_station = pd.DataFrame(
                    [r for r in mk_rows_all if r["station_id"] == st and r["frequency"] == freq]
                )
                out_png = out_root / figures_dir / freq / f"{st}.png"
                _plot_station_grid(
                    station=st,
                    frequency=freq,
                    series_by_var=series_by_var,
                    units_by_var=units_by_var,
                    mk_df_station=mk_df_station,
                    out_png=out_png,
                    alpha=alpha,
                    ncols=ncols,
                )

        # Save per-station tables and trend-matrix plots
        if mk_rows_all:
            df_all = pd.DataFrame(mk_rows_all)
            all_rows.extend(mk_rows_all)

            preferred = [
                "period", "station_id", "variable", "n_vals", "first", "last", "mean", "median", "std_dev", "iqr",
                "mk_p_val", "mk_trend", "sen_slp", "sen_incpt", "sen_trend",
            ]
            extras = [c for c in df_all.columns if c not in preferred]
            df_all = df_all[preferred + extras]

            # store per station for excel
            for st in df_all["station_id"].unique():
                results_by_station.setdefault(st, {})
                results_by_station[st][freq] = df_all[df_all["station_id"] == st].copy()

            # trend matrix plots (one per variable per freq)
            stations_order = list(stations_local)
            for var in vars_to_test:
                out_png = out_root / figures_dir / freq / "trend_matrix" / f"{var}.png"
                _plot_trend_matrix_for_variable(
                    df_all,
                    variable=var,
                    frequency=freq,
                    stations_order=stations_order,
                    out_png=out_png,
                    alpha=alpha,
                    non_sig_alpha=non_sig_alpha,
                )

    # ---- Combined table ----
    if all_rows:
        df_new = pd.DataFrame(all_rows)

        out_all = out_root / tables_dir / combined_name
        # _ensure_dir(out_all.parent)
        ensure_dirs(out_all.parent)

        # Split new rows
        df_new_m = df_new[df_new["frequency"] == "monthly"].copy()
        df_new_a = df_new[df_new["frequency"] == "annual"].copy()

        # Read existing (if any)
        if out_all.exists():
            try:
                existing = pd.read_excel(out_all, sheet_name=None, engine="openpyxl")
            except Exception:
                existing = {}

            df_old_m = existing.get("monthly", pd.DataFrame())
            df_old_a = existing.get("annual", pd.DataFrame())
        else:
            df_old_m = pd.DataFrame()
            df_old_a = pd.DataFrame()

        # Append + de-duplicate
        dedupe_cols = ["frequency", "station_id", "variable", "period"]

        def _append_dedup(old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
            if old_df is None or old_df.empty:
                out = new_df.copy()
            elif new_df is None or new_df.empty:
                out = old_df.copy()
            else:
                out = pd.concat([old_df, new_df], ignore_index=True)

            for c in dedupe_cols:
                if c not in out.columns:
                    out[c] = np.nan
            out = out.drop_duplicates(subset=dedupe_cols, keep="last")
            return out

        df_out_m = _append_dedup(df_old_m, df_new_m)
        df_out_a = _append_dedup(df_old_a, df_new_a)

        with pd.ExcelWriter(out_all, engine="openpyxl") as xl:
            if not df_out_m.empty:
                df_out_m.to_excel(xl, sheet_name="monthly", index=False)
            if not df_out_a.empty:
                df_out_a.to_excel(xl, sheet_name="annual", index=False)

        print(f"[trends] Saved combined table (append): {out_all}")

    # ---- Per-station files (optional) ----
    write_per_station = bool(cfg.get("results", {}).get("write_per_station_tables", True))

    written_excels: Dict[str, Path] = {}
    if write_per_station:
        for st, block in results_by_station.items():
            out_xlsx = out_root / tables_dir / f"{st}_mk_results.xlsx"
            _write_station_excel(out_xlsx, monthly_df=block.get("monthly"), annual_df=block.get("annual"))
            written_excels[st] = out_xlsx
            print(f"[trends] Saved: {out_xlsx}")

    return written_excels

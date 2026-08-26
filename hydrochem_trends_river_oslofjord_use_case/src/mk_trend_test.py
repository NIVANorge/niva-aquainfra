from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
import pymannkendall as mk
from scipy.stats import theilslopes

from .utils import (
    ensure_dirs,
    resolve_input_source,
    resolve_path,
)

plt.style.use("ggplot")


def _infer_time_name(ds: xr.Dataset) -> str | None:
    for c in ("date", "time", "datetime", "timestamp", "sample_date"):
        if c in ds.coords:
            return c
    for c in ("date", "time"):
        if c in ds.dims:
            return c
    return None


def _resolve_variable_name(
    var: str,
    rules: dict[str, Any] | None = None,
) -> str:
    """
    Convert an analysis variable name to the name used by a specific input source.
    Rules are optional and configured per input source.
    """
    rules = rules or {}
    name = var

    # Optional explicit mappings for exceptional cases
    name = rules.get("map", {}).get(name, name)

    # Optional generic replacements
    for old, new in rules.get("replace", {}).items():
        name = name.replace(old, new)

    return name


def _list_stations_from_ds(ds: xr.Dataset, station_dim: str, station_coord: str | None = None) -> list[str]:
    if station_coord and station_coord in ds.coords:
        vals = ds[station_coord].values
    elif station_coord and station_coord in ds.data_vars:
        vals = ds[station_coord].values
    elif station_dim in ds.coords:
        vals = ds[station_dim].values
    else:
        n = int(ds.dims.get(station_dim, 0))
        return [f"{station_dim}_{i}" for i in range(n)]

    out: list[str] = []
    for v in vals:
        try:
            if isinstance(v, bytes):
                out.append(v.decode("utf-8"))
            else:
                out.append(str(v))
        except Exception:
            out.append(str(v))
    return out


def _slice_period(s: pd.Series, *, start: str | None, end: str | None) -> pd.Series:
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

def _slice_years(
    s: pd.Series,
    *,
    start_year: int | None,
    end_year: int | None,
) -> pd.Series:
    """Restrict an already aggregated series to requested calendar years."""
    if s is None or s.empty:
        return s

    years = pd.to_datetime(s.index).year

    mask = np.ones(len(s), dtype=bool)

    if start_year is not None:
        mask &= years >= start_year

    if end_year is not None:
        mask &= years <= end_year

    return s.loc[mask]


def _covers_full_period(
    s: pd.Series,
    *,
    start_year: int | None,
    end_year: int | None,
) -> bool:
    """
    Check that an aggregated trend series reaches both requested
    boundary years.
    """
    if s is None or s.empty:
        return False

    years = pd.to_datetime(s.index).year

    if start_year is not None and years.min() > start_year:
        return False

    if end_year is not None and years.max() < end_year:
        return False

    return True

def _season_name(month: int) -> str:
    if month in (12, 1, 2):
        return "winter"
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    return "autumn"


def _aggregate_series(
    s: pd.Series,
    *,
    frequency: str,
    how: str = "mean",
    coverage: dict[str, Any] | None = None,
) -> pd.Series:

    coverage = coverage or {}

    s = s.copy()
    s.index = pd.to_datetime(s.index)
    s = s.dropna().sort_index()

    if s.empty:
        return s

    min_obs_per_period = int(coverage.get("min_obs_per_period", 1))

    if frequency == "monthly":
        rule = "MS"

        out = s.resample(rule).sum(min_count=1) if how == "sum" else s.resample(rule).mean()
        counts = s.resample(rule).count()
        return out[counts >= min_obs_per_period].dropna()

    if frequency == "annual":
        rule = "YS"

        out = s.resample(rule).sum(min_count=1) if how == "sum" else s.resample(rule).mean()
        counts = s.resample(rule).count()
        out = out[counts >= min_obs_per_period]

        if coverage.get("require_all_seasons", False):
            min_seasons = int(coverage.get("min_seasons_per_year", 4))

            tmp = pd.DataFrame({"value": s})
            tmp["year"] = tmp.index.year
            tmp["season"] = tmp.index.month.map(_season_name)

            seasons_per_year = tmp.groupby("year")["season"].nunique()
            valid_years = seasons_per_year[seasons_per_year >= min_seasons].index

            out = out[out.index.year.isin(valid_years)]

        return out.dropna()

    raise ValueError(f"Unsupported frequency: {frequency}")

def _aggregate_seasonal_by_season(
    s: pd.Series,
    *,
    how: str = "mean",
    coverage: dict[str, Any] | None = None
) -> dict[str, pd.Series]:

    coverage = coverage or {}
    min_obs_per_period = int(coverage.get("min_obs_per_period", 1))

    s = s.copy()
    s.index = pd.to_datetime(s.index)
    s = s.dropna().sort_index()

    out_by_season: dict[str, pd.Series] = {}

    for season in ["winter", "spring", "summer", "autumn"]:
        ss = s[s.index.month.map(_season_name) == season]

        if ss.empty:
            continue

        seasonal_year = ss.index.year.copy()

        if season == "winter":
            seasonal_year = ss.index.year + (ss.index.month == 12).astype(int)

        tmp = pd.DataFrame({
            "date": ss.index,
            "value": ss.values,
            "seasonal_year": seasonal_year,
        })

        rows = []

        for yr, g in tmp.groupby("seasonal_year"):
            n_raw = int(g["value"].count())

            if n_raw < min_obs_per_period:
                continue

            if how == "sum":
                agg_value = float(g["value"].sum())
            elif how == "mean":
                agg_value = float(g["value"].mean())
            else:
                raise ValueError(f"Unsupported aggregation: {how}")

            rows.append({
                "year": int(yr),
                "value": agg_value,
                "n_raw": n_raw,
                "dates": sorted(pd.to_datetime(g["date"]).dt.strftime("%Y-%m-%d").tolist()),
            })

        if not rows:
            continue

        out = pd.Series(
            [r["value"] for r in rows],
            index=pd.to_datetime([f"{r['year']}-01-01" for r in rows]),
            name=season,
        ).sort_index()

        out_by_season[season] = out

    return out_by_season

def _find_site_file(folder: Path, site: str) -> Path | None:
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
    nc_path: str | Path,
    var: str,
    *,
    station: str | None = None,
    station_dim: str | None = None,
    station_coord: str | None = None,
    depth_dim: str | None = None,
    depth_selection: dict[str, Any] | None = None,
) -> tuple[pd.Series | None, str | None]:

    ds = xr.open_dataset(nc_path)

    if var not in ds.data_vars:
        return None, None

    unit = ds[var].attrs.get("units", None)
    unit = str(unit) if unit is not None else None

    # Marine ragged/profile format:
    # time(profile), stationIndex(profile), rowSize(profile), variable(obs), depth(obs)
    if (
        "profile" in ds.dims
        and "obs" in ds.dims
        and "stationIndex" in ds.data_vars
        and "rowSize" in ds.data_vars
        and var in ds.data_vars
        and ds[var].dims == ("obs",)
    ):
        if station is None:
            return None, unit

        labels = []
        for x in ds[station_coord].values:
            labels.append(x.decode("utf-8") if isinstance(x, bytes) else str(x))

        if station not in labels:
            return None, unit

        station_i = labels.index(station)

        station_index = ds["stationIndex"].values
        row_size = ds["rowSize"].values
        times = pd.to_datetime(ds["time"].values, errors="coerce")

        values = ds[var].values
        depths = ds[depth_dim].values if depth_dim and depth_dim in ds.coords else None

        rows = []
        obs_start = 0

        for profile_i, n_obs in enumerate(row_size):
            obs_end = obs_start + int(n_obs)

            if station_index[profile_i] == station_i:
                v = values[obs_start:obs_end]

                if depths is not None:
                    d = depths[obs_start:obs_end]

                    if depth_selection:
                        mode = depth_selection.get("mode", "all")

                        if mode == "range":
                            dmin = depth_selection.get("min", None)
                            dmax = depth_selection.get("max", None)

                            if dmin is not None and dmax is not None:
                                mask = (d >= dmin) & (d <= dmax)
                                v = v[mask]

                        elif mode == "values":
                            wanted = depth_selection.get("values", [])
                            if wanted:
                                mask = np.isin(d, wanted)
                                v = v[mask]

                if len(v) > 0:
                    v = np.asarray(v, dtype=float)
                    if np.isfinite(v).any():
                        rows.append((times[profile_i], float(np.nanmean(v))))

            obs_start = obs_end

        if not rows:
            return None, unit

        s = pd.Series(
            [r[1] for r in rows],
            index=pd.DatetimeIndex([r[0] for r in rows], name="time"),
            name=var,
        ).sort_index()

        # If more than one profile exists for same day/time, average them
        s = s.groupby(s.index).mean()

        return s, unit

    # Existing simple time-series format
    time_coord = _infer_time_name(ds)
    if time_coord is None:
        return None, unit

    da = ds[var]

    if station is not None and station_dim is not None and station_dim in da.dims:
        if station_coord and station_coord in ds.coords:
            labels = []
            for x in ds[station_coord].values:
                labels.append(x.decode("utf-8") if isinstance(x, bytes) else str(x))

            if station in labels:
                da = da.isel({station_dim: labels.index(station)})
            else:
                return None, unit

    if depth_dim and depth_dim in da.dims:
        if depth_selection:
            mode = depth_selection.get("mode", "all")

            if mode == "range":
                dmin = depth_selection.get("min", None)
                dmax = depth_selection.get("max", None)
                if dmin is not None and dmax is not None:
                    da = da.sel({depth_dim: slice(dmin, dmax)})

            elif mode == "values":
                values = depth_selection.get("values", [])
                if values:
                    da = da.sel({depth_dim: values}, method="nearest")

        da = da.mean(dim=depth_dim, skipna=True)

    remaining_dims = [d for d in da.dims if d != time_coord]
    if remaining_dims:
        print(
            f"[trends] Skipping {var} in {nc_path}: "
            f"remaining dimensions {remaining_dims}"
        )
        return None, unit

    t = pd.to_datetime(ds[time_coord].values, errors="coerce")

    y = pd.Series(
        da.values,
        index=pd.DatetimeIndex(t, name=time_coord),
        name=var,
    ).sort_index()

    return y, unit

def _display_unit_for_plot(
    base_unit: str | None,
    *,
    frequency: str,
    aggregation: str,
    var: str,
    non_mass_vars: set[str],
    undefined_label: str = "undefined",
) -> str:
    """
    Return the unit after temporal aggregation.

    Examples:
    - daily flux in tonnes/day summed monthly -> tonnes/month
    - daily flux in tonnes/day summed annually -> tonnes/year
    - daily flux in tonnes/day summed by season -> tonnes/season
    - concentration averaged over time -> original concentration unit
    """

    bu = (base_unit or "").strip()

    if not bu or bu.lower() in {"undefined", "unknown"}:
        return undefined_label

    # Variables that are not mass fluxes keep their original units.
    if var in non_mass_vars:
        return bu

    # Summing a daily mass flux changes the temporal unit.
    if aggregation == "sum" and bu.lower() in {
        "tonnes/day",
        "tonnes d-1",
        "tonnes d^-1",
    }:
        if frequency == "monthly":
            return "tonnes/month"

        if frequency == "annual":
            return "tonnes/year"

        if frequency == "seasonal_by_season":
            return "tonnes/season"

    # For means, or units that do not need conversion,
    # preserve the source unit.
    return bu

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
      - annual and seasonal_by_season: YEAR integers
      - monthly: matplotlib date numbers
    """
    if frequency in ("annual", "seasonal_by_season"):
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


def _mk_test(
    y: pd.Series,
    *,
    frequency: str,
    mk_mode: str,
    alpha: float,
) -> dict[str, Any] | None:

    y = y.dropna().sort_index()

    if y.empty:
        return None

    mode = mk_mode

    if mode == "auto":
        if frequency == "monthly":
            mode = "seasonal"
        else:
            mode = "original"

    try:
        if mode == "seasonal":
            period = 12 if frequency == "monthly" else 1
            res = mk.seasonal_test(y.values, period=period, alpha=alpha)
        else:
            res = mk.original_test(y.values, alpha=alpha)

    except Exception as e:
        return {"error": str(e), "mk_mode_used": mode}

    return {
        "mk_mode_used": mode,
        "trend": getattr(res, "trend", None),
        "p": getattr(res, "p", None),
    }

def _plot_station_grid(
    station: str,
    frequency: str,
    series_by_var: dict[str, pd.Series],
    units_by_var: dict[str, str],
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

        unit_lbl = units_by_var.get(var, "undefined")
        ax.set_ylabel(f"{var} [{unit_lbl}]")

        plot_var = var
        plot_season = None

        if frequency == "seasonal_by_season":
            for season_name in ["winter", "spring", "summer", "autumn"]:
                suffix = f"_{season_name}"
                if var.endswith(suffix):
                    plot_var = var[: -len(suffix)]
                    plot_season = season_name
                    break

        row = mk_df_station[mk_df_station["variable"] == plot_var]

        if plot_season is not None and "season" in row.columns:
            row = row[row["season"] == plot_season]

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

    fig.suptitle(f"{station}", fontsize=18, y=0.99)
    fig.tight_layout(rect=[0, 0, 1, 0.985])

    ensure_dirs(out_png.parent)
    plt.savefig(out_png, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _plot_trend_matrix_for_variable(
    df_freq: pd.DataFrame,
    *,
    variable: str,
    frequency: str,
    stations_order: list[str],
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

    y_map = {st: i for i, st in enumerate(stations_order)}

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

    mask = np.isfinite(np.array(ys, dtype=float))
    xs = np.array(xs, dtype=float)[mask]
    ys = np.array(ys, dtype=float)[mask]
    cols = np.array(cols, dtype=object)[mask]
    alphas = np.array(alphas, dtype=float)[mask]

    if xs.size == 0:
        return

    plt.figure(figsize=(6.5, max(2.5, 0.45 * len(stations_order))))
    ax = plt.gca()

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
    ensure_dirs(out_png.parent)
    plt.savefig(out_png, dpi=300, bbox_inches="tight")
    plt.close()


def analyze_trends(
    cfg: dict[str, Any],
    *,
    frequency: str = "config",
    mk_mode: str = "auto",
    stations: list[str] | None = None,
) -> dict[str, Path]:

    vars_to_test: list[str] = cfg.get("variables", [])
    if not vars_to_test:
        raise ValueError("No variables in cfg['variables'].")

    out_root = resolve_path(cfg["output_dir"])
    figures_dir = cfg.get("results", {}).get("figures_dir", "figures")
    tables_dir = cfg.get("results", {}).get("tables_dir", "tables")
    combined_name = cfg.get("results", {}).get("combined_table_name", "mk_results.xlsx")

    trend_opt = cfg.get("trend_options", {})
    alpha = float(trend_opt.get("alpha", 0.05))
    min_points = trend_opt.get("min_points", {"monthly": 36, "annual": 5, "seasonal_by_season": 12})

    period_cfg = (
        trend_opt.get("period", {})
        if isinstance(trend_opt.get("period", {}), dict)
        else {}
    )

    start_year = period_cfg.get("start_year")
    end_year = period_cfg.get("end_year")
    require_full_period = bool(
        period_cfg.get("require_full_period", False)
    )

    start_year = int(start_year) if start_year is not None else None
    end_year = int(end_year) if end_year is not None else None

    if (
            start_year is not None
            and end_year is not None
            and start_year > end_year
    ):
        raise ValueError(
            "trend_options.period.start_year must be <= end_year"
        )

    unit_opt = cfg.get("unit_options", {})
    non_mass_vars = set(unit_opt.get("non_mass_vars", []))
    undefined_unit_label = str(unit_opt.get("undefined_unit_label", "undefined"))

    plot_opt = cfg.get("plot_options", {})
    ncols = int(plot_opt.get("ncols", 3))
    non_sig_alpha = float(plot_opt.get("non_sig_alpha", 0.25))

    if frequency == "config":
        freq_list = cfg.get(
            "trend_frequencies",
            ["monthly", "annual"]
        )

    elif frequency == "both":
        freq_list = ["monthly", "annual"]

    elif frequency in (
            "monthly",
            "annual",
            "seasonal_by_season",
    ):
        freq_list = [frequency]

    else:
        raise ValueError(
            f"Unsupported trend frequency: {frequency}"
        )

    inputs = cfg.get("inputs", {})
    daily_inputs = inputs.get("daily", [])

    if not isinstance(daily_inputs, list):
        raise ValueError("cfg['inputs']['daily'] must be a list.")

    stations_cfg = cfg.get("stations", cfg.get("site_li", []))
    if not stations_cfg and stations is None:
        raise ValueError("No stations provided.")

    results_by_station: dict[
        str,
        dict[str, pd.DataFrame],
    ] = {}
    all_rows: list[dict[str, Any]] = []

    for freq in freq_list:
        print(f"\n[trends] Frequency: {freq}")

        mk_rows_all: list[dict[str, Any]] = []
        stations_order_all: list[str] = []

        for fcfg in daily_inputs:
            data_type = str(fcfg.get("data_type", "river"))
            mode = str(fcfg.get("mode", "folder")).lower()
            src_path = resolve_input_source(fcfg.get("path", ""))
            aggregation = str(fcfg.get("aggregation", "mean"))

            variable_name_rules = fcfg.get(
                "variable_name_rules",
                {},
            )

            print(
                f"[trends] Source: {fcfg.get('name')} | "
                f"type={data_type} | aggregation={aggregation}"
            )

            if isinstance(src_path, Path) and not src_path.exists():
                print(f"[trends] Missing daily input: {src_path}")
                continue

            station_dim = fcfg.get("station_dim")
            station_coord = fcfg.get("station_coord")

            if stations is not None:
                stations_local = stations
            elif isinstance(stations_cfg, dict):
                stations_local = stations_cfg.get(data_type, [])
            else:
                stations_local = stations_cfg

            if isinstance(stations_local, str):
                stations_local = [stations_local]

            if isinstance(stations_local, (list, tuple)) and len(stations_local) == 1:
                if str(stations_local[0]).strip().lower() == "all":
                    stations_local = ["all"]

            if mode == "file" and stations_local == ["all"]:
                with xr.open_dataset(src_path) as ds:
                    if not station_dim:
                        raise ValueError(
                            "station_dim is required when mode='file'"
                        )

                    stations_local = _list_stations_from_ds(
                        ds,
                        station_dim=str(station_dim),
                        station_coord=station_coord,
                    )

            stations_order_all.extend([str(x) for x in stations_local])

            for st in stations_local:

                series_by_var: dict[str, pd.Series] = {}
                units_by_var: dict[str, str] = {}

                for var in vars_to_test:
                    source_var = _resolve_variable_name(
                        var,
                        variable_name_rules,
                    )

                    if mode == "folder":
                        nc_path = _find_site_file(src_path, st)
                        if nc_path is None:
                            continue

                        s, unit = _open_series_and_unit_from_nc(
                            nc_path,
                            source_var,
                        )

                    else:
                        s, unit = _open_series_and_unit_from_nc(
                            src_path,
                            source_var,
                            station=st,
                            station_dim=str(station_dim) if station_dim else None,
                            station_coord=station_coord,
                            depth_dim=fcfg.get("depth_dim"),
                            depth_selection=fcfg.get("depth_selection"),
                        )

                    if s is None:
                        continue

                    s = s.dropna()

                    raw_start = None
                    raw_end = None

                    if start_year is not None:
                        if freq == "seasonal_by_season":
                            # Winter of start_year may need December of the previous year.
                            raw_start = f"{start_year - 1}-12-01"
                        else:
                            raw_start = f"{start_year}-01-01"

                    if end_year is not None:
                        raw_end = f"{end_year}-12-31"

                    s = _slice_period(
                        s,
                        start=raw_start,
                        end=raw_end,
                    )

                    if s.empty:
                        continue

                    aggregation_requirements = fcfg.get(
                        "aggregation_requirements", {}
                    )

                    coverage = aggregation_requirements.get(
                        freq, {}
                    )

                    unit_display = _display_unit_for_plot(
                        unit,
                        frequency=freq,
                        aggregation=aggregation,
                        var=var,
                        non_mass_vars=non_mass_vars,
                        undefined_label=undefined_unit_label,
                    )

                    if freq == "seasonal_by_season":
                        seasonal_series = _aggregate_seasonal_by_season(
                            s,
                            how=aggregation,
                            coverage=coverage
                        )

                        if not seasonal_series:
                            continue

                        for season_name, s_season in seasonal_series.items():
                            s_season = _slice_years(
                                s_season,
                                start_year=start_year,
                                end_year=end_year,
                            )

                            if s_season.empty:
                                continue


                            plot_var_name = f"{var}_{season_name}"
                            series_by_var[plot_var_name] = s_season
                            units_by_var[plot_var_name] = unit_display

                            n_valid = int(s_season.shape[0])
                            min_n = int(min_points.get(freq, 10))

                            first = float(s_season.iloc[0]) if n_valid else np.nan
                            last = float(s_season.iloc[-1]) if n_valid else np.nan
                            mean = float(s_season.mean()) if n_valid else np.nan
                            median = float(s_season.median()) if n_valid else np.nan
                            std_dev = float(s_season.std()) if n_valid else np.nan
                            iqr = float(s_season.quantile(0.75) - s_season.quantile(0.25)) if n_valid else np.nan
                            period = _period_str(s_season.index)

                            base_row = {
                                "station_id": st,
                                "period": period,
                                "variable": var,
                                "season": season_name,
                                "unit_display": unit_display,
                                "n_vals": n_valid,
                                "first": first,
                                "last": last,
                                "mean": mean,
                                "median": median,
                                "std_dev": std_dev,
                                "iqr": iqr,
                                "frequency": freq,
                            }

                            full_period = _covers_full_period(
                                s_season,
                                start_year=start_year,
                                end_year=end_year,
                            )

                            base_row["full_period"] = full_period

                            if require_full_period and not full_period:
                                mk_rows_all.append({
                                    **base_row,
                                    "mk_p_val": np.nan,
                                    "mk_trend": "insufficient_data",
                                    "sen_slp": np.nan,
                                    "sen_incpt": np.nan,
                                    "sen_trend": "insufficient_data",
                                    "mk_mode_used": "n/a",
                                })
                                continue

                            if n_valid < min_n:
                                mk_rows_all.append({
                                    **base_row,
                                    "mk_p_val": np.nan,
                                    "mk_trend": "insufficient_data",
                                    "sen_slp": np.nan,
                                    "sen_incpt": np.nan,
                                    "sen_trend": "insufficient_data",
                                    "mk_mode_used": "n/a",
                                })
                                continue

                            res = _mk_test(s_season, frequency="seasonal_by_season", mk_mode="original", alpha=alpha)

                            if res is None:
                                continue

                            if "error" in res:
                                mk_rows_all.append({
                                    **base_row,
                                    "mk_p_val": np.nan,
                                    "mk_trend": "error",
                                    "sen_slp": np.nan,
                                    "sen_incpt": np.nan,
                                    "sen_trend": "error",
                                    "mk_mode_used": res.get("mk_mode_used"),
                                    "error": res.get("error"),
                                })
                                continue

                            pval = res.get("p", np.nan)
                            mk_trend = _mk_trend_label(res.get("trend"))

                            x = _x_for_fit(s_season, "seasonal_by_season")
                            sen_slp, sen_incpt = _sen_slope_intercept(s_season, x)
                            sen_trend = _classify_sen_trend(
                                sen_slp,
                                float(pval) if np.isfinite(pval) else np.nan,
                                alpha,
                            )

                            mk_rows_all.append({
                                **base_row,
                                "mk_p_val": float(pval) if np.isfinite(pval) else np.nan,
                                "mk_trend": mk_trend,
                                "sen_slp": sen_slp,
                                "sen_incpt": sen_incpt,
                                "sen_trend": sen_trend,
                                "mk_mode_used": res.get("mk_mode_used"),
                            })

                        continue

                    s = _aggregate_series(
                        s,
                        frequency=freq,
                        how=aggregation,
                        coverage=coverage,
                    )

                    s = _slice_years(
                        s,
                        start_year=start_year,
                        end_year=end_year,
                    )

                    if s.empty:
                        continue

                    series_by_var[var] = s
                    units_by_var[var] = unit_display

                    n_valid = int(s.shape[0])
                    min_n = int(min_points.get(freq, 5))

                    first = float(s.iloc[0]) if n_valid else np.nan
                    last = float(s.iloc[-1]) if n_valid else np.nan
                    mean = float(s.mean()) if n_valid else np.nan
                    median = float(s.median()) if n_valid else np.nan
                    std_dev = float(s.std()) if n_valid else np.nan
                    iqr = float(s.quantile(0.75) - s.quantile(0.25)) if n_valid else np.nan
                    period = _period_str(s.index)

                    base_row = {
                        "station_id": st,
                        "period": period,
                        "variable": var,
                        "season": None,
                        "unit_display": unit_display,
                        "n_vals": n_valid,
                        "first": first,
                        "last": last,
                        "mean": mean,
                        "median": median,
                        "std_dev": std_dev,
                        "iqr": iqr,
                        "frequency": freq,
                    }

                    full_period = _covers_full_period(
                        s,
                        start_year=start_year,
                        end_year=end_year,
                    )

                    base_row["full_period"] = full_period

                    if require_full_period and not full_period:
                        mk_rows_all.append({
                            **base_row,
                            "mk_p_val": np.nan,
                            "mk_trend": "insufficient_data",
                            "sen_slp": np.nan,
                            "sen_incpt": np.nan,
                            "sen_trend": "insufficient_data",
                            "mk_mode_used": "n/a",
                        })
                        continue

                    if n_valid < min_n:
                        mk_rows_all.append({
                            **base_row,
                            "mk_p_val": np.nan,
                            "mk_trend": "insufficient_data",
                            "sen_slp": np.nan,
                            "sen_incpt": np.nan,
                            "sen_trend": "insufficient_data",
                            "mk_mode_used": "n/a",
                        })
                        continue

                    res = _mk_test(s, frequency=freq, mk_mode=mk_mode, alpha=alpha)

                    if res is None:
                        continue

                    if "error" in res:
                        mk_rows_all.append({
                            **base_row,
                            "mk_p_val": np.nan,
                            "mk_trend": "error",
                            "sen_slp": np.nan,
                            "sen_incpt": np.nan,
                            "sen_trend": "error",
                            "mk_mode_used": res.get("mk_mode_used"),
                            "error": res.get("error"),
                        })
                        continue

                    pval = res.get("p", np.nan)
                    mk_trend = _mk_trend_label(res.get("trend"))

                    x = _x_for_fit(s, freq)
                    sen_slp, sen_incpt = _sen_slope_intercept(s, x)
                    sen_trend = _classify_sen_trend(
                        sen_slp,
                        float(pval) if np.isfinite(pval) else np.nan,
                        alpha,
                    )

                    mk_rows_all.append({
                        **base_row,
                        "mk_p_val": float(pval) if np.isfinite(pval) else np.nan,
                        "mk_trend": mk_trend,
                        "sen_slp": sen_slp,
                        "sen_incpt": sen_incpt,
                        "sen_trend": sen_trend,
                        "mk_mode_used": res.get("mk_mode_used"),
                    })
                if series_by_var:
                    mk_df_station = pd.DataFrame(
                        [
                            r for r in mk_rows_all
                            if r["station_id"] == st and r["frequency"] == freq
                        ]
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

        if mk_rows_all:
            df_all = pd.DataFrame(mk_rows_all)
            all_rows.extend(mk_rows_all)

            preferred = [
                "period", "full_period", "station_id", "variable", "n_vals",
                "first", "last", "mean", "median", "std_dev", "iqr",
                "mk_p_val", "mk_trend", "sen_slp", "sen_incpt", "sen_trend",
            ]
            extras = [c for c in df_all.columns if c not in preferred]
            df_all = df_all[preferred + extras]

            for st in df_all["station_id"].unique():
                results_by_station.setdefault(st, {})
                results_by_station[st][freq] = df_all[df_all["station_id"] == st].copy()

            matrix_keys = []

            for var in vars_to_test:
                if freq == "seasonal_by_season":
                    for season_name in ["winter", "spring", "summer", "autumn"]:
                        matrix_keys.append((var, season_name))
                else:
                    matrix_keys.append((var, None))

            for var, season_name in matrix_keys:
                if season_name is None:
                    df_var = df_all[df_all["variable"] == var].copy()
                    plot_variable_name = var
                    out_name = f"{var}.png"
                else:
                    df_var = df_all[
                        (df_all["variable"] == var) &
                        (df_all["season"] == season_name)
                        ].copy()
                    plot_variable_name = f"{var}_{season_name}"
                    out_name = f"{var}_{season_name}.png"

                    df_var = df_var.copy()
                    df_var["variable"] = plot_variable_name

                if df_var.empty:
                    continue

                stations_order_var = [
                    st for st in stations_order_all
                    if st in set(df_var["station_id"].astype(str))
                ]

                if not stations_order_var:
                    continue

                out_png = out_root / figures_dir / freq / "trend_matrix" / out_name

                _plot_trend_matrix_for_variable(
                    df_var,
                    variable=plot_variable_name,
                    frequency=freq,
                    stations_order=stations_order_var,
                    out_png=out_png,
                    alpha=alpha,
                    non_sig_alpha=non_sig_alpha,
                )

    if all_rows:
        df_new = pd.DataFrame(all_rows)

        out_all = out_root / tables_dir / combined_name
        ensure_dirs(out_all.parent)

        # Recreate the combined workbook for each run.
        # One sheet is written for each requested trend frequency.
        with pd.ExcelWriter(out_all, engine="openpyxl") as xl:
            for freq in sorted(df_new["frequency"].unique()):
                df_freq = df_new[df_new["frequency"] == freq].copy()
                if not df_freq.empty:
                    df_freq.to_excel(xl, sheet_name=freq, index=False)

        print(f"[trends] Saved combined table: {out_all}")

    write_per_station = bool(cfg.get("results", {}).get("write_per_station_tables", True))

    written_excels: dict[str, Path] = {}

    if write_per_station:
        for st, block in results_by_station.items():
            out_xlsx = out_root / tables_dir / f"{st}_mk_results.xlsx"

            ensure_dirs(out_xlsx.parent)
            with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xl:
                for freq, df_freq in block.items():
                    if df_freq is not None and not df_freq.empty:
                        df_freq.to_excel(xl, sheet_name=freq, index=False)

            written_excels[st] = out_xlsx
            print(f"[trends] Saved: {out_xlsx}")

    return written_excels
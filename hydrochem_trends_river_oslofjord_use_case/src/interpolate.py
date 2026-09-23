from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

from pygam import LinearGAM, s, te
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from .export_netcdf import export_dataset
from .utils import (
    ensure_dirs,
    merge_daily_discharge_and_chemistry,
    netcdf_to_dataframe,
    resolve_input_source,
    resolve_path,
    save_or_show_plot,
    standardize_time_and_station,
)


# ----------------------------- Helper functions -----------------------------
def meta_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    return (cfg.get("meta") or {}).copy()


def read_meta_value(
        df_like,
        m: dict[str, Any],
        key: str,
) -> Any | None:
    """
    Pull a meta value from DataFrame (first non-null) or constant.
    Works for both pandas DataFrame or xarray Dataset via .to_dataframe().
    """
    spec = m.get(key)
    if not spec:
        return None
    if "from_col" in spec:
        col = spec["from_col"]
        if isinstance(df_like, pd.DataFrame):
            df = df_like
        else:
            df = df_like.to_dataframe().reset_index()
        if col not in df.columns:
            raise KeyError(f"meta.{key}.from_col='{col}' not found.")
        s = df[col].dropna()
        return s.iloc[0] if not s.empty else None
    if "value" in spec:
        return spec["value"]
    return None


def render_template(
        s: str | None,
        ctx: dict[str, Any],
) -> str | None:
    if not s:
        return None
    return s.format(**ctx)


def method_pretty_name(suffix: str) -> str:
    mapping = {
        "annual_gam": "Annual GAM interpolation",
        "monthly_regres": "Monthly regression interpolation",
        "monthly_interp": "Monthly median interpolation",
        "linear_interp": "Linear interpolation",
    }
    return mapping.get(suffix, suffix.replace("_", " ").title())


def build_method_comment(
    var: str,
    selected_col: str,
    fallback_cols: list[str] | None,
) -> str:
    """Build a concise description of how the final daily series was produced."""

    selected_suffix = selected_col.replace(f"{var}_", "")
    selected_txt = method_pretty_name(selected_suffix)

    if not fallback_cols:
        return f"primary: {selected_txt}"

    fallback_txt = ", ".join(
        method_pretty_name(col.replace(f"{var}_", ""))
        for col in fallback_cols
    )

    return (
        f"primary: {selected_txt}; "
        f"fallbacks: {fallback_txt}"
    )

def build_global_attrs(
        cfg: dict[str, Any],
        station_id: str,
        time_name: str,
) -> dict[str, str]:
    md = cfg.get("metadata", {}) or {}
    md_tpl = md.get("templates", {}) or {}
    md_defaults = md.get("defaults", {}) or {}
    md_timestamps = md.get("timestamps", {}) or {}

    # context for templates
    ctx = {
        "station_id": station_id,
        "time_name": time_name,
    }

    # base provided attrs
    base = dict(cfg.get("global_metadata_config", {}) or {})

    # apply defaults only if missing
    for k, v in md_defaults.items():
        base.setdefault(k, v)

    # template-derived fields
    for k in ["title", "title_no", "summary", "summary_no"]:
        if k not in base:
            rendered = render_template(md_tpl.get(k), ctx)
            if rendered:
                base[k] = rendered

    # timestamps
    if md_timestamps.get("date_created") == "auto":
        base["date_created"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        base.setdefault("date_created", md_timestamps.get("date_created"))

    # ensure strings
    return {k: str(v) for k, v in base.items()}


# ------------------------- interpolation -------------------------
def interpolate_with_gap_limit(
        series: pd.Series,
        max_gap: int,
        method: str = "linear",
        order: int | None = None,
) -> pd.Series:
    """Interpolate only across gaps up to max_gap samples."""
    if method in ["spline", "polynomial"] and order is None:
        raise ValueError(f"Interpolation method '{method}' requires 'order'.")

    series = series.sort_index()

    interpolated = series.interpolate(
        method=method,
        order=order,
        limit_area="inside",
    )

    missing = series.isna()
    groups = missing.ne(missing.shift()).cumsum()

    for _, group in series[missing].groupby(groups[missing]):
        if len(group) > max_gap:
            interpolated.loc[group.index] = np.nan

    return interpolated


def interpolate_station_df(
        df: pd.DataFrame,
        variables: list[str],
        date_col="date",
        meta_cols: list[str] | None = None,
        max_gap=30,
        method="linear",
        order: int | None = None,
) -> pd.DataFrame:
    """Resample to daily frequency and interpolate  variables with a maximum gap limit."""

    if not meta_cols:
        raise ValueError("meta_cols required for ffill/bfill (e.g., ['river_name']).")
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col])
    out = out.drop_duplicates(subset=date_col).set_index(date_col).resample("D").asfreq()

    for c in meta_cols:
        if c in out.columns:
            out[c] = out[c].ffill().bfill()

    for var in variables:
        if var in out.columns:
            out[f"{var}_linear_interp"] = interpolate_with_gap_limit(out[var], max_gap=max_gap, method=method,
                                                                     order=order)
    return out.reset_index()


def compute_gam(
        df: pd.DataFrame,
        var: str,
        discharge_col="discharge",
        date_col="date",
        station_name: str | None = None,
        n_splines_xy: tuple[int, int] = (10, 20),
        lam_grid: np.ndarray | None = None,
        verbose: bool = True,
) -> pd.DataFrame | None:
    """Fit a GAM using discharge and day-of-year and predict within the observed time range."""

    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col])
    out["doy"] = out[date_col].dt.dayofyear

    train = out.dropna(subset=[var, discharge_col, "doy"]).copy()
    if train.empty:
        if verbose:
            print(f"No training data for {station_name} -> {var}")
        return None

    X = train[[discharge_col, "doy"]].values
    y = train[var].values

    if lam_grid is None:
        lam_grid = np.logspace(-3, 3, 7)

    model = LinearGAM(te(0, 1, n_splines=list(n_splines_xy), spline_order=[3, 3]) + s(1, basis="cp"))
    try:
        gam = model.gridsearch(X, y, lam=lam_grid)
    except Exception as e:
        if verbose:
            print(f"GAM failed for {station_name} → {var}: {e}")
        return None

    first, last = train[date_col].min(), train[date_col].max()
    mask = (out[discharge_col].notna() & out["doy"].notna() & (out[date_col] >= first) & (out[date_col] <= last))
    Xp = out.loc[mask, [discharge_col, "doy"]].values
    yp = gam.predict(Xp)
    yp[yp < 0] = 0

    out[f"{var}_annual_gam"] = np.nan
    out.loc[mask, f"{var}_annual_gam"] = yp

    # train fit metrics
    yhat = gam.predict(X)
    mae = mean_absolute_error(y, yhat)
    mse = mean_squared_error(y, yhat)
    r2 = r2_score(y, yhat)
    if verbose:
        print(f"GAM {station_name} -> {var}: MAE={mae:.2f}, MSE={mse:.2f}, fit R^2={r2:.3f}")
    return out


def apply_gam_to_df(
        df: pd.DataFrame,
        variables: list[str],
        discharge_col="discharge",
        date_col="date",
        station_name="",
        n_splines_xy=(10, 20),
        lam_grid=None,
        verbose: bool = True,
) -> pd.DataFrame:
    out = df.copy()
    for var in variables:
        if var in out.columns:
            gdf = compute_gam(
                out, var,
                discharge_col=discharge_col,
                date_col=date_col,
                station_name=station_name,
                n_splines_xy=n_splines_xy,
                lam_grid=lam_grid,
                verbose=verbose,
            )
            if gdf is not None:
                out = gdf
    return out


def monthly_to_daily_for_year(monthly_df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Convert monthly values to a daily series for a given year using time interpolation."""

    tmp = monthly_df.copy()
    tmp.index = pd.to_datetime(tmp.index.astype(str) + f"-{year}", format="%m-%Y")
    tmp.index = tmp.index + pd.offsets.MonthBegin(1) - pd.Timedelta("17D")

    daily_idx = pd.date_range(f"{year}-01-01", f"{year}-12-31")
    tmp = tmp.reindex(daily_idx).resample("D").asfreq()

    # guardrails: seed ends using mid-month
    try:
        jan15 = tmp.loc[f"{year}-01-15"]
        dec15 = tmp.loc[f"{year}-12-15"]
        end_val = (jan15 - dec15) / 2 + dec15
        tmp.loc[f"{year}-01-01"] = end_val
        tmp.loc[f"{year}-12-31"] = end_val
    except KeyError:
        pass

    tmp = tmp.interpolate(method="time")
    tmp = tmp.map(lambda x: 0 if pd.notna(x) and x < 0 else x)
    return tmp


def monthly_medians_to_daily_all_years(station_df: pd.DataFrame, variables: list[str], date_col="date") -> pd.DataFrame:
    """Compute monthly medians per year and interpolate to daily values."""

    s = station_df.copy()
    s[date_col] = pd.to_datetime(s[date_col])
    s = s.set_index(date_col)
    daily_chunks = []

    for year in sorted(s.index.year.unique()):
        ydf = s[s.index.year == year]
        monthly = ydf[variables].resample("ME").median()
        monthly.index = monthly.index.month
        daily_year = monthly_to_daily_for_year(monthly, year)
        daily_chunks.append(daily_year)

    if not daily_chunks:
        return pd.DataFrame(index=pd.DatetimeIndex([], name=date_col))

    daily = pd.concat(daily_chunks)
    daily = daily.rename(columns=lambda c: f"{c}_monthly_interp")
    daily.index.name = date_col
    return daily.reset_index()


def monthwise_loglog_regressions(
        station_df: pd.DataFrame,
        variables: list[str],
        discharge_col: str = "discharge",
        date_col: str = "date",
        min_points: int = 5,
        bias_correct: bool = True,
) -> pd.DataFrame:
    """Fit per-month log–log regressions of var vs discharge and predict daily values."""

    s = station_df.copy()
    s[date_col] = pd.to_datetime(s[date_col])
    s = s.drop_duplicates(subset=[date_col]).sort_values(date_col)

    out_all = []

    for var in variables:
        if var not in s.columns or discharge_col not in s.columns:
            continue

        sub = s[[date_col, "river_name", discharge_col, var]].copy().set_index(date_col)

        obs = sub[var].dropna()
        if obs.empty:
            continue
        first_date, last_date = obs.index.min(), obs.index.max()
        sub = sub.loc[first_date:last_date].copy()

        sub["month"] = sub.index.month

        for _, grp in sub.groupby("month"):
            # training data: need positive values for log
            train = grp[[discharge_col, var]].dropna().copy()
            train = train[(train[discharge_col] > 0) & (train[var] > 0)]
            if train.shape[0] < min_points:
                continue

            train["log_q"] = np.log10(train[discharge_col].values)
            train["log_y"] = np.log10(train[var].values)

            X = train[["log_q"]].values
            y = train["log_y"].values
            mdl = LinearRegression().fit(X, y)

            g2 = grp.copy()
            pred = np.full(shape=(len(g2),), fill_value=np.nan, dtype=float)

            ok = g2[discharge_col].notna() & (g2[discharge_col] > 0)
            X_all = np.log10(g2.loc[ok, discharge_col].values).reshape(-1, 1)
            yhat_log = mdl.predict(X_all)

            if bias_correct:
                resid = y - mdl.predict(X)
                corr = float(np.mean(10 ** resid))
            else:
                corr = 1.0

            pred_vals = (10 ** yhat_log) * corr
            pred[ok.values] = pred_vals

            g2[f"{var}_monthly_regres"] = pred
            out_all.append(g2.reset_index())

    if not out_all:
        return pd.DataFrame(columns=station_df.columns)

    return pd.concat(out_all, ignore_index=True)


def _series_from_candidate(
        df: pd.DataFrame | None,
        col: str,
        date_col: str = "date",
) -> pd.Series:
    if df is None or df.empty or col not in df.columns:
        return pd.Series(dtype=float)
    tmp = df[[date_col, col]].copy()
    tmp[date_col] = pd.to_datetime(tmp[date_col])
    tmp = tmp.dropna(subset=[date_col])
    if tmp.empty:
        return pd.Series(dtype=float)
    return tmp.groupby(date_col)[col].mean().sort_index()


def _candidate_predictions_for_var(
        station_df: pd.DataFrame,
        var: str,
        *,
        station_name: str,
        linear_cfg: dict[str, Any],
        gam_cfg: dict[str, Any],
) -> dict[str, pd.Series]:
    preds: dict[str, pd.Series] = {}

    linear_df = interpolate_station_df(
        station_df,
        variables=[var],
        date_col="date",
        meta_cols=["river_name"],
        max_gap=int(linear_cfg.get("max_gap", 90)),
        method=str(linear_cfg.get("method", "linear")),
        order=linear_cfg.get("order", None),
    )
    preds["linear_interp"] = _series_from_candidate(
        linear_df,
        f"{var}_linear_interp",
    )

    gam_df = compute_gam(
        station_df,
        var,
        discharge_col="discharge",
        date_col="date",
        station_name=station_name,
        n_splines_xy=tuple(gam_cfg.get("n_splines_xy", [10, 20])),
        lam_grid=np.array(gam_cfg["lam_grid"]) if gam_cfg.get("lam_grid") else None,
        verbose=False,
    )
    preds["annual_gam"] = _series_from_candidate(
        gam_df,
        f"{var}_annual_gam",
    )

    reg_df = monthwise_loglog_regressions(
        station_df,
        [var],
        discharge_col="discharge",
        date_col="date",
    )
    preds["monthly_regres"] = _series_from_candidate(
        reg_df,
        f"{var}_monthly_regres",
    )

    month_df = monthly_medians_to_daily_all_years(
        station_df,
        [var],
        date_col="date",
    )
    preds["monthly_interp"] = _series_from_candidate(
        month_df,
        f"{var}_monthly_interp",
    )

    return preds


def _validation_blocks(
        obs_dates: pd.DatetimeIndex,
        *,
        n_repeats: int,
        block_size: int,
) -> list[pd.DatetimeIndex]:
    obs_dates = pd.DatetimeIndex(obs_dates).sort_values()
    if len(obs_dates) <= 2:
        return []

    interior = obs_dates[1:-1]
    block_size = max(1, min(int(block_size), len(interior)))
    max_start = len(interior) - block_size

    if max_start <= 0:
        return [pd.DatetimeIndex(interior)]

    n_blocks = max(1, min(int(n_repeats), max_start + 1))
    starts = np.unique(
        np.linspace(0, max_start, num=n_blocks, dtype=int)
    )
    return [
        pd.DatetimeIndex(interior[start:start + block_size])
        for start in starts
    ]


def validate_interpolation_methods(
        station_df: pd.DataFrame,
        var: str,
        *,
        station_name: str,
        linear_cfg: dict[str, Any],
        gam_cfg: dict[str, Any],
        n_repeats: int,
        block_size: int,
        min_observations: int,
) -> tuple[dict[str, dict[str, float]], pd.DataFrame]:
    tmp = station_df.copy()
    tmp["date"] = pd.to_datetime(tmp["date"])

    obs = (
        tmp[["date", var]]
        .dropna()
        .groupby("date")[var]
        .mean()
        .sort_index()
    )

    if len(obs) < min_observations:
        return {}, pd.DataFrame(
            columns=["method", "date", "observed", "predicted"]
        )

    blocks = _validation_blocks(
        pd.DatetimeIndex(obs.index),
        n_repeats=n_repeats,
        block_size=block_size,
    )
    if not blocks:
        return {}, pd.DataFrame(
            columns=["method", "date", "observed", "predicted"]
        )

    records: list[dict[str, Any]] = []
    withheld_dates: set[pd.Timestamp] = set()

    for block in blocks:
        train_df = tmp.copy()
        mask = train_df["date"].isin(block)
        train_df.loc[mask, var] = np.nan
        withheld_dates.update(pd.Timestamp(d) for d in block)

        predictions = _candidate_predictions_for_var(
            train_df,
            var,
            station_name=station_name,
            linear_cfg=linear_cfg,
            gam_cfg=gam_cfg,
        )

        truth = obs.reindex(block)

        for suffix, pred in predictions.items():
            pred_block = pred.reindex(block)
            for date in block:
                observed_value = truth.get(date, np.nan)
                predicted_value = pred_block.get(date, np.nan)
                records.append(
                    {
                        "method": suffix,
                        "date": pd.Timestamp(date),
                        "observed": observed_value,
                        "predicted": predicted_value,
                    }
                )

    validation_df = pd.DataFrame(records)
    if validation_df.empty:
        return {}, validation_df

    validation_df = (
        validation_df
        .groupby(["method", "date"], as_index=False)
        .agg(
            observed=("observed", "first"),
            predicted=("predicted", "mean"),
        )
    )

    total_withheld = len(withheld_dates)
    metrics: dict[str, dict[str, float]] = {}

    for suffix, grp in validation_df.groupby("method"):
        valid = grp.dropna(subset=["observed", "predicted"]).copy()
        n = len(valid)
        coverage = n / total_withheld if total_withheld else 0.0

        if n:
            errors = valid["predicted"] - valid["observed"]

            mae = float(
                mean_absolute_error(
                    valid["observed"],
                    valid["predicted"],
                )
            )

            rmse = float(
                np.sqrt(
                    mean_squared_error(
                        valid["observed"],
                        valid["predicted"],
                    )
                )
            )

            bias = float(errors.mean())

            r2 = (
                float(
                    r2_score(
                        valid["observed"],
                        valid["predicted"],
                    )
                )
                if n >= 2
                else np.nan
            )

            obs_iqr = float(
                valid["observed"].quantile(0.75)
                - valid["observed"].quantile(0.25)
            )

            nrmse = (
                rmse / obs_iqr
                if np.isfinite(obs_iqr) and obs_iqr > 0
                else np.nan
            )

        else:
            mae = np.nan
            rmse = np.nan
            bias = np.nan
            r2 = np.nan
            nrmse = np.nan

        metrics[suffix] = {
            "r2": r2,
            "mae": mae,
            "rmse": rmse,
            "nrmse": nrmse,
            "bias": bias,
            "n": float(n),
            "coverage": float(coverage),
        }

    return metrics, validation_df


def plot_validation_qc(
        validation_df: pd.DataFrame,
        metrics: Mapping[str, dict[str, float]],
        *,
        var: str,
        station: str,
        pars_meta_df: pd.DataFrame,
        save_path: Path,
) -> None:
    valid_df = validation_df.dropna(subset=["observed", "predicted"]).copy()
    if valid_df.empty:
        return

    unit = ""
    if var in set(pars_meta_df["parameter_name"]):
        unit_val = pars_meta_df.loc[
            pars_meta_df["parameter_name"] == var,
            "unit",
        ].values[0]
        unit = f" ({unit_val})"

    plt.figure(figsize=(7, 7))

    all_vals = pd.concat(
        [valid_df["observed"], valid_df["predicted"]],
        ignore_index=True,
    ).dropna()
    vmin = float(all_vals.min())
    vmax = float(all_vals.max())
    plt.plot([vmin, vmax], [vmin, vmax], linestyle="--", label="1:1")

    for suffix, grp in valid_df.groupby("method"):
        score = metrics.get(suffix, {})
        r2 = score.get("r2", np.nan)
        rmse = score.get("rmse", np.nan)
        nrmse = score.get("nrmse", np.nan)
        coverage = score.get("coverage", np.nan)

        label = method_pretty_name(suffix)
        if np.isfinite(r2):
            label += f" | R2={r2:.2f}"
        if np.isfinite(rmse):
            label += f" | RMSE={rmse:.3g}"
        if np.isfinite(nrmse):
            label += f" | nRMSE={nrmse:.2f}"
        if np.isfinite(coverage):
            label += f" | cov={coverage:.0%}"

        plt.scatter(
            grp["observed"],
            grp["predicted"],
            s=35,
            alpha=0.7,
            label=label,
        )

    plt.xlabel(f"Observed {var}{unit}")
    plt.ylabel(f"Withheld prediction {var}{unit}")
    plt.title(f"{var} validation at {station}")
    plt.grid(True)
    plt.legend(fontsize=8)
    plt.tight_layout()
    save_or_show_plot(save_path=save_path, dpi=300)


# ------------------------------ plotting ------------------------------
def plot_qc(
        df: pd.DataFrame,
        var: str,
        station: str,
        method_col: str,
        method_label: str,
        pars_meta_df: pd.DataFrame,
        unit_par_col="parameter_name",
        unit_unit_col="unit",
        station_col="river_name",
        date_col="date",
        save_path: Path | None = None,
) -> None:
    unit = ""
    if var in set(pars_meta_df[unit_par_col]):
        unit_val = pars_meta_df.loc[pars_meta_df[unit_par_col] == var, unit_unit_col].values[0]
        unit = f" ({unit_val})"

    df_s = df[df[station_col] == station].copy()
    plt.figure(figsize=(12, 5))
    plt.scatter(
        df_s[date_col],
        df_s[var],
        label="Observations",
        color="black",
        alpha=0.7,
        s=30
    )

    if method_col in df_s.columns:
        plt.plot(
            df_s[date_col],
            df_s[method_col],
            label="Final estimate",
            lw=2
        )

    title = f"{var} at {station} — {method_label}"
    plt.title(title)
    plt.xlabel(" ")
    plt.ylabel(f"{var}{unit}")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    save_or_show_plot(
        save_path=save_path,
        dpi=300,
    )


# ------------------------------- main --------------------------------
def interpolate(cfg: dict[str, Any]) -> list[Path]:
    """
    Run full daily interpolation for ONE river/station, driven by JSON.
    Returns list of NetCDF paths
    """

    # inputs/paths
    inp = cfg["input"]
    wc_time_col = inp.get("wc_time_col", "time")
    q_time_col = inp.get("q_time_col", "time")
    wc_station_col = inp.get("wc_station_col", "station_name")
    q_station_col = inp.get("q_station_col", "station_name")
    discharge_var = inp.get("discharge_var", "discharge")

    paths = cfg["paths"]
    wc_file = resolve_input_source(inp["waterchem_file"])
    q_file = resolve_input_source(inp["discharge_file"])

    figs_all_dir = resolve_path(paths["fig_all_methods_dir"])
    figs_selected_dir = resolve_path(paths["fig_selected_dir"])
    figs_validation_dir = resolve_path(
        paths.get(
            "fig_validation_dir",
            str(Path(paths["fig_selected_dir"]).parent / "validation"),
        )
    )
    out_dir = resolve_path(paths["output_dir"])

    ensure_dirs(figs_all_dir, figs_selected_dir, figs_validation_dir, out_dir)

    rename_maps = cfg.get("rename_maps", {})
    wc_rename = rename_maps.get("wc", {})
    q_rename = rename_maps.get("q", {})

    # variables & metadata
    chem_variables: list[str] = cfg.get("chem_variables", [])
    pars_meta_df = pd.DataFrame(cfg.get("pars_metadata", []))
    standard_name_map: dict[str, str] = cfg.get("standard_name_map", {})

    # interpolation
    interp_cfg = cfg.get("interpolation", {})
    linear_cfg = interp_cfg.get("linear", {"max_gap": 90, "method": "linear", "order": None})
    gam_cfg = interp_cfg.get("gam", {"n_splines_xy": [10, 20], "lam_grid": None})

    # selection
    sel_cfg = cfg.get("selection", {})
    candidate_suffixes = sel_cfg.get("candidate_suffixes", ["annual_gam", "monthly_regres", "monthly_interp"])
    fallback_method = sel_cfg.get("fallback_method", "linear_interp")
    use_linear_fallback = bool(sel_cfg.get("use_linear_fallback", True))
    min_r2 = float(sel_cfg.get("min_r2", 0.0))
    r2_tolerance = float(
        sel_cfg.get("r2_tolerance", 0.05)
    )
    max_abs_bias_fraction = float(
        sel_cfg.get("max_abs_bias_fraction", 0.25)
    )

    extreme_ratio_limit = float(sel_cfg.get("extreme_ratio_limit", 3.0))

    val_cfg = cfg.get("validation", {})
    validation_enabled = bool(val_cfg.get("enabled", True))
    validation_n_repeats = int(val_cfg.get("n_repeats", 10))
    validation_block_size = int(val_cfg.get("block_size", 2))
    validation_min_observations = int(val_cfg.get("min_observations", 20))
    validation_min_points = int(val_cfg.get("min_validation_points", 10))
    validation_min_coverage = float(val_cfg.get("min_coverage", 0.5))

    # output/global
    out_cfg = cfg.get("output", {})
    time_name_out = out_cfg.get("time_name", "date")  # coord name in output
    file_prefix = out_cfg.get("file_prefix", "daily_water_chemistry_modeled_")

    meta_map = meta_cfg(cfg)

    # load & harmonize
    if isinstance(wc_file, Path) and not wc_file.exists():
        raise FileNotFoundError(f"Water chemistry file not found: {wc_file}")
    if isinstance(q_file, Path) and not q_file.exists():
        raise FileNotFoundError(f"Discharge file not found: {q_file}")

    wc_df_raw = netcdf_to_dataframe(wc_file, time_vars=(wc_time_col, "time", "sample_date"))
    q_df_raw = netcdf_to_dataframe(q_file, time_vars=(q_time_col, "time", "date"))

    # station id we will work with
    station_name_val = read_meta_value(wc_df_raw, meta_map, "station_name")
    if station_name_val is None and wc_station_col in wc_df_raw.columns:
        svals = wc_df_raw[wc_station_col].dropna().unique()
        station_name_val = svals[0] if len(svals) else "UNKNOWN"
    station_id = str(station_name_val)

    wc_df = standardize_time_and_station(
        wc_df_raw,
        time_col_in=wc_time_col,
        date_col_out="date",
        station_col_in=wc_station_col,
        station_col_out="river_name",
        station_rename_map=wc_rename,
    )

    q_df = standardize_time_and_station(
        q_df_raw,
        time_col_in=q_time_col,
        date_col_out="date",
        station_col_in=q_station_col,
        station_col_out="river_name",
        station_rename_map=q_rename,
    )

    if discharge_var in q_df.columns and discharge_var != "discharge":
        q_df = q_df.rename(columns={discharge_var: "discharge"})

    merged = merge_daily_discharge_and_chemistry(
        wc_df, q_df,
        station_name=station_id,
        station_col="river_name",
        date_col="date",
        discharge_col="discharge",
        drop_wc_cols=cfg.get("drop_columns", ['latitude', 'longitude', 'station_id', 'station_code', 'station_type']),
    )

    # run interpolation
    # 1) linear (gap-limited)
    df_linear = interpolate_station_df(
        merged,
        variables=chem_variables,
        date_col="date",
        meta_cols=["river_name"],
        max_gap=int(linear_cfg.get("max_gap", 90)),
        method=str(linear_cfg.get("method", "linear")),
        order=linear_cfg.get("order", None),
    )

    # 2) GAM on discharge + day-of-year
    df_gam = apply_gam_to_df(
        merged,
        chem_variables,
        discharge_col="discharge",
        date_col="date",
        station_name=station_id,
        n_splines_xy=tuple(gam_cfg.get("n_splines_xy", [10, 20])),
        lam_grid=np.array(gam_cfg["lam_grid"]) if gam_cfg.get("lam_grid") else None,
    )

    # 3) monthwise log–log(Q, var)
    df_month_reg = monthwise_loglog_regressions(
        merged, chem_variables, discharge_col="discharge", date_col="date"
    )

    # 4) monthly medians -> daily
    df_month_interp = monthly_medians_to_daily_all_years(
        merged, chem_variables, date_col="date"
    )

    # attach monthly_interp to the station frame so we carry river_name + discharge for consistent joins
    st_plus_month = (
        merged.set_index("date")
        .merge(df_month_interp.set_index("date"), left_index=True, right_index=True, how="left")
        .reset_index()
    )

    # combine method outputs
    # A <- linear + GAM
    merged1 = pd.merge(
        df_linear, df_gam,
        on=["river_name", "date", "discharge"],
        how="left", suffixes=("", "_drop")
    )
    merged1 = merged1.loc[:, ~merged1.columns.str.endswith("_drop")]

    # B <- A + monthwise log–log regression
    merged2 = pd.merge(
        merged1, df_month_reg,
        on=["river_name", "date", "discharge"],
        how="left", suffixes=("", "_drop")
    )
    merged2 = merged2.loc[:, ~merged2.columns.str.endswith("_drop")]

    # C <- B + monthly medians
    merged3 = pd.merge(
        merged2, st_plus_month,
        on=["river_name", "date", "discharge"],
        how="left", suffixes=("", "_drop")
    )
    merged3 = merged3.loc[:, ~merged3.columns.str.endswith("_drop")]

    # group and aggregate daily values
    df_grouped = (
        merged3.groupby(["date", "river_name"])
        .agg(lambda x: x.mean() if pd.api.types.is_numeric_dtype(x) else x.iloc[0])
        .reset_index()
    )

    # resample to daily per station
    df_daily_all = (
        df_grouped.set_index("date")
        .groupby("river_name")
        .resample("D")
        .mean(numeric_only=True)
        .reset_index()
        .sort_values(["river_name", "date"])
    )

    # copy for plotting/selection
    df_sel = df_daily_all.copy()

    # method selection per variable
    methods_chosen: dict[str, Any] = {}
    method_scores: dict[
        str,
        dict[str, dict[str, dict[str, float]]],
    ] = {}
    validation_rows: list[dict[str, Any]] = []
    completeness_rows: list[dict[str, Any]] = []

    for var in chem_variables:
        method_scores[var] = {}
        methods_chosen[var] = {}

        df_station = df_daily_all[df_daily_all["river_name"] == station_id].copy()
        if df_station.empty or var not in df_station.columns:
            print(f"{station_id} -> {var}: No data column, skipping.")
            continue

        df_station = df_station.set_index("date")
        y_obs_full = df_station[var]
        obs_start, obs_end = y_obs_full.first_valid_index(), y_obs_full.last_valid_index()
        if obs_start is None or obs_end is None:
            print(f"{station_id} -> {var}: No observations found")
            continue

        df_obs_range = df_station.loc[obs_start:obs_end]
        y_obs = df_obs_range[var]

        if validation_enabled:
            metrics, validation_df = validate_interpolation_methods(
                merged,
                var,
                station_name=station_id,
                linear_cfg=linear_cfg,
                gam_cfg=gam_cfg,
                n_repeats=validation_n_repeats,
                block_size=validation_block_size,
                min_observations=validation_min_observations,
            )
        else:
            metrics = {}
            validation_df = pd.DataFrame(
                columns=["method", "date", "observed", "predicted"]
            )
            for suffix in candidate_suffixes + [fallback_method]:
                colname = f"{var}_{suffix}"
                if colname not in df_obs_range.columns:
                    continue
                y_pred = df_obs_range[colname]
                valid = y_obs.notna() & y_pred.notna()
                if valid.sum() < validation_min_points:
                    continue
                errors = y_pred[valid] - y_obs[valid]
                metrics[suffix] = {
                    "r2": float(r2_score(y_obs[valid], y_pred[valid])),
                    "mae": float(mean_absolute_error(y_obs[valid], y_pred[valid])),
                    "rmse": float(np.sqrt(mean_squared_error(y_obs[valid], y_pred[valid]))),
                    "bias": float(errors.mean()),
                    "n": float(valid.sum()),
                    "coverage": 1.0,
                }

        method_scores[var][station_id] = metrics

        for suffix, score in metrics.items():
            validation_rows.append(
                {
                    "station": station_id,
                    "variable": var,
                    "method": suffix,
                    **score,
                }
            )

        if not validation_df.empty:
            plot_validation_qc(
                validation_df,
                metrics,
                var=var,
                station=station_id,
                pars_meta_df=pars_meta_df,
                save_path=figs_validation_dir / f"{station_id}_{var}_validation.png",
            )

        good_methods = []
        methods_to_compare = list(candidate_suffixes)

        for suffix in methods_to_compare:
            colname = f"{var}_{suffix}"

            if colname not in df_obs_range.columns:
                continue

            score = metrics.get(suffix)

            if not score:
                print(
                    f"{station_id} -> {var}: {colname} rejected "
                    "— no validation result"
                )
                continue

            n_val = int(score.get("n", 0))
            coverage = float(score.get("coverage", 0.0))
            r2 = float(score.get("r2", np.nan))
            rmse = float(score.get("rmse", np.nan))
            nrmse = float(score.get("nrmse", np.nan))
            mae = float(score.get("mae", np.nan))
            bias = float(score.get("bias", np.nan))

            obs_typical = float(y_obs.dropna().median())

            if np.isfinite(obs_typical) and obs_typical != 0:
                abs_bias_fraction = abs(bias) / abs(obs_typical)
            else:
                abs_bias_fraction = np.nan

            if n_val < validation_min_points:
                print(
                    f"{station_id} -> {var}: {colname} rejected "
                    f"— only {n_val} validation points"
                )
                continue

            if coverage < validation_min_coverage:
                print(
                    f"{station_id} -> {var}: {colname} rejected "
                    f"— validation coverage {coverage:.0%} below "
                    f"{validation_min_coverage:.0%}"
                )
                continue

            if not np.isfinite(rmse):
                print(
                    f"{station_id} -> {var}: {colname} rejected "
                    "— validation RMSE is not finite"
                )
                continue

            if not np.isfinite(r2) or r2 < min_r2:
                print(
                    f"{station_id} -> {var}: {colname} rejected "
                    f"— validation R^2 = {r2:.3f} below "
                    f"{min_r2:.3f}"
                )
                continue

            if (
                    np.isfinite(abs_bias_fraction)
                    and abs_bias_fraction > max_abs_bias_fraction
            ):
                print(
                    f"{station_id} -> {var}: {colname} rejected "
                    f"— relative validation bias = "
                    f"{abs_bias_fraction:.1%} above "
                    f"{max_abs_bias_fraction:.1%}"
                )
                continue

            good_methods.append(
                {
                    "suffix": suffix,
                    "r2": r2,
                    "rmse": rmse,
                    "nrmse": nrmse,
                    "mae": mae,
                    "bias": bias,
                    "abs_bias_fraction": abs_bias_fraction,
                    "coverage": coverage,
                    "n": n_val,
                    "colname": colname,
                    "y_pred": df_obs_range[colname],
                }
            )

        # First identify the best validation R2.
        # Methods within r2_tolerance are treated as similarly skilled.
        if good_methods:
            best_r2 = max(
                method["r2"]
                for method in good_methods
                if np.isfinite(method["r2"])
            )

            comparable_methods = [
                method
                for method in good_methods
                if (
                        np.isfinite(method["r2"])
                        and method["r2"] >= best_r2 - r2_tolerance
                )
            ]

            # Among similarly skilled methods, prefer lower normalized
            # prediction error, then lower relative bias, then higher R2.
            comparable_methods.sort(
                key=lambda x: (
                    (
                        x["nrmse"]
                        if np.isfinite(x["nrmse"])
                        else np.inf
                    ),
                    (
                        x["abs_bias_fraction"]
                        if np.isfinite(x["abs_bias_fraction"])
                        else np.inf
                    ),
                    -x["r2"],
                )
            )

            good_methods = comparable_methods

        selected_col: str | None = None
        selected_series: pd.Series | None = None

        if good_methods:
            selected = good_methods[0]
            selected_col = selected["colname"]
            selected_series = selected["y_pred"].copy()
            print(
                f"{station_id} -> {var}: Selected {selected_col} "
                f"(validation R^2 = {selected['r2']:.3f}, "
                f"RMSE = {selected['rmse']:.3g})"
            )

        linear_col = f"{var}_{fallback_method}"

        if selected_col is None:
            if use_linear_fallback and linear_col in df_obs_range.columns:
                selected_col = linear_col
                selected_series = df_obs_range[linear_col].copy()
                print(f"{station_id} -> {var}: Fallback to {linear_col}")
            else:
                print(f"{station_id} -> {var}: No valid method available")
                continue

        filled_series = selected_series.copy()
        fallback_cols_used: list[str] = []

        # 1. Fill remaining short gaps using gap-limited linear interpolation.
        # The max_gap restriction has already been applied when the linear series was created.

        if (
                use_linear_fallback
                and linear_col in df_obs_range.columns
                and linear_col != selected_col
        ):
            linear_series = df_obs_range[linear_col]

            missing_mask = filled_series.isna()
            fill_dates = missing_mask & linear_series.notna()

            if fill_dates.any():
                filled_series.loc[fill_dates] = linear_series.loc[fill_dates]

                if linear_col not in fallback_cols_used:
                    fallback_cols_used.append(linear_col)

                print(
                    f"{station_id} -> {var}: Remaining short gaps filled from "
                    f"{linear_col}"
                )

        # 2. Fill remaining gaps using GAM or monthly discharge regression.

        missing_mask = filled_series.isna()

        if missing_mask.any():

            observed_values = y_obs.dropna()

            # Robust reference range for evaluating predictions
            # during periods without observations.
            if not observed_values.empty:
                observed_low = float(
                    observed_values.quantile(0.05)
                )
                observed_high = float(
                    observed_values.quantile(0.95)
                )

                min_allowed = (
                    observed_low / extreme_ratio_limit
                    if observed_low > 0
                    else 0.0
                )

                max_allowed = (
                        observed_high * extreme_ratio_limit
                )

            else:
                min_allowed = 0.0
                max_allowed = np.inf

            # Candidate model fallbacks.
            # Do not retry the method that is already the selected method.
            model_fallback_candidates = [
                suffix
                for suffix in ["annual_gam", "monthly_regres"]
                if f"{var}_{suffix}" != selected_col
            ]

            # Rank long-gap fallback models using both validation
            # performance and prediction plausibility.
            def fallback_rank(
                    suffix: str,
            ) -> tuple[float, float, float, float]:

                candidate_col = f"{var}_{suffix}"

                if candidate_col not in df_obs_range.columns:
                    return np.inf, np.inf, np.inf, np.inf

                remaining_dates = filled_series.index[
                    filled_series.isna()
                ]

                candidate_values = (
                    df_obs_range[candidate_col]
                    .reindex(remaining_dates)
                    .dropna()
                )

                if candidate_values.empty:
                    return np.inf, np.inf, np.inf, np.inf

                # Fraction of long-gap predictions outside the
                # robust observed concentration range.
                outside = (
                        (candidate_values < min_allowed)
                        | (candidate_values > max_allowed)
                )

                outside_fraction = float(outside.mean())

                score = metrics.get(suffix, {})

                r2 = float(score.get("r2", np.nan))
                nrmse = float(score.get("nrmse", np.nan))
                bias = float(score.get("bias", np.nan))

                obs_typical = float(observed_values.median())

                if (
                        np.isfinite(obs_typical)
                        and obs_typical != 0
                        and np.isfinite(bias)
                ):
                    bias_fraction = abs(bias) / abs(obs_typical)
                else:
                    bias_fraction = np.inf

                r2_rank = (
                    -r2
                    if np.isfinite(r2)
                    else np.inf
                )

                nrmse_rank = (
                    nrmse
                    if np.isfinite(nrmse)
                    else np.inf
                )

                return (
                    outside_fraction,
                    r2_rank,
                    nrmse_rank,
                    bias_fraction,
                )

            model_fallback_candidates.sort(
                key=fallback_rank
            )

            for suffix in model_fallback_candidates:

                # Stop once no missing values remain.
                if not filled_series.isna().any():
                    break

                candidate_col = f"{var}_{suffix}"

                if candidate_col not in df_obs_range.columns:
                    continue

                candidate_series = df_obs_range[candidate_col]

                # Evaluate predictions only for dates that are still missing.
                remaining_dates = filled_series.index[filled_series.isna()]

                candidate_values = (
                    candidate_series
                    .reindex(remaining_dates)
                    .dropna()
                )

                if candidate_values.empty:
                    continue

                # Reject candidate if it contains negative concentrations
                if (candidate_values < 0).any():
                    print(
                        f"{station_id} -> {var}: "
                        f"{candidate_col} rejected as fallback "
                        "— negative predictions"
                    )
                    continue


                # Fill dates where this model has an estimate
                fill_dates = candidate_values.index[
                    filled_series.loc[candidate_values.index].isna()
                ]

                if len(fill_dates) == 0:
                    continue

                filled_series.loc[fill_dates] = candidate_values.loc[fill_dates]

                if candidate_col not in fallback_cols_used:
                    fallback_cols_used.append(candidate_col)

                print(
                    f"{station_id} -> {var}: "
                    f"{len(fill_dates)} remaining days filled from "
                    f"{candidate_col}"
                )

        # ---------------------------------------------------------
        # Report anything that is STILL missing
        # ---------------------------------------------------------

        remaining_missing = int(filled_series.isna().sum())

        if remaining_missing > 0:
            print(
                f"{station_id} -> {var}: "
                f"{remaining_missing} days still missing after all fallbacks"
            )
        else:
            print(
                f"{station_id} -> {var}: "
                "Final estimated series has no internal missing values"
            )

        methods_chosen[var][station_id] = {
            "selected_col": selected_col,
            "fallback_cols": fallback_cols_used,
        }

        mask = df_daily_all["river_name"] == station_id
        dates = pd.DatetimeIndex(df_daily_all.loc[mask, "date"].values)

        aligned_model = filled_series.reindex(dates)
        observed = df_daily_all.loc[mask, var].copy()
        observed.index = dates

        final_series = observed.combine_first(aligned_model)

        df_daily_all.loc[mask, f"{var}_final"] = final_series.values

        # QC: check completeness of the final daily series
        # Only assess the period between the first and last observation.
        final_in_range = final_series.loc[obs_start:obs_end]

        n_total = len(final_in_range)
        n_missing = int(final_in_range.isna().sum())

        completeness_rows.append(
            {
                "station": station_id,
                "variable": var,
                "start_date": obs_start.date(),
                "end_date": obs_end.date(),
                "total_days": n_total,
                "missing_days": n_missing,
                "missing_percent": (
                    100.0 * n_missing / n_total
                    if n_total > 0
                    else np.nan
                ),
            }
        )

        final_df = (
            final_series
            .rename("final")
            .rename_axis("date")
            .reset_index()
        )

        df_plot = df_sel[
            df_sel["river_name"] == station_id
            ][["date", "river_name", var]].merge(
            final_df,
            on="date",
            how="left",
        )

        entry = methods_chosen[var][station_id]
        selected_col = entry["selected_col"]
        fallback_cols = entry["fallback_cols"]

        method_label = build_method_comment(
            var=var,
            selected_col=selected_col,
            fallback_cols=fallback_cols,
        )

        plot_qc(
            df_plot,
            var=var,
            station=station_id,
            method_col="final",
            method_label=method_label,
            pars_meta_df=pars_meta_df,
            save_path=figs_selected_dir / f"{station_id}_{var}_selected_method.png",
        )

    if validation_rows:
        validation_table = pd.DataFrame(validation_rows)
        validation_table.to_csv(
            figs_validation_dir / f"{station_id}_validation_metrics.csv",
            index=False,
        )

    # Save QC summary of missing values in the final time series
    if completeness_rows:
        completeness_table = pd.DataFrame(completeness_rows)

        completeness_table.to_csv(
            figs_validation_dir / f"{station_id}_final_series_completeness.csv",
            index=False,
        )

        print("\nFinal-series completeness:")
        print(
            completeness_table[
                ["variable", "missing_days", "missing_percent"]
            ].to_string(index=False)
        )

    # overview plots
    stations = [station_id]
    for var in chem_variables:
        fig, axes = plt.subplots(nrows=1, ncols=len(stations), figsize=(6 * len(stations), 6))
        if len(stations) == 1:
            axes = [axes]
        unit = ""
        if var in set(pars_meta_df["parameter_name"]):
            u = pars_meta_df.loc[pars_meta_df["parameter_name"] == var, "unit"].values[0]
            unit = f" ({u})"
        for ax, st in zip(axes, stations):
            d = df_sel[df_sel["river_name"] == st]
            ax.scatter(d["date"], d[var], label="Raw", s=20, facecolors="white", edgecolors="black", alpha=0.8,
                       zorder=4)
            for label, style, color in [
                (f"{var}_monthly_interp", "-", None),
                (f"{var}_linear_interp", "-", "orange"),
                (f"{var}_annual_gam", "--", "darkcyan"),
                (f"{var}_monthly_regres", "--", "brown"),
            ]:
                if label in d.columns:
                    ax.plot(d["date"], d[label], label=label.replace(f"{var}_", "").replace("_", " ").title(),
                            linestyle=style, color=color, alpha=0.7)
            ax.set_title(st);
            ax.set_xlabel("date");
            ax.set_ylabel(f"{var}{unit}");
            ax.legend();
            ax.grid(True)
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.suptitle(f"{var} Raw vs Interpolations", fontsize=16)
        plt.savefig(figs_all_dir / f"{var}_all_interp_methods.png", dpi=300, bbox_inches="tight")
        plt.close()

    # export final
    final_cols = [c for c in df_daily_all.columns if c.endswith("_final")]
    export = df_daily_all[["date", "river_name"] + final_cols].copy()
    export = export.rename(columns={c: c.replace("_final", "") for c in final_cols})
    export = export.sort_values(["river_name", "date"])

    # xarray per station
    df_station = export[export["river_name"] == station_id].copy()
    df_station["date"] = pd.to_datetime(df_station["date"])
    df_station = df_station.set_index("date")
    ds = xr.Dataset.from_dataframe(df_station.drop(columns=["river_name"]))

    # coord attrs
    ds = ds.assign_coords(**{time_name_out: ("date", df_station.index)})
    ds = ds.swap_dims({"date": time_name_out})
    if time_name_out != "date":
        ds = ds.drop_vars("date")

    # coordinates (lat/lon from meta if present)
    lat = read_meta_value(wc_df_raw, meta_map, "latitude")
    lon = read_meta_value(wc_df_raw, meta_map, "longitude")
    if lat is None:
        lat = meta_map.get("latitude", {}).get("value")
    if lon is None:
        lon = meta_map.get("longitude", {}).get("value")
    if lat is not None and lon is not None:
        ds = ds.assign_coords(
            latitude=xr.DataArray(float(lat), dims=()),
            longitude=xr.DataArray(float(lon), dims=()),
        ).set_coords(["latitude", "longitude"])

    # station_name as scalar
    ds["station_name"] = xr.DataArray(
        station_id, dims=(),
        attrs={"cf_role": "timeseries_id", "long_name": "Station name", "units": "1"}
    )

    # annotate variables
    for var in ds.data_vars:
        if var == "river_name":
            continue
        if var in set(pars_meta_df["parameter_name"]):
            row = pars_meta_df[pars_meta_df["parameter_name"] == var].iloc[0]
            ds[var].attrs["units"] = str(row["unit"])
            ds[var].attrs["long_name"] = str(standard_name_map.get(var, var))

        # method comment
        chosen = (methods_chosen.get(var) or {}).get(station_id)
        if chosen:
            auto_comment = build_method_comment(
                var=var,
                selected_col=chosen["selected_col"],
                fallback_cols=chosen["fallback_cols"],
            )
            ds[var].attrs["comment"] = auto_comment

    # global attrs
    ds.attrs = build_global_attrs(
        cfg=cfg,
        station_id=station_id,
        time_name=time_name_out,
    )

    export_cfg = cfg.get("export", {})
    engine = export_cfg.get("engine", "netcdf4")
    nc_format = export_cfg.get("format", "NETCDF4")
    time_enc_cfg = export_cfg.get("time", {})

    # filename + stable id seed
    filename = export_cfg.get(
        "filename_template",
        f"{file_prefix}{station_id.lower().replace(' ', '_')}.nc"
    ).format(station_id_or_stem=station_id, station_id=station_id)

    # export metadata/id settings
    md = cfg.get("metadata") or {}
    md_id = md.get("id") or {}

    namespace_uuid = md_id.get("namespace_uuid") or cfg.get("processed_namespace_uuid")
    id_prefix = md_id.get("prefix") or (export_cfg.get("id_prefix") if export_cfg else None)

    id_seed = render_template(md_id.get("seed_template", "{station_id}"),
                              {"station_id": station_id, "time_name": time_name_out}) or station_id


    export_dir = out_dir

    if cfg.get("output_file"):
        target = Path(cfg["output_file"]).expanduser().resolve()
        export_dir = target.parent
        filename = target.name

    out_path = export_dataset(
        ds=ds,
        output_dir=export_dir,
        filename=filename,

        time_name=time_name_out,
        global_attrs=ds.attrs,
        namespace_uuid=namespace_uuid,
        id_prefix=id_prefix,
        id_seed=id_seed,
        engine=engine,
        nc_format=nc_format,
        time_encoding_cfg=time_enc_cfg,
        var_encoding_overrides=None,
    )

    return [out_path]

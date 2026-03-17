from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Mapping
from src.export_netcdf import export_dataset

from src.utils import (
    ensure_dirs,
    resolve_path,
    netcdf_to_dataframe,
    standardize_time_and_station,
    merge_daily_discharge_and_chemistry,
)

import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

from pygam import LinearGAM, s, te
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

plt.style.use("ggplot")


# ----------------------------- utils -----------------------------
def meta_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return (cfg.get("meta") or {}).copy()


def read_meta_value(df_like, m: Dict[str, Any], key: str) -> Optional[Any]:
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

def render_template(s: Optional[str], ctx: Dict[str, Any]) -> Optional[str]:
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
    fallback_col: Optional[str],
    scores_for_station: Mapping[str, Dict[str, float]]
) -> str:
    """ Builds a description of how the final daily series was produced. """

    base_suffix = selected_col.replace(f"{var}_", "")
    base_txt = method_pretty_name(base_suffix)

    r2_base = scores_for_station.get(base_suffix, {}).get("R2")
    if r2_base is not None:
        base_txt += f" (R^2 = {r2_base:.3f})"

    if not fallback_col:
        return base_txt + "."

    fb_suffix = fallback_col.replace(f"{var}_", "")
    fb_txt = method_pretty_name(fb_suffix)
    r2_fb = scores_for_station.get(fb_suffix, {}).get("R2")
    if r2_fb is not None:
        fb_txt += f" (R^2 = {r2_fb:.3f})"

    return f"{base_txt}; gaps filled from {fb_txt}."

def build_global_attrs(
    cfg: Dict[str, Any],
    station_id: str,
    time_name: str,
) -> Dict[str, str]:
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
        base["date_created"] = pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        base.setdefault("date_created", md_timestamps.get("date_created"))

    # ensure strings
    return {k: str(v) for k, v in base.items()}

# ------------------------- interpolation -------------------------
def interpolate_with_gap_limit(series: pd.Series, max_gap: int, method="linear", order=None) -> pd.Series:
    """Interpolate a series but only across gaps up to max_gap samples."""
    if method in ["spline", "polynomial"] and order is None:
        raise ValueError(f"Interpolation method '{method}' requires 'order'.")
    return series.interpolate(method=method, limit=max_gap, order=order)

def interpolate_station_df(
    df: pd.DataFrame,
    variables: List[str],
    date_col="date",
    meta_cols: Optional[List[str]] = None,
    max_gap=30,
    method="linear",
    order=None,
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
            out[f"{var}_linear_interp"] = interpolate_with_gap_limit(out[var], max_gap=max_gap, method=method, order=order)
    return out.reset_index()


def compute_gam(
    df: pd.DataFrame,
    var: str,
    discharge_col="discharge",
    date_col="date",
    station_name: Optional[str] = None,
    n_splines_xy: Tuple[int, int] = (10, 20),
    lam_grid: Optional[np.ndarray] = None,
) -> Optional[pd.DataFrame]:
    """Fit a GAM on discharge and day - of - year and predict within the observed time range."""

    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col])
    out["doy"] = out[date_col].dt.dayofyear

    train = out.dropna(subset=[var, discharge_col, "doy"]).copy()
    if train.empty:
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
    print(f"GAM {station_name} -> {var}: MAE={mae:.2f}, MSE={mse:.2f}, R^2={r2:.3f}")
    return out


def apply_gam_to_df(
    df: pd.DataFrame,
    variables: List[str],
    discharge_col="discharge",
    date_col="date",
    station_name="",
    n_splines_xy=(10, 20),
    lam_grid=None,
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
            )
            if gdf is not None:
                out = gdf
    return out


def monthly_to_daily_for_year(monthly_df: pd.DataFrame, year: int) -> pd.DataFrame:
    """ Convert monthly values to daily series for a given year using time interpolation. """

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

def monthly_medians_to_daily_all_years(station_df: pd.DataFrame, variables: List[str], date_col="date") -> pd.DataFrame:
    """ Compute monthly medians per year and interpolate to daily values. """

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
    variables: List[str],
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
                # smearing-style correction in log10 space: residuals are in log10 units; convert variance to multiplicative factor.
                resid = y - mdl.predict(X)
                sigma2 = float(np.var(resid, ddof=1)) if len(resid) > 1 else 0.0
                # corr = 10 ** (0.5 * sigma2 * np.log(10) ** 2 / (np.log(10) ** 2))
                # # The above simplifies to: corr = 10 ** (0.5 * sigma2)
                # # Keeping it explicit isn't necessary—see simpler line below:
                corr = 10 ** (0.5 * sigma2)
            else:
                corr = 1.0

            pred_vals = (10 ** yhat_log) * corr
            pred[ok.values] = pred_vals

            g2[f"{var}_monthly_regres"] = pred
            out_all.append(g2.reset_index())

    if not out_all:
        return pd.DataFrame(columns=station_df.columns)

    return pd.concat(out_all, ignore_index=True)


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
    r2_value: Optional[float] = None,
    save_path: Optional[Path] = None,
) -> None:
    unit = ""
    if var in set(pars_meta_df[unit_par_col]):
        unit_val = pars_meta_df.loc[pars_meta_df[unit_par_col] == var, unit_unit_col].values[0]
        unit = f" ({unit_val})"

    df_s = df[df[station_col] == station].copy()
    plt.figure(figsize=(12, 5))
    plt.scatter(df_s[date_col], df_s[var], label="Raw", color="black", alpha=0.7, s=30)
    if method_col in df_s.columns:
        plt.plot(df_s[date_col], df_s[method_col], label=f"{method_label}", lw=2)
    title = f"{var} at {station} – {method_label}"
    if r2_value is not None:
        title += f" (R2={r2_value:.2f})"
    plt.title(title)
    plt.xlabel(" ")
    plt.ylabel(f"{var}{unit}")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
        plt.close()
    else:
        plt.show()


# ------------------------------- main --------------------------------
def interpolate(cfg: Dict[str, Any]) -> List[Path]:
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
    wc_file = resolve_path(inp["waterchem_file"])
    q_file = resolve_path(inp["discharge_file"])

    figs_all_dir = resolve_path(paths["fig_all_methods_dir"])
    figs_selected_dir = resolve_path(paths["fig_selected_dir"])
    out_dir = resolve_path(paths["output_dir"])

    ensure_dirs(figs_all_dir, figs_selected_dir, out_dir)

    rename_maps = cfg.get("rename_maps", {})
    wc_rename = rename_maps.get("wc", {})
    q_rename = rename_maps.get("q", {})

    # variables & metadata
    chem_variables: List[str] = cfg.get("chem_variables", [])
    pars_meta_df = pd.DataFrame(cfg.get("pars_metadata", []))
    standard_name_map: Dict[str, str] = cfg.get("standard_name_map", {})

    # interpolation
    interp_cfg = cfg.get("interpolation", {})
    linear_cfg = interp_cfg.get("linear", {"max_gap": 90, "method": "linear", "order": None})
    gam_cfg = interp_cfg.get("gam", {"n_splines_xy": [10, 20], "lam_grid": None})

    # selection
    sel_cfg = cfg.get("selection", {})
    candidate_suffixes = sel_cfg.get("candidate_suffixes", ["annual_gam", "monthly_regres", "monthly_interp"])
    fallback_method = sel_cfg.get("fallback_method", "linear_interp")
    r2_threshold = float(sel_cfg.get("r2_threshold", 0.80))
    z_score_limit = float(sel_cfg.get("z_score_limit", 3))
    tolerance = float(sel_cfg.get("tolerance", 50))
    extreme_ratio_limit = float(sel_cfg.get("extreme_ratio_limit", 2.0))

    # output/global
    out_cfg = cfg.get("output", {})
    time_name_out = out_cfg.get("time_name", "date")  # coord name in output
    file_prefix = out_cfg.get("file_prefix", "daily_water_chemistry_modeled_")

    meta_map = meta_cfg(cfg)

    # load & harmonize
    if not wc_file.exists():
        raise FileNotFoundError(f"Water chemistry file not found: {wc_file}")
    if not q_file.exists():
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
    methods_chosen: Dict[str, Any] = {}
    method_scores: Dict[str, Dict[str, Dict[str, Dict[str, float]]]] = {}

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
        good_methods = []

        # evaluate candidates
        for suffix in candidate_suffixes:
            colname = f"{var}_{suffix}"
            if colname not in df_station.columns:
                continue
            y_pred = df_obs_range[colname]
            valid = y_obs.notna() & y_pred.notna()
            if valid.sum() < 10:
                continue
            r2 = r2_score(y_obs[valid], y_pred[valid])
            method_scores[var].setdefault(station_id, {})[suffix] = {"R^2": r2}
            if r2 >= r2_threshold:
                good_methods.append({'suffix': suffix, 'r2': r2, 'colname': colname, 'y_pred': y_pred})

        good_methods.sort(key=lambda x: x['r2'], reverse=True)

        selected_col: Optional[str] = None
        selected_series: Optional[pd.Series] = None

        # outlier check
        for m in good_methods:
            pred = m['y_pred']
            obs = y_obs.dropna()
            base_threshold = obs.mean() + z_score_limit * obs.std()
            final_threshold = base_threshold + tolerance
            if (pred > final_threshold).any():
                print(f"{station_id} -> {var}: {m['colname']} rejected — {int((pred > final_threshold).sum())} extreme value(s)")
                continue
            selected_col = m['colname']
            selected_series = pred.copy()
            print(f"{station_id} -> {var}: Selected {selected_col} (R^2 = {m['r2']:.3f})")
            break

        # fallback to linear
        if selected_col is None:
            fallback_col = f"{var}_{fallback_method}"
            if fallback_col in df_obs_range.columns:
                selected_col = fallback_col
                selected_series = df_obs_range[fallback_col].copy()
                print(f"{station_id} -> {var}: Fallback to {fallback_col}")
            else:
                print(f"{station_id} -> {var}: No valid method available")
                continue

        # fill gaps from best of {annual_gam, monthly_regres}
        filled_series = selected_series.copy()
        missing_mask = filled_series.loc[obs_start:obs_end].isna()
        fallback_used = None

        if missing_mask.any():
            # check for long gap >= 4y
            gap_lengths = (missing_mask.astype(int).groupby((~missing_mask).cumsum()).sum())
            long_gap = (not gap_lengths.empty) and (gap_lengths.max() >= 1460)

            fb_cands = []
            for suffix in ['annual_gam', 'monthly_regres']:
                col = f"{var}_{suffix}"
                if col not in df_obs_range.columns:
                    continue
                y_pred = df_obs_range[col]
                valid = y_obs.notna() & y_pred.notna()
                if valid.sum() < 10:
                    continue
                r2b = r2_score(y_obs[valid], y_pred[valid])
                method_scores[var].setdefault(station_id, {})[suffix] = {"R^2": r2b}

                if long_gap:
                    obs_min, obs_max = y_obs.min(), y_obs.max()
                    pred_min, pred_max = y_pred.min(), y_pred.max()
                    too_extreme = (pred_max > obs_max * extreme_ratio_limit) or (pred_min < obs_min / extreme_ratio_limit)
                    if too_extreme:
                        print(f"{station_id} -> {var}: {col} rejected for gap fill — predicted min/max too extreme")
                        continue

                fb_cands.append({'suffix': suffix, 'r2': r2b, 'colname': col, 'series': y_pred})

            if fb_cands:
                fb_cands.sort(key=lambda x: x['r2'], reverse=True)
                best_fb = fb_cands[0]
                avail = missing_mask & best_fb['series'].notna()
                if avail.any():
                    filled_series.loc[avail] = best_fb['series'].loc[avail]
                    fallback_used = best_fb['colname']
                    print(f"{station_id} -> {var}: Gaps filled from fallback {fallback_used} (R^2 = {best_fb['r2']:.3f})")

        # save decision
        methods_chosen[var][station_id] = {
            "selected_col": selected_col,
            "selected_suffix": selected_col.replace(f"{var}_", ""),
            "fallback_col": fallback_used,
            "fallback_suffix": fallback_used.replace(f"{var}_", "") if fallback_used else None,
        }

        # write back
        mask = df_daily_all["river_name"] == station_id
        dates = pd.DatetimeIndex(df_daily_all.loc[mask, "date"].values)
        aligned = filled_series.reindex(dates)
        df_daily_all.loc[mask, f"{var}_final"] = aligned.values

        # QC plot
        pred_df = filled_series.rename("pred").reset_index()  # date, pred
        df_plot = df_sel[df_sel["river_name"] == station_id][["date", "river_name", var]].merge(
            pred_df, on="date", how="left"
        )

        # label + R^2 for the legend/title
        entry = methods_chosen[var][station_id]
        selected_col = entry["selected_col"]
        fallback_col = entry["fallback_col"]

        scores_for_station = method_scores.get(var, {}).get(station_id, {})

        # method label
        method_label = build_method_comment(
            var=var,
            selected_col=selected_col,
            fallback_col=fallback_col,
            scores_for_station=scores_for_station,
        )

        # optionally show only base R^2 in title
        base_suffix = entry["selected_suffix"]
        r2_for_label = scores_for_station.get(base_suffix, {}).get("R2")

        plot_qc(
            df_plot,
            var=var,
            station=station_id,
            method_col="pred",
            method_label=method_label,
            pars_meta_df=pars_meta_df,
            r2_value=r2_for_label,
            save_path=figs_selected_dir / f"{station_id}_{var}_selected_method.png",
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
            ax.scatter(d["date"], d[var], label="Raw", s=20, facecolors="white", edgecolors="black", alpha=0.8, zorder=4)
            for label, style, color in [
                (f"{var}_monthly_interp", "-", None),
                (f"{var}_linear_interp", "-", "orange"),
                (f"{var}_annual_gam", "--", "darkcyan"),
                (f"{var}_monthly_regres", "--", "brown"),
            ]:
                if label in d.columns:
                    ax.plot(d["date"], d[label], label=label.replace(f"{var}_", "").replace("_", " ").title(),
                            linestyle=style, color=color, alpha=0.7)
            ax.set_title(st); ax.set_xlabel("date"); ax.set_ylabel(f"{var}{unit}"); ax.legend(); ax.grid(True)
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
            scores_for_station = method_scores.get(var, {}).get(station_id, {})
            auto_comment = build_method_comment(
                var=var,
                selected_col=chosen["selected_col"],
                fallback_col=chosen["fallback_col"],
                scores_for_station=scores_for_station,
            )
            ds[var].attrs["comment"] = auto_comment

    # global attrs
    ds.attrs = build_global_attrs(
        cfg=cfg,
        # ds=ds,
        station_id=station_id,
        time_name=time_name_out,
        # lat=float(lat) if lat is not None else None,
        # lon=float(lon) if lon is not None else None,
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

    out_path = export_dataset(
        ds=ds,
        output_dir=out_dir,
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



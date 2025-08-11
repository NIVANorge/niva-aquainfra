from __future__ import annotations

import uuid
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

# Optional: pygam for GAMs
try:
    from pygam import LinearGAM, s, te
    _HAVE_PYGAM = True
except Exception:
    _HAVE_PYGAM = False

from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# --- utils
try:
    from .utils import ensure_dirs  # package use
except Exception:
    def ensure_dirs(*paths: Path) -> None:
        for p in paths:
            Path(p).mkdir(parents=True, exist_ok=True)

plt.style.use("ggplot")


# ----------------------------- tiny utils -----------------------------
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


# ---------------------------- data loading ----------------------------
def load_netcdf_to_dataframe(nc_path: Path, time_vars: Tuple[str, ...]) -> Tuple[str, pd.DataFrame]:
    with xr.open_dataset(nc_path) as ds:
        df = ds.to_dataframe().reset_index()
    # coerce any time-like columns to datetime
    for t in time_vars:
        if t in df.columns:
            df[t] = pd.to_datetime(df[t], errors="coerce")
    return nc_path.name, df


def process_df(
    fname: str,
    df: pd.DataFrame,
    time_col_name: str,
    station_col_in: str,
    date_col_out: str,
    station_rename_map: Dict[str, str],
    station_col_out: str,
) -> Tuple[str, pd.DataFrame]:
    df = df.copy()

    # rename time -> date
    if time_col_name in df.columns:
        df = df.rename(columns={time_col_name: date_col_out})
    df[date_col_out] = pd.to_datetime(df[date_col_out], errors="coerce")
    df[date_col_out] = df[date_col_out].dt.normalize()

    # normalize station names
    if station_col_in in df.columns:
        df[station_col_in] = df[station_col_in].replace(station_rename_map or {})
        df = df.rename(columns={station_col_in: station_col_out})
    return fname, df


def merge_daily_wc_q(
    wc_df: pd.DataFrame,
    q_df: pd.DataFrame,
    station_name: str,
    station_col: str,
    date_col: str,
    discharge_col: str,
    drop_cols: List[str] | bool = False,
) -> pd.DataFrame:
    """Create daily frame over Q date range, merge discharge + chemistry for a single station."""
    wc_df = wc_df.copy()
    q_df = q_df.copy()

    # filter this station
    if station_col in wc_df.columns:
        wc_df = wc_df[wc_df[station_col] == station_name]
    if station_col in q_df.columns:
        q_df = q_df[q_df[station_col] == station_name]

    if q_df.empty:
        raise ValueError(f"No discharge rows for station '{station_name}'")

    q_df = q_df.drop_duplicates(subset=date_col).copy()
    full_dates = pd.date_range(q_df[date_col].min(), q_df[date_col].max(), freq="D")
    full_df = pd.DataFrame({date_col: full_dates})

    merged = pd.merge(full_df, q_df[[date_col, discharge_col]], on=date_col, how="left")

    if isinstance(drop_cols, list):
        wc_df = wc_df.drop(columns=drop_cols, errors="ignore")

    merged = pd.merge(merged, wc_df, on=date_col, how="left")

    # keep station col last
    station = station_name
    if station_col in merged.columns:
        merged = merged.drop(columns=[station_col], errors="ignore")

    # daily mean on duplicate dates
    num_cols = merged.select_dtypes(include="number").columns.tolist()
    agg_df = merged.groupby(date_col)[num_cols].mean(numeric_only=True).reset_index()
    agg_df[station_col] = station

    # order columns
    cols = [date_col, station_col]
    if discharge_col in agg_df.columns:
        cols.append(discharge_col)
    rest = [c for c in agg_df.columns if c not in cols]
    agg_df = agg_df[cols + rest]
    return agg_df


# ------------------------- interpolation -------------------------
def interpolate_with_gap_limit(series: pd.Series, max_gap: int, method="linear", order=None) -> pd.Series:
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
    if not _HAVE_PYGAM:
        print("pygam not installed; skipping GAM.")
        return None

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

    # train fit metrics (just to print)
    yhat = gam.predict(X)
    mae = mean_absolute_error(y, yhat)
    mse = mean_squared_error(y, yhat)
    r2 = r2_score(y, yhat)
    print(f"GAM {station_name} -> {var}: MAE={mae:.2f}, MSE={mse:.2f}, R2={r2:.3f}")
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
    tmp = tmp.applymap(lambda x: 0 if pd.notna(x) and x < 0 else x)
    return tmp


def monthly_medians_to_daily_all_years(station_df: pd.DataFrame, variables: List[str], date_col="date") -> pd.DataFrame:
    s = station_df.copy()
    s[date_col] = pd.to_datetime(s[date_col])
    s = s.set_index(date_col)
    daily_chunks = []

    for year in sorted(s.index.year.unique()):
        ydf = s[s.index.year == year]
        monthly = ydf[variables].resample("M").median()
        monthly.index = monthly.index.month
        daily_year = monthly_to_daily_for_year(monthly, year)
        daily_chunks.append(daily_year)

    if not daily_chunks:
        return pd.DataFrame(index=pd.DatetimeIndex([], name=date_col))

    daily = pd.concat(daily_chunks)
    daily = daily.rename(columns=lambda c: f"{c}_monthly_interp")
    daily.index.name = date_col
    return daily.reset_index()


def monthwise_loglog_regressions(station_df: pd.DataFrame, variables: List[str], discharge_col="discharge", date_col="date") -> pd.DataFrame:
    out_all = []
    s = station_df.copy()
    s[date_col] = pd.to_datetime(s[date_col])
    s = s.drop_duplicates(subset=[date_col]).copy()
    s["month"] = s[date_col].dt.month

    for var in variables:
        if var not in s.columns:
            continue
        sub = s.copy()
        # limit to observed span
        first, last = sub[var].first_valid_index(), sub[var].last_valid_index()
        if first is None or last is None:
            continue
        sub = sub.loc[first:last]

        for _, grp in sub.groupby("month"):
            train = grp.dropna(subset=[var, discharge_col]).copy()
            if train.shape[0] < 2:
                continue

            train[f"log_{var}"] = np.log10(train[var].replace(0, np.nan)).fillna(0)
            train["log_q"] = np.log10(train[discharge_col].replace(0, np.nan)).fillna(0)

            X = train[["log_q"]].values
            y = train[f"log_{var}"].values
            mdl = LinearRegression().fit(X, y)

            X_all = np.log10(grp[discharge_col].replace(0, np.nan)).fillna(0).values.reshape(-1, 1)
            yhat_log = mdl.predict(X_all)
            yhat = np.power(10.0, yhat_log)

            g2 = grp.copy()
            g2[f"{var}_monthly_regres"] = yhat
            out_all.append(g2)

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
    Returns NetCDF path.
    """
    # ---- inputs / paths ----
    inp = cfg["input"]
    wc_file = Path(inp["waterchem_file"])
    q_file = Path(inp["discharge_file"])
    wc_time_col = inp.get("wc_time_col", "time")
    q_time_col = inp.get("q_time_col", "time")
    wc_station_col = inp.get("wc_station_col", "station_name")
    q_station_col = inp.get("q_station_col", "station_name")
    discharge_var = inp.get("discharge_var", "discharge")

    paths = cfg["paths"]
    figs_all_dir = Path(paths["fig_all_methods_dir"])
    figs_selected_dir = Path(paths["fig_selected_dir"])
    out_dir = Path(paths["output_dir"])
    ensure_dirs(figs_all_dir, figs_selected_dir, out_dir)

    rename_maps = cfg.get("rename_maps", {})
    wc_rename = rename_maps.get("wc", {})
    q_rename = rename_maps.get("q", {})

    # variables & metadata
    chem_variables: List[str] = cfg.get("chem_variables", [])
    pars_meta_df = pd.DataFrame(cfg.get("pars_metadata", []))
    standard_name_map: Dict[str, str] = cfg.get("standard_name_map", {})
    var_comments_cfg: Dict[str, Any] = cfg.get("var_comments", {})

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

    global_metadata_config: Dict[str, Any] = cfg.get("global_metadata_config", {})
    processed_namespace_uuid = uuid.UUID(cfg["processed_namespace_uuid"])
    meta_map = meta_cfg(cfg)

    # ---- load & harmonize ----
    if not wc_file.exists():
        raise FileNotFoundError(f"Water chemistry file not found: {wc_file}")
    if not q_file.exists():
        raise FileNotFoundError(f"Discharge file not found: {q_file}")

    wc_fname, wc_df_raw = load_netcdf_to_dataframe(wc_file, time_vars=(wc_time_col, "time", "sample_date"))
    q_fname, q_df_raw = load_netcdf_to_dataframe(q_file, time_vars=(q_time_col, "time", "date"))

    # station id we will work with (from meta or from wc)
    station_name_val = read_meta_value(wc_df_raw, meta_map, "station_name")
    if station_name_val is None and wc_station_col in wc_df_raw.columns:
        svals = wc_df_raw[wc_station_col].dropna().unique()
        station_name_val = svals[0] if len(svals) else "UNKNOWN"
    station_id = str(station_name_val)

    _, wc_df = process_df(wc_fname, wc_df_raw, wc_time_col, wc_station_col, "date", wc_rename, "river_name")
    _, q_df = process_df(q_fname, q_df_raw, q_time_col, q_station_col, "date", q_rename, "river_name")

    # rename discharge column if needed -> 'discharge'
    if discharge_var in q_df.columns and discharge_var != "discharge":
        q_df = q_df.rename(columns={discharge_var: "discharge"})

    # columns to drop from chem before merge (optional)
    drop_cols = cfg.get("drop_columns", ['latitude', 'longitude', 'station_id', 'station_code', 'station_type'])

    merged = merge_daily_wc_q(
        wc_df, q_df,
        station_name=station_id,
        station_col="river_name",
        date_col="date",
        discharge_col="discharge",
        drop_cols=drop_cols,
    )

    # ---- run interpolation ----
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

    # Attach monthly_interp to the station frame so we carry river_name + discharge for consistent joins
    st_plus_month = (
        merged.set_index("date")
        .merge(df_month_interp.set_index("date"), left_index=True, right_index=True, how="left")
        .reset_index()
    )

    # ---- combine method outputs ----
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

    # --- Group and aggregate daily values
    df_grouped = (
        merged3.groupby(["date", "river_name"])
        .agg(lambda x: x.mean() if pd.api.types.is_numeric_dtype(x) else x.iloc[0])
        .reset_index()
    )

    # Resample to daily per station
    df_daily_all = (
        df_grouped.set_index("date")
        .groupby("river_name")
        .resample("D")
        .mean(numeric_only=True)
        .reset_index()
        .sort_values(["river_name", "date"])
    )

    # Copy for plotting/selection
    df_sel = df_daily_all.copy()

    # ---- method selection per variable ----
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

        # Evaluate candidates
        for suffix in candidate_suffixes:
            colname = f"{var}_{suffix}"
            if colname not in df_station.columns:
                continue
            y_pred = df_obs_range[colname]
            valid = y_obs.notna() & y_pred.notna()
            if valid.sum() < 10:
                continue
            r2 = r2_score(y_obs[valid], y_pred[valid])
            method_scores[var].setdefault(station_id, {})[suffix] = {"R2": r2}
            if r2 >= r2_threshold:
                good_methods.append({'suffix': suffix, 'r2': r2, 'colname': colname, 'y_pred': y_pred})

        good_methods.sort(key=lambda x: x['r2'], reverse=True)

        selected_col: Optional[str] = None
        selected_series: Optional[pd.Series] = None

        # Outlier check
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
            print(f"{station_id} -> {var}: Selected {selected_col} (R2 = {m['r2']:.3f})")
            break

        # Fallback to linear
        if selected_col is None:
            fallback_col = f"{var}_{fallback_method}"
            if fallback_col in df_obs_range.columns:
                selected_col = fallback_col
                selected_series = df_obs_range[fallback_col].copy()
                print(f"{station_id} -> {var}: Fallback to {fallback_col}")
            else:
                print(f"{station_id} -> {var}: No valid method available")
                continue

        # Fill gaps from best of {annual_gam, monthly_regres}
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
                method_scores[var].setdefault(station_id, {})[suffix] = {"R2": r2b}

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
                    print(f"{station_id} -> {var}: Gaps filled from fallback {fallback_used} (R2 = {best_fb['r2']:.3f})")

        # Save decision
        methods_chosen[var][station_id] = [selected_col, fallback_used] if fallback_used else selected_col

        # Write back as *_final — align by date
        mask = df_daily_all["river_name"] == station_id
        dates = pd.DatetimeIndex(df_daily_all.loc[mask, "date"].values)
        aligned = filled_series.reindex(dates)
        df_daily_all.loc[mask, f"{var}_final"] = aligned.values

        # QC plot
        pred_df = filled_series.rename("pred").reset_index()  # date, pred
        df_plot = df_sel[df_sel["river_name"] == station_id][["date", "river_name", var]].merge(
            pred_df, on="date", how="left"
        )

        # Label + R2 for the legend/title
        entry = methods_chosen[var][station_id]
        if isinstance(entry, str):
            method_label = entry
            suf_base = entry.replace(f"{var}_", "")
            r2_for_label = method_scores.get(var, {}).get(station_id, {}).get(suf_base, {}).get("R2", None)
        else:
            base_col, fb_col = entry[0], entry[1]
            method_label = base_col if not fb_col else f"{base_col} + {fb_col}"
            suf_fb = fb_col.replace(f"{var}_", "") if fb_col else None
            suf_base = base_col.replace(f"{var}_", "")
            r2_for_label = None
            if suf_fb:
                r2_for_label = method_scores.get(var, {}).get(station_id, {}).get(suf_fb, {}).get("R2", None)
            if r2_for_label is None:
                r2_for_label = method_scores.get(var, {}).get(station_id, {}).get(suf_base, {}).get("R2", None)

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

    # ---- big overview plots
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

    # ---- export final
    final_cols = [c for c in df_daily_all.columns if c.endswith("_final")]
    export = df_daily_all[["date", "river_name"] + final_cols].copy()
    export = export.rename(columns={c: c.replace("_final", "") for c in final_cols})
    export = export.sort_values(["river_name", "date"])

    # -> xarray per station (here one station)
    df_station = export[export["river_name"] == station_id].copy()
    df_station["date"] = pd.to_datetime(df_station["date"])
    df_station = df_station.set_index("date")
    ds = xr.Dataset.from_dataframe(df_station.drop(columns=["river_name"]))

    # coord attrs
    ds = ds.assign_coords(**{time_name_out: ("date", df_station.index)})
    ds = ds.swap_dims({"date": time_name_out})
    if time_name_out != "date":
        ds = ds.drop_vars("date")
    ds[time_name_out].attrs.update({"standard_name": "time", "long_name": "Time of measurement", "axis": "T"})

    # coordinates (lat/lon from meta if present)
    lat = read_meta_value(wc_df_raw, meta_map, "latitude")
    lon = read_meta_value(wc_df_raw, meta_map, "longitude")
    if lat is None:
        lat = meta_map.get("latitude", {}).get("value")
    if lon is None:
        lon = meta_map.get("longitude", {}).get("value")
    if lat is not None and lon is not None:
        ds = ds.assign_coords(
            latitude=xr.DataArray(float(lat), dims=(), attrs={"standard_name": "latitude", "long_name": "Latitude", "units": "degree_north"}),
            longitude=xr.DataArray(float(lon), dims=(), attrs={"standard_name": "longitude", "long_name": "Longitude", "units": "degree_east"}),
        ).set_coords(["latitude", "longitude"])

    # river_name as scalar id
    ds["river_name"] = xr.DataArray(station_id, dims=(), attrs={"cf_role": "timeseries_id"})

    # annotate variables
    for var in ds.data_vars:
        if var == "river_name":
            continue
        if var in set(pars_meta_df["parameter_name"]):
            row = pars_meta_df[pars_meta_df["parameter_name"] == var].iloc[0]
            ds[var].attrs["units"] = str(row["unit"])
            ds[var].attrs["long_name"] = str(standard_name_map.get(var, var))
        # comments can be dict per station or single string
        vc = var_comments_cfg.get(var)
        if isinstance(vc, dict):
            com = vc.get(station_id)
            if com:
                ds[var].attrs["comment"] = str(com)
        elif isinstance(vc, str):
            ds[var].attrs["comment"] = vc

    # global attrs (keep what's in JSON; only fill missing)
    gmeta = dict(global_metadata_config)
    unique_id = f"no.niva:{uuid.uuid5(processed_namespace_uuid, station_id)}"
    gmeta["id"] = unique_id

    gmeta.setdefault("title", f"Daily water chemistry concentrations estimated for river {station_id}")
    gmeta.setdefault("title_no", f"Estimerte daglige konsentrasjoner av vannkjemi for elv {station_id}")
    gmeta.setdefault(
        "summary",
        f"Daily time series of water chemistry for river {station_id}, estimated by harmonizing observed "
        f"data and interpolating missing values (linear, GAM, monthly regressions, monthly medians).",
    )
    gmeta.setdefault(
        "summary_no",
        f"Daglige tidsserier for vannkjemi ved elv {station_id}, beregnet ved å harmonisere observerte målinger "
        f"og interpolere manglende verdier (lineær, GAM, månedlige regresjoner, månedlige medianer).",
    )
    gmeta.setdefault("date_created", pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
    gmeta["time_coverage_start"] = np.datetime_as_string(ds[time_name_out].min().values, unit="s", timezone="UTC")
    gmeta["time_coverage_end"] = np.datetime_as_string(ds[time_name_out].max().values, unit="s", timezone="UTC")
    if lat is not None and lon is not None:
        gmeta["geospatial_lat_min"] = float(lat); gmeta["geospatial_lat_max"] = float(lat)
        gmeta["geospatial_lon_min"] = float(lon); gmeta["geospatial_lon_max"] = float(lon)
    ds.attrs = {k: str(v) for k, v in gmeta.items()}

    # encoding
    encoding = {
        time_name_out: {"dtype": "int32", "_FillValue": None, "units": "seconds since 1970-01-01 00:00:00"},
    }
    if "latitude" in ds.coords:  encoding["latitude"]  = {"_FillValue": None}
    if "longitude" in ds.coords: encoding["longitude"] = {"_FillValue": None}

    out_name = f"{file_prefix}{station_id.lower().replace(' ', '_')}.nc"
    out_path = (out_dir / out_name).resolve()
    ds.to_netcdf(out_path, encoding=encoding, format="NETCDF4")
    print(f"Saved NetCDF: {out_path}")

    return [out_path]


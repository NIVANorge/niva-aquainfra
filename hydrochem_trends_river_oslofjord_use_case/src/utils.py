# from __future__ import annotations
#
# import json
#
# from pathlib import Path
# from typing import Dict, Any, List
#
# def ensure_dirs(*paths: str | Path) -> None:
#     for p in paths:
#         Path(p).mkdir(parents=True, exist_ok=True)
#
# def load_json(path: str | Path) -> Dict[str, Any]:
#     with open(path, "r", encoding="utf-8") as f:
#         return json.load(f)
#
# def expand_globs(root: str | Path, pattern: str) -> List[Path]:
#     root = Path(root)
#     return sorted([Path(p) for p in root.glob(pattern)])

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd
import xarray as xr


# ---------------------------- filesystem ----------------------------

def ensure_dirs(*paths: str | Path) -> None:
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)


def project_root() -> Path:
    """
    Returns project root assuming structure:
      <root>/src/utils.py
    i.e. parents[1] from this file is <root>.
    """
    return Path(__file__).resolve().parents[1]


def resolve_path(p: str | Path, *, root: Optional[str | Path] = None) -> Path:
    """
    Resolve p to an absolute path.
    - If p is already absolute -> return it
    - Else join with root (defaults to project_root()).
    """
    pp = Path(p)
    if pp.is_absolute():
        return pp
    base = Path(root) if root is not None else project_root()
    return (base / pp).resolve()


# ---------------------------- config/json ----------------------------

def load_json(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def expand_globs(root: str | Path, pattern: str) -> List[Path]:
    root = Path(root)
    return sorted(root.glob(pattern))


# ---------------------------- netcdf / io ----------------------------

def netcdf_to_dataframe(
    nc_path: str | Path,
    *,
    time_vars: Iterable[str] = ("time", "date", "sample_date", "datetime", "timestamp"),
) -> pd.DataFrame:
    """
    Open NetCDF with xarray and return a flat DataFrame.
    Also coerces any time-like columns listed in time_vars to datetime.
    """
    nc_path = Path(nc_path)
    with xr.open_dataset(nc_path) as ds:
        df = ds.to_dataframe().reset_index()

    for t in time_vars:
        if t in df.columns:
            df[t] = pd.to_datetime(df[t], errors="coerce")
    return df


# ---------------------------- dataframe harmonization ----------------------------

def standardize_time_and_station(
    df: pd.DataFrame,
    *,
    time_col_in: str,
    date_col_out: str = "date",
    station_col_in: str = "station_name",
    station_col_out: str = "river_name",
    station_rename_map: Optional[Dict[str, str]] = None,
    normalize_date: bool = True,
) -> pd.DataFrame:
    """
    - rename time_col_in -> date_col_out
    - parse date as datetime
    - optionally normalize (set time to 00:00)
    - rename station_col_in -> station_col_out and apply rename map
    """
    out = df.copy()

    if time_col_in in out.columns and time_col_in != date_col_out:
        out = out.rename(columns={time_col_in: date_col_out})

    if date_col_out in out.columns:
        out[date_col_out] = pd.to_datetime(out[date_col_out], errors="coerce")
        if normalize_date:
            out[date_col_out] = out[date_col_out].dt.normalize()

    if station_col_in in out.columns:
        if station_rename_map:
            out[station_col_in] = out[station_col_in].replace(station_rename_map)
        if station_col_in != station_col_out:
            out = out.rename(columns={station_col_in: station_col_out})

    return out


# ---------------------------- daily merge helper ----------------------------

def merge_daily_discharge_and_chemistry(
    wc_df: pd.DataFrame,
    q_df: pd.DataFrame,
    *,
    station_name: str,
    station_col: str = "river_name",
    date_col: str = "date",
    discharge_col: str = "discharge",
    drop_wc_cols: Sequence[str] | bool = False,
) -> pd.DataFrame:
    """
    Creates a complete daily date range based on Q coverage, merges discharge and chemistry,
    averages duplicates by day (numeric only), and returns a daily DataFrame with station_col restored.
    """
    wc = wc_df.copy()
    q = q_df.copy()

    # filter station
    if station_col in wc.columns:
        wc = wc[wc[station_col] == station_name]
    if station_col in q.columns:
        q = q[q[station_col] == station_name]

    if q.empty:
        raise ValueError(f"No discharge rows for station '{station_name}'")

    q = q.drop_duplicates(subset=[date_col]).copy()

    full_dates = pd.date_range(q[date_col].min(), q[date_col].max(), freq="D")
    out = pd.DataFrame({date_col: full_dates})
    out = out.merge(q[[date_col, discharge_col]], on=date_col, how="left")

    if isinstance(drop_wc_cols, list) or isinstance(drop_wc_cols, tuple):
        wc = wc.drop(columns=list(drop_wc_cols), errors="ignore")

    out = out.merge(wc, on=date_col, how="left")

    # average duplicates by day (numeric only)
    num_cols = out.select_dtypes(include="number").columns.tolist()
    out_num = out.groupby(date_col)[num_cols].mean(numeric_only=True).reset_index()
    out_num[station_col] = station_name

    cols = [date_col, station_col]
    if discharge_col in out_num.columns:
        cols.append(discharge_col)
    rest = [c for c in out_num.columns if c not in cols]
    return out_num[cols + rest]


# ---------------------------- plotting convenience (optional) ----------------------------

def save_or_show_plot(*, save_path: str | Path | None, dpi: int = 300) -> None:
    """
    Standardize the repeated pattern:
      if save_path: mkdir + savefig + close
      else: show
    Assumes you already created the figure using matplotlib.pyplot.
    """
    import matplotlib.pyplot as plt  # local import to keep utils lighter

    if save_path:
        p = Path(save_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(p, dpi=dpi, bbox_inches="tight")
        plt.close()
    else:
        plt.show()

# # ######### Check a netcdf
# # from pathlib import Path
# # import xarray as xr
# # from pprint import pprint
# #
# # nc_path = Path("../data/processed/cleaned_riverchem_40355.nc")
# #
# # with xr.open_dataset(nc_path) as ds:
# #     print("Opened:", nc_path.resolve())
# #
# #     # --- Global attributes ---
# #     print("\n=== Global attributes ===")
# #     pprint(dict(ds.attrs))
# #
# #     # --- Coordinate attributes (time/lat/lon/etc.) ---
# #     print("\n=== Coordinate variables ===")
# #     for c in ds.coords:
# #         da = ds[c]
# #         print(f"[coord] {c} dims={da.dims} shape={da.shape} dtype={da.dtype}")
# #         if da.attrs:
# #             for k, v in da.attrs.items():
# #                 print(f"  - {k}: {v}")
# #         if da.encoding:
# #             print("  encoding:", da.encoding)
# #
# #     # --- Data variable attributes ---
# #     print("\n=== Data variables ===")
# #     for v in ds.data_vars:
# #         da = ds[v]
# #         print(f"[var] {v} dims={da.dims} shape={da.shape} dtype={da.dtype}")
# #         if da.attrs:
# #             for k, val in da.attrs.items():
# #                 print(f"  - {k}: {val}")
# #         if da.encoding:
# #             print("  encoding:", da.encoding)
# #
# # print('-'*50)
#
# #### VERIFY FPATHS
# import json, sys
# from pathlib import Path
#
# p = Path("../config/river/drammenselva/interpolate.json")  # adjust if needed
# cfg = json.loads(p.read_text(encoding="utf-8"))
# inp = cfg["input"]
#
# print("CONFIG:", p)
# print("INPUT  file :", cfg["input"]["file"])
# print("FIGURES dir :", cfg["paths"]["fig_dir"])
# print("OUTPUT  dir :", cfg["paths"]["output_dir"])
# print("Time col    :", (cfg["input"].get("time_col") or "(auto-detect)"))
# # Existence checks
# print("\nEXISTS?")
# print(" - input file :", Path(cfg["input"]["file"]).exists())
# print(" - figures dir:", Path(cfg["paths"]["fig_dir"]).resolve())
# print(" - output dir :", Path(cfg["paths"]["output_dir"]).resolve())
#
#
# inp = cfg["input"]
# print("CONFIG:", p)
# if "file" in inp:
#     print("Mode         : single file")
#     f = Path(inp["file"])
#     print("INPUT file   :", f.resolve())
#     print("Exists       :", f.exists())
# else:
#     print("Mode         : dir + glob")
#     root = Path(inp["raw_data_dir"])
#     patt = inp["file_glob"]
#     print("raw_data_dir :", root.resolve())
#     print("file_glob    :", patt)
#     files = sorted(root.glob(patt))
#     print("Matched files:"); [print(" -", f.resolve()) for f in files]
#     if not files: print(" (!!) No input files matched)")

# # ###### RUN PREPROCESS
# import json, sys
# from pathlib import Path
# import xarray as xr
# import numpy as np
#
# ROOT = Path(__file__).resolve().parents[1]     # subproject root
# if str(ROOT) not in sys.path:
#     sys.path.insert(0, str(ROOT))
#
# from src.preprocess import preprocess          # absolute import works now
#
# cfg_path = ROOT / "config/river/drammenselva/preprocess.json"
# cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
#
# written = preprocess(cfg)
# print("\nWROTE NetCDF:")
# for p in written:
#     print(" -", p)
#
# # ---- SUMMARY OF THE JUST-WRITTEN FILE ----
# if not written:
#     print("\n(No files were written)")
# else:
#     # pick the last written file
#     nc_path = Path(written[-1]).resolve()
#
#     print("\n=== SUMMARY ===")
#     print("File   :", nc_path)
#     ds = xr.open_dataset(nc_path)
#
#     # basics
#     print("dims   :", dict(ds.dims))
#     print("coords :", list(ds.coords))
#     print("n_vars :", len(ds.data_vars))
#
#     # time
#     if "time" in ds.coords:
#         print("time   :", str(ds.time.min().values), "→", str(ds.time.max().values))
#         print("n_time :", ds.sizes.get("time"))
#
#     # station meta (if present)
#     def scalar(name):
#         if name in ds.variables:
#             try:
#                 return ds[name].values.item()
#             except Exception:
#                 return str(ds[name].values)
#         return None
#
#     print("\nmeta   :")
#     for k in ["station_id", "station_code", "station_name", "station_type"]:
#         val = scalar(k)
#         if val is not None:
#             print(f"  {k}: {val}")
#     if "latitude" in ds.coords and "longitude" in ds.coords:
#         print("  lat/lon:", float(ds["latitude"].values), float(ds["longitude"].values))
#
#     # quick stats for first ~10 variables
#     print("\nvars   : (first 10 with count/min/max)")
#     for i, var in enumerate(ds.data_vars):
#         if i == 10:
#             print("  ...")
#             break
#         da = ds[var]
#         units = da.attrs.get("units", "")
#         arr = da.values
#         finite = np.isfinite(arr)
#         n = int(finite.sum())
#         vmin = float(np.nanmin(arr)) if n > 0 else float("nan")
#         vmax = float(np.nanmax(arr)) if n > 0 else float("nan")
#         print(f"  - {var} [{units}]  n={n}  min={vmin}  max={vmax}")
#
#     # ---- GLOBAL METADATA ----
#     print("\n=== Global attributes ===")
#     for k, v in sorted(ds.attrs.items()):
#         print(f" - {k}: {v}")
#
#     # (optional) common CF/ACDD keys at a glance
#     print("\nCF/ACDD quick view:")
#     for k in ["Conventions", "featureType", "project", "id", "title",
#               "time_coverage_start", "time_coverage_end", "license"]:
#         if k in ds.attrs:
#             print(f" - {k}: {ds.attrs[k]}")
#
#     # ---- VARIABLE METADATA (pick a variable) ----
#     var = "DOC"  # <- change to any variable name you care about
#     if var in ds:
#         da = ds[var]
#         print(f"\n=== Variable: {var} ===")
#         print("dims   :", da.dims, " shape:", da.shape, " dtype:", da.dtype)
#         print("attrs  :")
#         if da.attrs:
#             for k, v in da.attrs.items():
#                 print(f" - {k}: {v}")
#         else:
#             print(" (no attrs)")
#         # comment (if present)
#         if "comment" in da.attrs:
#             print("comment:", da.attrs["comment"])
#         # encoding used for write-out (fill value, dtype, units, etc.)
#         if da.encoding:
#             keep = ("_FillValue", "dtype", "units", "scale_factor", "add_offset", "missing_value")
#             enc = {k: da.encoding[k] for k in keep if k in da.encoding}
#             print("encoding:", enc)
#     else:
#         print(f"\nVariable '{var}' not found. Available:", list(ds.data_vars)[:15], "...")
#
#     # ---- COORDINATE ATTRS (time/lat/lon) ----
#     for c in ["time", "latitude", "longitude"]:
#         if c in ds.coords:
#             print(f"\n=== Coord: {c} ===")
#             print("attrs:", dict(ds[c].attrs))
#             if c == "time":
#                 print("range:", str(ds.time.min().values), "→", str(ds.time.max().values))
#
#     ds.close()


# src/check_interpolation_paths.py
from pathlib import Path
import json
import sys

# Optional: deep checks
import xarray as xr

CFG_PATH = Path("../config/river/drammenselva/interpolate.json")  # adjust if needed

cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))
inp = cfg["input"]
paths = cfg["paths"]

wc_fp = Path(inp["waterchem_file"])
q_fp  = Path(inp["discharge_file"])

print("CONFIG:", CFG_PATH.resolve())
print("\n-- INPUTS --")
print(" waterchem_file :", wc_fp.resolve())
print("   exists       :", wc_fp.exists())
print(" discharge_file :", q_fp.resolve())
print("   exists       :", q_fp.exists())

print("\n-- PATHS --")
print(" fig_all_methods_dir:", Path(paths["fig_all_methods_dir"]).resolve())
print(" fig_selected_dir   :", Path(paths["fig_selected_dir"]).resolve())
print(" output_dir         :", Path(paths["output_dir"]).resolve())

print("\n-- COLUMNS/VARIABLES FROM CONFIG --")
print(" wc_time_col   :", inp.get("wc_time_col"))
print(" q_time_col    :", inp.get("q_time_col"))
print(" wc_station_col:", inp.get("wc_station_col"))
print(" q_station_col :", inp.get("q_station_col"))
print(" discharge_var :", inp.get("discharge_var"))

# -------- optional deep checks (open files and verify columns/vars) --------
if wc_fp.exists():
    with xr.open_dataset(wc_fp) as ds_wc:
        wc_time = inp.get("wc_time_col", "time")
        wc_station = inp.get("wc_station_col", "station_name")
        assert wc_time in ds_wc.coords or wc_time in ds_wc.variables, (
            f"[waterchem] time col '{wc_time}' not found. "
            f"Available coords: {list(ds_wc.coords)}, vars: {list(ds_wc.variables)}"
        )
        if wc_station not in ds_wc.variables and wc_station not in ds_wc:
            print(f"⚠️  [waterchem] station column '{wc_station}' not found (ok if you don’t need it here).")
        else:
            print(f"✅ [waterchem] found time '{wc_time}' and station '{wc_station}'")

if q_fp.exists():
    with xr.open_dataset(q_fp) as ds_q:
        q_time = inp.get("q_time_col", "date")
        q_station = inp.get("q_station_col", "station_name")
        q_var = inp.get("discharge_var", "discharge")

        assert q_time in ds_q.coords or q_time in ds_q.variables, (
            f"[discharge] time col '{q_time}' not found. "
            f"Available coords: {list(ds_q.coords)}, vars: {list(ds_q.variables)}"
        )
        assert q_var in ds_q.data_vars, (
            f"[discharge] discharge var '{q_var}' not found. "
            f"Data vars: {list(ds_q.data_vars)}"
        )
        if q_station not in ds_q.variables and q_station not in ds_q:
            print(f"⚠️  [discharge] station column '{q_station}' not found (ok if single-station file).")
        print(f"✅ [discharge] found time '{q_time}' and var '{q_var}'")
print('-'*100)
# src/run_interpolation_check.py
# src/test.py (or run_interpolation_check.py)
import sys, json, argparse
from pathlib import Path
from typing import Dict, Any, List
import numpy as np
import xarray as xr

# Make "src" importable when running as a script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from src.interpolate import interpolate  # preferred absolute import
except ImportError:
    from interpolate import interpolate      # fallback if run from inside src/

def _as_unit_map(pars_metadata: List[Dict[str, Any]]) -> Dict[str, str]:
    out = {}
    for row in pars_metadata:
        pn = str(row.get("parameter_name", "")).strip()
        if pn:
            out[pn] = str(row.get("unit", "")).strip()
    return out

def _ok(msg: str) -> None: print(f"✅ {msg}")
def _warn(msg: str) -> None: print(f"⚠️  {msg}")
def _fail(msg: str) -> None: print(f"❌ {msg}")

def check_dataset(ds: xr.Dataset, cfg: Dict[str, Any], path: Path) -> None:
    print(f"\n=== SUMMARY: {path} ===")
    chem_vars: List[str] = cfg["chem_variables"]
    time_name: str = cfg["output"]["time_name"]
    gmeta_cfg: Dict[str, Any] = cfg.get("global_metadata_config", {})
    unit_map = _as_unit_map(cfg.get("pars_metadata", []))
    standard_name_map: Dict[str, str] = cfg.get("standard_name_map", {})

    assert time_name in ds.coords, f"Expected time coordinate '{time_name}' not found. Found: {list(ds.coords)}"
    _ok(f"time coordinate present: '{time_name}'")

    for c in ("latitude", "longitude"):
        assert c in ds.coords, f"Missing coordinate: {c}"
    _ok("latitude/longitude coordinates present")

    assert "river_name" in ds, "Missing scalar variable 'river_name'"
    _ok("river_name variable present")

    if "date_created" in gmeta_cfg:
        exp = gmeta_cfg["date_created"]
        got = str(ds.attrs.get("date_created", ""))
        assert got == exp, f"date_created mismatch. expected={exp} got={got}"
        _ok(f"date_created matches config: {got}")
    else:
        _warn("date_created not fixed in config; skipping exact match")

    missing = [v for v in chem_vars if v not in ds.data_vars]
    assert not missing, f"Missing expected variables: {missing}"
    _ok("all expected chemistry variables present")

    for v in chem_vars:
        attrs = ds[v].attrs
        if unit_map.get(v):
            assert str(attrs.get("units", "")) == unit_map[v], f"{v}: units mismatch (got '{attrs.get('units')}', expected '{unit_map[v]}')"
        exp_long = standard_name_map.get(v)
        if exp_long and str(attrs.get("long_name", "")) != exp_long:
            _warn(f"{v}: long_name differs (got '{attrs.get('long_name','')}', expected '{exp_long}')")
    _ok("units OK; long_name checked")

    print("\n-- NaN summary (count / % of time steps) --")
    n_time = ds.sizes[time_name]
    for v in chem_vars:
        a = ds[v].to_numpy()
        n_nan = int(np.isnan(a).sum())
        pct = (100.0 * n_nan / n_time) if n_time else 0.0
        print(f"  {v:14s} : {n_nan:5d}  ({pct:5.1f}%)")

    tmin = np.datetime_as_string(ds[time_name].min().values, unit="s")
    tmax = np.datetime_as_string(ds[time_name].max().values, unit="s")
    print(f"\nTime coverage: {tmin}  →  {tmax}")
    _ok("dataset looks consistent")

def main():
    parser = argparse.ArgumentParser(description="Run interpolation and validate outputs.")
    parser.add_argument(
        "--config", "-c",
        default=str(ROOT / "config/river/drammenselva/interpolate.json"),
        help="Path to interpolation JSON config (default: %(default)s)"
    )
    args = parser.parse_args()

    cfg_path = Path(args.config)
    assert cfg_path.exists(), f"Config file not found: {cfg_path}"
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

    wc_path = Path(cfg["input"]["waterchem_file"]).resolve()
    q_path = Path(cfg["input"]["discharge_file"]).resolve()
    print("CONFIG:", cfg_path.resolve(), "\n")
    print("-- INPUTS --")
    print(" waterchem_file :", wc_path);   print("   exists       :", wc_path.exists())
    print(" discharge_file :", q_path);    print("   exists       :", q_path.exists())

    out_all = Path(cfg["paths"]["fig_all_methods_dir"]).resolve()
    out_sel = Path(cfg["paths"]["fig_selected_dir"]).resolve()
    out_dir = Path(cfg["paths"]["output_dir"]).resolve()
    print("\n-- PATHS --")
    print(" fig_all_methods_dir:", out_all)
    print(" fig_selected_dir   :", out_sel)
    print(" output_dir         :", out_dir)

    _ok(f"inputs found:\n - {wc_path}\n - {q_path}")

    print("\nRunning interpolation …")
    written_paths = interpolate(cfg)
    if not written_paths:
        raise RuntimeError("interpolate(cfg) returned no output paths")

    print("\nWROTE NetCDF:")
    for pth in written_paths:
        print(" -", Path(pth).resolve())

    for pth in written_paths:
        with xr.open_dataset(pth) as ds:
            check_dataset(ds, cfg, Path(pth).resolve())

    print("\nAll checks passed.")

if __name__ == "__main__":
    main()
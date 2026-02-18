import argparse
import matplotlib.pyplot as plt

from pathlib import Path

from src.utils import load_json as load_cfg
from src.preprocess import preprocess
from src.interpolate import interpolate
from src.estimate_fluxes import flux
from src.mk_trend_test import analyze_trends

plt.style.use("ggplot")

# Choose exactly which steps to run (any combination), or use ["all"].
STEPS_OVERRIDE = ["fluxes"] # e.g. ["interpolate", "fluxes", "trends"]

# Choose which rivers to run (any list), or ["all"], or None (use argparse defaults)
RIVERS_OVERRIDE = ["all"]  # e.g. ["drammenselva"] or ["all"] or None

# Choose which marine datasets to run (any list), or ["all"], or None
MARINE_OVERRIDE = None  # e.g. ["oslofjord"] or ["all"] or None

def available_names(base_dir: Path) -> list[str]:
    if not base_dir.exists():
        return []
    return sorted([p.name for p in base_dir.iterdir() if p.is_dir()])


def run_river(
    river: str,
    steps: list[str],
    cfg_base: Path,
    *,
    trend_freq: str,
    mk_mode: str,
    trend_sites: list[str] | None,
) -> None:
    river_dir = cfg_base / river
    print(f"\n=== River: {river} ===")

    if "preprocess" in steps:
        cfg = load_cfg(river_dir / "preprocess.json")
        preprocess(cfg)

    if "interpolate" in steps:
        cfg = load_cfg(river_dir / "interpolate.json")
        interpolate(cfg)

    if "fluxes" in steps:
        cfg = load_cfg(river_dir / "fluxes.json")
        flux(cfg)

    # --- trends stays as an IF (as you requested) ---
    if "trends" in steps:
        trends_path = Path("config") / "mk_trend_test.json"
        if not trends_path.exists():
            raise FileNotFoundError(f"Missing trends config: {trends_path}")
        cfg = load_cfg(trends_path)
        analyze_trends(cfg, frequency=trend_freq, mk_mode=mk_mode, stations=trend_sites)

# def run_marine(
#     dataset: str,
#     steps: list[str],
#     cfg_base: Path,
#     *,
#     trend_freq: str,
#     mk_mode: str,
#     trend_sites: list[str] | None,
# ) -> None:
#     ds_dir = cfg_base / dataset
#     print(f"\n=== Marine: {dataset} ===")
#
#     # If later you add preprocess/interpolate/flux for marine, you can mirror river logic here.
#     # For now: only trends makes sense.
#
#     if "trends" in steps:
#         trends_path = ds_dir / "mk_trend_test.json"
#         if not trends_path.exists():
#             raise FileNotFoundError(f"Missing trends config: {trends_path}")
#         cfg = load_cfg(trends_path)
#         analyze_trends(cfg, frequency=trend_freq, mk_mode=mk_mode, stations=trend_sites)
#

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--step",
        default="preprocess",
        help="preprocess|interpolate|fluxes|trends|all OR comma-list like 'interpolate,fluxes,trends'",
    )
    ap.add_argument("--rivers", nargs="+", default=["drammenselva"], help="River folder names, or: all")
    ap.add_argument("--marine", nargs="+", default=[], help="Marine dataset folder names, or: all")

    # --- trends knobs ---
    ap.add_argument("--trend_freq", default="both", choices=["monthly", "annual", "both"])
    ap.add_argument("--mk_mode", default="auto", choices=["auto", "original", "seasonal"])
    ap.add_argument(
        "--trend_sites",
        nargs="*",
        default=None,
        help="Optional override for sites used in trends (otherwise uses cfg['site_li'] or cfg['sites'])",
    )

    args = ap.parse_args()

    # --- overrides for running from PyCharm (no terminal args) ---
    if STEPS_OVERRIDE is not None:
        if "all" in STEPS_OVERRIDE:
            args.step = "all"
        else:
            args.step = ",".join(STEPS_OVERRIDE)

    if RIVERS_OVERRIDE is not None:
        args.rivers = RIVERS_OVERRIDE

    if MARINE_OVERRIDE is not None:
        args.marine = MARINE_OVERRIDE

    cfg_river_base = Path("config/river")
    cfg_marine_base = Path("config/marine")

    rivers_all = available_names(cfg_river_base)
    marine_all = available_names(cfg_marine_base)

    # Steps parsing (supports "all" or comma list)
    if args.step == "all":
        steps = ["preprocess", "interpolate", "fluxes", "trends"]
    else:
        steps = [s.strip() for s in str(args.step).split(",") if s.strip()]

    # Rivers selection
    rivers = rivers_all if args.rivers == ["all"] else args.rivers

    # Marine selection
    marine = marine_all if args.marine == ["all"] else args.marine

    # Validate rivers (only if river pipeline steps are requested OR if trends is requested for rivers)
    missing_rivers = [r for r in rivers if r not in rivers_all]
    if missing_rivers:
        raise SystemExit(f"Unknown rivers: {missing_rivers}. Available: {rivers_all}")

    # Validate marine (only if any marine selection was given)
    if marine:
        missing_marine = [m for m in marine if m not in marine_all]
        if missing_marine:
            raise SystemExit(f"Unknown marine datasets: {missing_marine}. Available: {marine_all}")

    # --- Run rivers ---
    for r in rivers:
        run_river(
            r,
            steps,
            cfg_river_base,
            trend_freq=args.trend_freq,
            mk_mode=args.mk_mode,
            trend_sites=args.trend_sites,
        )

    # # --- Run marine trends (only if user provided --marine something) ---
    # # This does NOT interfere with river trends. It's additive.
    # for ds in marine:
    #     run_marine(
    #         ds,
    #         steps,
    #         cfg_marine_base,
    #         trend_freq=args.trend_freq,
    #         mk_mode=args.mk_mode,
    #         trend_sites=args.trend_sites,
    #     )


if __name__ == "__main__":
    main()



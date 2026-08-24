import argparse
import matplotlib.pyplot as plt

from pathlib import Path

from src.utils import load_json as load_cfg
from src.preprocess import preprocess
from src.interpolate import interpolate
from src.estimate_fluxes import flux
from src.mk_trend_test import analyze_trends

plt.style.use("ggplot")

TRENDS_CONFIG = "mk_trend_test.json"

# Choose exactly which steps to run (any combination), or use ["all"].
STEPS_OVERRIDE = ["fluxes"] # e.g. ["interpolate", "fluxes", "trends"]

# Choose which rivers to run (any list), or ["all"], or None
RIVERS_OVERRIDE = ["glomma"]  # e.g. ["drammenselva"] or ["all"] or None


def available_names(base_dir: Path) -> list[str]:
    if not base_dir.exists():
        return []
    return sorted([p.name for p in base_dir.iterdir() if p.is_dir()])


def run_river(
    river: str,
    steps: list[str],
    cfg_base: Path,
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


def run_trends(
    *,
    trends_config: str,
    trend_freq: str,
    mk_mode: str,
) -> None:

    trends_path = Path("config") / trends_config

    if not trends_path.exists():
        raise FileNotFoundError(
            f"Missing trends config: {trends_path}"
        )

    cfg = load_cfg(trends_path)

    print(
        "\n=== Combined river + marine trends ==="
    )

    analyze_trends(
        cfg,
        frequency=trend_freq,
        mk_mode=mk_mode,
    )


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--step",
        default="preprocess",
        help="preprocess|interpolate|fluxes|trends|all OR comma-list like 'interpolate,fluxes,trends'",
    )

    ap.add_argument(
        "--rivers",
        nargs="+",
        default=["drammenselva"],
        help="River folder names, or: all",
    )


    ap.add_argument(
        "--trends_config",
        default=TRENDS_CONFIG,
        help="Trend config filename inside config/",
    )

    ap.add_argument(
        "--trend_freq",
        default="config",
        choices=["config", "monthly", "annual", "seasonal_by_season", "both"],
    )

    ap.add_argument(
        "--mk_mode",
        default="auto",
        choices=["auto", "original", "seasonal"],
    )


    args = ap.parse_args()

    if STEPS_OVERRIDE is not None:
        if "all" in STEPS_OVERRIDE:
            args.step = "all"
        else:
            args.step = ",".join(STEPS_OVERRIDE)

    if RIVERS_OVERRIDE is not None:
        args.rivers = RIVERS_OVERRIDE

    cfg_river_base = Path("config/river")

    rivers_all = available_names(cfg_river_base)

    if args.step == "all":
        steps = ["preprocess", "interpolate", "fluxes", "trends"]
    else:
        steps = [s.strip() for s in str(args.step).split(",") if s.strip()]

    rivers = rivers_all if args.rivers == ["all"] else args.rivers

    missing_rivers = [r for r in rivers if r not in rivers_all]
    if missing_rivers:
        raise SystemExit(f"Unknown rivers: {missing_rivers}. Available: {rivers_all}")

    if steps == ["trends"]:
        run_trends(
            trends_config=args.trends_config,
            trend_freq=args.trend_freq,
            mk_mode=args.mk_mode,
        )
        return

    for r in rivers:
        run_river(
            r,
            steps,
            cfg_river_base,
        )

    if "trends" in steps:
        run_trends(
            trends_config=args.trends_config,
            trend_freq=args.trend_freq,
            mk_mode=args.mk_mode,
        )


if __name__ == "__main__":
    main()
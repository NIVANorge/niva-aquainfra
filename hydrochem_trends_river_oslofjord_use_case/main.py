import argparse
import matplotlib.pyplot as plt
from pathlib import Path

from src.utils import load_json as load_cfg
from src.interpolate import interpolate
from src.estimate_fluxes import flux
from src.mk_trend_test import analyze_trends

plt.style.use("ggplot")

WORKFLOW_CONFIG = "workflow.json"


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

    print("\n=== Combined river + marine trends ===")

    analyze_trends(
        cfg,
        frequency=trend_freq,
        mk_mode=mk_mode,
    )


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--workflow_config",
        default=WORKFLOW_CONFIG,
        help="Workflow config filename inside config/",
    )

    ap.add_argument(
        "--step",
        default=None,
        help="Optional override: interpolate|fluxes|trends|all or comma-list",
    )

    ap.add_argument(
        "--rivers",
        nargs="+",
        default=None,
        help="Optional river override: river folder names, or all",
    )

    ap.add_argument(
        "--trends_config",
        default=None,
        help="Optional trend config override",
    )

    ap.add_argument(
        "--trend_freq",
        default=None,
        choices=["config", "monthly", "annual", "seasonal_by_season", "both"],
    )

    ap.add_argument(
        "--mk_mode",
        default=None,
        choices=["auto", "original", "seasonal"],
    )

    args = ap.parse_args()

    workflow_path = Path("config") / args.workflow_config
    if not workflow_path.exists():
        raise FileNotFoundError(
            f"Missing workflow config: {workflow_path}"
        )

    workflow = load_cfg(workflow_path)

    steps = workflow.get("steps", [])
    rivers = workflow.get("rivers", ["all"])

    trends_workflow = workflow.get("trends", {})
    trends_config = trends_workflow.get("config", "mk_trend_test.json")
    trend_freq = trends_workflow.get("frequency", "config")
    mk_mode = trends_workflow.get("mk_mode", "auto")

    if args.step is not None:
        if args.step == "all":
            steps = ["interpolate", "fluxes", "trends"]
        else:
            steps = [s.strip() for s in args.step.split(",") if s.strip()]

    if args.rivers is not None:
        rivers = args.rivers

    if args.trends_config is not None:
        trends_config = args.trends_config

    if args.trend_freq is not None:
        trend_freq = args.trend_freq

    if args.mk_mode is not None:
        mk_mode = args.mk_mode

    valid_steps = {"interpolate", "fluxes", "trends"}
    unknown_steps = [step for step in steps if step not in valid_steps]

    if unknown_steps:
        raise SystemExit(
            f"Unknown workflow steps: {unknown_steps}. "
            f"Available: {sorted(valid_steps)}"
        )

    cfg_river_base = Path("config/river")
    rivers_all = available_names(cfg_river_base)

    if rivers == ["all"]:
        rivers = rivers_all

    missing_rivers = [r for r in rivers if r not in rivers_all]
    if missing_rivers:
        raise SystemExit(
            f"Unknown rivers: {missing_rivers}. Available: {rivers_all}"
        )

    print("\n=== Workflow ===")
    print(f"Steps: {steps}")

    if "interpolate" in steps or "fluxes" in steps:
        print(f"Rivers: {rivers}")

    if "trends" in steps:
        print(f"Trend config: {trends_config}")
        print(f"Trend frequency: {trend_freq}")
        print(f"MK mode: {mk_mode}")

    for river in rivers:
        run_river(
            river,
            steps,
            cfg_river_base,
        )

    if "trends" in steps:
        run_trends(
            trends_config=trends_config,
            trend_freq=trend_freq,
            mk_mode=mk_mode,
        )


if __name__ == "__main__":
    main()
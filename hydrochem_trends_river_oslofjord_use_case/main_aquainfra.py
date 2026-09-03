from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from src.estimate_fluxes import flux
from src.interpolate import interpolate
from src.mk_trend_test import analyze_trends
from src.utils import load_json as load_cfg


REPO_ROOT = Path(__file__).resolve().parent

INTERPOLATION_TEMPLATE = (
    REPO_ROOT / "config" / "aquainfra" / "interpolate.json"
)

FLUX_TEMPLATE = (
    REPO_ROOT / "config" / "aquainfra" / "fluxes.json"
)

TREND_TEMPLATE = REPO_ROOT / "config" / "aquainfra" / "mk_trend_test.json"


def parse_steps(value: str) -> list[str]:
    allowed = {"interpolate", "fluxes", "trends"}

    steps = [step.strip().lower() for step in value.split(",") if step.strip()]

    invalid = set(steps) - allowed
    if invalid:
        raise argparse.ArgumentTypeError(
            f"Unknown processing step(s): {', '.join(sorted(invalid))}. "
            f"Allowed values are: {', '.join(sorted(allowed))}."
        )

    if not steps:
        raise argparse.ArgumentTypeError("At least one processing step is required.")

    return steps


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="River and coastal hydrochemistry analysis workflow."
    )

    parser.add_argument(
        "--steps",
        required=True,
        type=parse_steps,
        help=(
            "Comma-separated processing steps: "
            "interpolate, fluxes, trends."
        ),
    )

    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where workflow outputs will be written.",
    )

    # River input
    parser.add_argument(
        "--waterchem",
        help="Cleaned river water-chemistry NetCDF file or OPeNDAP URL.",
    )

    parser.add_argument(
        "--interpolated-waterchem",
        help=(
            "Daily interpolated river water-chemistry NetCDF file. "
            "Required when fluxes are run without interpolation."
        ),
    )

    parser.add_argument(
        "--discharge",
        help="Cleaned daily river-discharge NetCDF file.",
    )

    parser.add_argument(
        "--river-name",
        help="River name/identifier used in the outputs.",
    )

    parser.add_argument(
        "--wc-station-name",
        help="Station name as stored in the water-chemistry input.",
    )

    parser.add_argument(
        "--q-station-name",
        help="Station name as stored in the discharge input.",
    )

    parser.add_argument(
        "--latitude",
        type=float,
        help="River station latitude in decimal degrees.",
    )

    parser.add_argument(
        "--longitude",
        type=float,
        help="River station longitude in decimal degrees.",
    )

    parser.add_argument(
        "--marine-input",
        help="Marine chemistry NetCDF file or OPeNDAP URL.",
    )

    parser.add_argument(
        "--river-flux-dir",
        help=(
            "Folder containing daily river flux NetCDF files when trends "
            "are run without the flux step."
        ),
    )

    parser.add_argument(
        "--trend-frequency",
        default="config",
        choices=["config", "monthly", "annual", "seasonal_by_season"],
        help="Temporal frequency for trend analysis.",
    )

    parser.add_argument(
        "--mk-mode",
        default="auto",
        choices=["auto", "original", "seasonal"],
        help="Mann-Kendall test mode.",
    )

    parser.add_argument(
        "--trend-start-year",
        type=int,
        help="Optional first year of the trend-analysis period.",
    )

    parser.add_argument(
        "--trend-end-year",
        type=int,
        help="Optional last year of the trend-analysis period.",
    )

    parser.add_argument(
        "--variables",
        help=(
            "Comma-separated variables to process, for example "
            "DOC,TOTN,TOTP,NO3-N. If omitted, variables from the internal "
            "configuration are used."
        ),
    )

    parser.add_argument(
        "--creator-name",
        help="Name of the person or organisation responsible for the dataset.",
    )

    parser.add_argument(
        "--creator-email",
        help="Contact email for the dataset creator.",
    )

    parser.add_argument(
        "--creator-institution",
        help="Institution responsible for creating the dataset.",
    )

    parser.add_argument(
        "--data-owner",
        help="Person or organisation owning the dataset.",
    )

    parser.add_argument(
        "--project",
        help="Project associated with the generated dataset.",
    )

    parser.add_argument(
        "--license",
        dest="license_name",
        help="License applying to the generated dataset.",
    )

    parser.add_argument(
        "--creator-url",
        help="URL identifying the dataset creator.",
    )

    parser.add_argument(
        "--institution",
        help="Institution associated with the generated dataset.",
    )

    parser.add_argument(
        "--institution-short-name",
        help="Short name or acronym of the institution.",
    )

    parser.add_argument(
        "--publisher-name",
        help="Name of the dataset publisher.",
    )

    parser.add_argument(
        "--publisher-email",
        help="Contact email for the dataset publisher.",
    )

    parser.add_argument(
        "--publisher-institution",
        help="Institution responsible for publishing the dataset.",
    )

    parser.add_argument(
        "--publisher-url",
        help="URL of the dataset publisher.",
    )

    parser.add_argument(
        "--naming-authority",
        help="Naming authority used for generated dataset identifiers.",
    )

    parser.add_argument(
        "--keywords",
        help="Comma-separated keywords describing the generated dataset.",
    )

    parser.add_argument(
        "--linear-max-gap",
        type=int,
        help="Maximum gap length in days for linear interpolation.",
    )

    parser.add_argument(
        "--gam-n-splines-q",
        type=int,
        help="Number of GAM splines for discharge.",
    )

    parser.add_argument(
        "--gam-n-splines-doy",
        type=int,
        help="Number of GAM splines for day of year.",
    )

    parser.add_argument(
        "--validation-repeats",
        type=int,
        help="Number of blocked-holdout validation repeats.",
    )

    parser.add_argument(
        "--validation-block-size",
        type=int,
        help="Block size used in holdout validation.",
    )

    parser.add_argument(
        "--validation-min-observations",
        type=int,
        help="Minimum number of observations required for validation.",
    )

    parser.add_argument(
        "--validation-min-points",
        type=int,
        help="Minimum number of validation points required.",
    )

    parser.add_argument(
        "--validation-min-coverage",
        type=float,
        help="Minimum validation coverage required for a candidate method.",
    )

    parser.add_argument(
        "--selection-min-r2",
        type=float,
        help="Minimum R-squared required for a candidate interpolation method.",
    )

    parser.add_argument(
        "--selection-r2-tolerance",
        type=float,
        help="R-squared tolerance used when comparing acceptable methods.",
    )

    parser.add_argument(
        "--selection-max-bias-fraction",
        type=float,
        help="Maximum allowed absolute bias as a fraction of the observed mean.",
    )

    parser.add_argument(
        "--selection-extreme-ratio-limit",
        type=float,
        help="Limit used to flag implausible extreme model predictions.",
    )

    parser.add_argument(
        "--trend-alpha",
        type=float,
        help="Significance level used for the Mann-Kendall trend test.",
    )

    parser.add_argument(
        "--trend-min-points-monthly",
        type=int,
        help="Minimum number of points required for monthly trend analysis.",
    )

    parser.add_argument(
        "--trend-min-points-annual",
        type=int,
        help="Minimum number of points required for annual trend analysis.",
    )

    parser.add_argument(
        "--trend-min-points-seasonal",
        type=int,
        help="Minimum number of points required for seasonal trend analysis.",
    )

    parser.add_argument(
        "--marine-depth-min",
        type=float,
        help="Minimum marine sampling depth in metres.",
    )

    parser.add_argument(
        "--marine-depth-max",
        type=float,
        help="Maximum marine sampling depth in metres.",
    )

    parser.add_argument(
        "--river-min-obs-monthly",
        type=int,
        help="Minimum number of daily river values required to create a monthly value.",
    )

    parser.add_argument(
        "--river-min-obs-annual",
        type=int,
        help="Minimum number of daily river values required to create an annual value.",
    )

    parser.add_argument(
        "--river-min-obs-seasonal",
        type=int,
        help="Minimum number of daily river values required to create a seasonal value.",
    )

    parser.add_argument(
        "--marine-min-obs-monthly",
        type=int,
        help="Minimum number of marine observations required to create a monthly value.",
    )

    parser.add_argument(
        "--marine-min-obs-annual",
        type=int,
        help="Minimum number of marine observations required to create an annual value.",
    )

    parser.add_argument(
        "--marine-min-obs-seasonal",
        type=int,
        help="Minimum number of marine observations required to create a seasonal value.",
    )

    parser.add_argument(
        "--wc-time-col",
        help="Name of the time coordinate/column in the water-chemistry input.",
    )

    parser.add_argument(
        "--q-time-col",
        help="Name of the time coordinate/column in the discharge input.",
    )

    parser.add_argument(
        "--wc-station-col",
        help="Name of the station coordinate/column in the water-chemistry input.",
    )

    parser.add_argument(
        "--q-station-col",
        help="Name of the station coordinate/column in the discharge input.",
    )

    parser.add_argument(
        "--discharge-var",
        help="Name of the discharge variable in the discharge input.",
    )

    parser.add_argument(
        "--flux-date-col",
        help="Name of the date coordinate/column in the interpolated chemistry input.",
    )

    parser.add_argument(
        "--flux-station-col",
        help="Name of the station coordinate/column used in flux processing.",
    )

    parser.add_argument(
        "--flux-discharge-col",
        help="Name of the discharge variable/column used in flux processing.",
    )

    parser.add_argument(
        "--non-mass-vars",
        help=(
            "Comma-separated variables that should not be interpreted as mass fluxes, "
            "for example Color,UV_Abs_254nm,UV_Abs_410nm."
        ),
    )

    parser.add_argument(
        "--trend-require-full-period",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Require the complete requested trend period. "
            "Use --trend-require-full-period or --no-trend-require-full-period."
        ),
    )

    parser.add_argument(
        "--marine-require-all-seasons",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Require all seasons to be represented when creating annual marine values. "
            "Use --marine-require-all-seasons or --no-marine-require-all-seasons."
        ),
    )

    parser.add_argument(
        "--marine-min-seasons-per-year",
        type=int,
        help="Minimum number of seasons required to create an annual marine value.",
    )

    parser.add_argument(
        "--marine-station-dim",
        help="Name of the station dimension in the marine input.",
    )

    parser.add_argument(
        "--marine-station-coord",
        help="Name of the station-name coordinate in the marine input.",
    )

    parser.add_argument(
        "--marine-depth-dim",
        help="Name of the depth dimension/coordinate in the marine input.",
    )

    parser.add_argument(
        "--variable-units",
        help=(
            "Comma-separated variable=unit pairs for processed variables, "
            "for example DOC=mg/l,TOTN=ug/l,TOTP=ug/l."
        ),
    )

    return parser


def require_river_arguments(args: argparse.Namespace) -> None:
    required = {
        "--discharge": args.discharge,
        "--river-name": args.river_name,
        "--q-station-name": args.q_station_name,
        "--latitude": args.latitude,
        "--longitude": args.longitude,
    }

    if "interpolate" in args.steps:
        required["--waterchem"] = args.waterchem
        required["--wc-station-name"] = args.wc_station_name

    if "fluxes" in args.steps and "interpolate" not in args.steps:
        required["--interpolated-waterchem"] = args.interpolated_waterchem

    missing = [name for name, value in required.items() if value is None]

    if missing:
        raise ValueError(
            "The following arguments are required for the selected processing steps: "
            + ", ".join(missing)
        )

def parse_variable_list(value: str | None) -> list[str] | None:
    if not value:
        return None

    variables = [item.strip() for item in value.split(",") if item.strip()]

    return variables or None

def apply_user_metadata(
    cfg: dict,
    args: argparse.Namespace,
) -> None:
    metadata = cfg.setdefault("global_metadata_config", {})

    user_values = {
        "creator_name": args.creator_name,
        "creator_email": args.creator_email,
        "creator_institution": args.creator_institution,
        "creator_url": args.creator_url,
        "institution": args.institution,
        "institution_short_name": args.institution_short_name,
        "data_owner": args.data_owner,
        "publisher_name": args.publisher_name,
        "publisher_email": args.publisher_email,
        "publisher_institution": args.publisher_institution,
        "publisher_url": args.publisher_url,
        "project": args.project,
        "license": args.license_name,
        "naming_authority": args.naming_authority,
        "keywords": args.keywords,
    }

    for key, value in user_values.items():
        if value is not None:
            metadata[key] = value

    timestamps = cfg.setdefault("metadata", {}).setdefault("timestamps", {})
    timestamps["date_created"] = (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )

def parse_variable_mapping(value: str | None) -> dict[str, str] | None:
    if not value:
        return None

    mapping = {}

    for item in value.split(","):
        item = item.strip()

        if not item:
            continue

        if "=" not in item:
            raise ValueError(
                f"Invalid mapping '{item}'. Expected format variable=value."
            )

        key, val = item.split("=", 1)
        key = key.strip()
        val = val.strip()

        if not key or not val:
            raise ValueError(
                f"Invalid mapping '{item}'. Expected format variable=value."
            )

        mapping[key] = val

    return mapping or None

def make_interpolation_config(
    args: argparse.Namespace,
    output_root: Path,
) -> dict:
    cfg = deepcopy(load_cfg(INTERPOLATION_TEMPLATE))

    apply_user_metadata(cfg, args)

    if args.wc_time_col is not None:
        cfg["input"]["wc_time_col"] = args.wc_time_col

    if args.q_time_col is not None:
        cfg["input"]["q_time_col"] = args.q_time_col

    if args.wc_station_col is not None:
        cfg["input"]["wc_station_col"] = args.wc_station_col

    if args.q_station_col is not None:
        cfg["input"]["q_station_col"] = args.q_station_col

    if args.discharge_var is not None:
        cfg["input"]["discharge_var"] = args.discharge_var

    variables = parse_variable_list(args.variables)
    variable_units = parse_variable_mapping(args.variable_units)

    if variables is not None:
        cfg["chem_variables"] = variables

        existing_metadata = {
            item["parameter_name"]: item
            for item in cfg.get("pars_metadata", [])
            if "parameter_name" in item
        }

        pars_metadata = []

        for variable in variables:
            item = deepcopy(
                existing_metadata.get(
                    variable,
                    {"parameter_name": variable},
                )
            )

            if variable_units is not None and variable in variable_units:
                item["unit"] = variable_units[variable]

            pars_metadata.append(item)

        cfg["pars_metadata"] = pars_metadata

    if args.linear_max_gap is not None:
        cfg["interpolation"]["linear"]["max_gap"] = args.linear_max_gap

    if args.gam_n_splines_q is not None or args.gam_n_splines_doy is not None:
        current = cfg["interpolation"]["gam"]["n_splines_xy"]

        cfg["interpolation"]["gam"]["n_splines_xy"] = [
            args.gam_n_splines_q
            if args.gam_n_splines_q is not None
            else current[0],
            args.gam_n_splines_doy
            if args.gam_n_splines_doy is not None
            else current[1],
        ]

    river = args.river_name
    river_folder = river.lower().replace(" ", "_")

    cfg["input"]["waterchem_file"] = args.waterchem
    cfg["input"]["discharge_file"] = args.discharge

    cfg["rename_maps"]["wc"] = {
        args.wc_station_name: river
    }

    cfg["rename_maps"]["q"] = {
        args.q_station_name: river
    }

    cfg["meta"]["station_name"]["value"] = river
    cfg["meta"]["latitude"]["value"] = args.latitude
    cfg["meta"]["longitude"]["value"] = args.longitude

    interpolation_root = output_root / "daily_estimates"

    cfg["paths"]["output_dir"] = str(
        interpolation_root / "data"
    )

    cfg["paths"]["fig_all_methods_dir"] = str(
        interpolation_root / "figures" / river_folder / "all_methods"
    )

    cfg["paths"]["fig_selected_dir"] = str(
        interpolation_root / "figures" / river_folder / "selected"
    )

    cfg["paths"]["fig_validation_dir"] = str(
        interpolation_root / "figures" / river_folder / "validation"
    )

    validation = cfg["validation"]

    if args.validation_repeats is not None:
        validation["n_repeats"] = args.validation_repeats

    if args.validation_block_size is not None:
        validation["block_size"] = args.validation_block_size

    if args.validation_min_observations is not None:
        validation["min_observations"] = args.validation_min_observations

    if args.validation_min_points is not None:
        validation["min_validation_points"] = args.validation_min_points

    if args.validation_min_coverage is not None:
        validation["min_coverage"] = args.validation_min_coverage

    selection = cfg["selection"]

    if args.selection_min_r2 is not None:
        selection["min_r2"] = args.selection_min_r2

    if args.selection_r2_tolerance is not None:
        selection["r2_tolerance"] = args.selection_r2_tolerance

    if args.selection_max_bias_fraction is not None:
        selection["max_abs_bias_fraction"] = args.selection_max_bias_fraction

    if args.selection_extreme_ratio_limit is not None:
        selection["extreme_ratio_limit"] = args.selection_extreme_ratio_limit

    return cfg


def make_flux_config(
    args: argparse.Namespace,
    output_root: Path,
    interpolated_file: Path,
) -> dict:
    cfg = deepcopy(load_cfg(FLUX_TEMPLATE))

    apply_user_metadata(cfg, args)

    non_mass_vars = parse_variable_list(args.non_mass_vars)

    if non_mass_vars is not None:
        cfg["unit_options"]["non_mass_vars"] = non_mass_vars

    if args.flux_date_col is not None:
        cfg["date_col"] = args.flux_date_col

    if args.flux_station_col is not None:
        cfg["station_col"] = args.flux_station_col

    if args.flux_discharge_col is not None:
        cfg["discharge_col"] = args.flux_discharge_col

    variables = parse_variable_list(args.variables)
    variable_units = parse_variable_mapping(args.variable_units)

    if variables is not None:
        parameter_names = cfg["flux_metadata"]["parameter_name"]
        units = cfg["flux_metadata"]["unit"]

        existing_units = dict(zip(parameter_names, units))

        cfg["flux_metadata"]["parameter_name"] = variables
        cfg["flux_metadata"]["unit"] = [
            (
                variable_units[variable]
                if variable_units is not None and variable in variable_units
                else existing_units.get(
                    variable,
                    cfg["unit_options"]["undefined_unit_label"],
                )
            )
            for variable in variables
        ]

    river = args.river_name

    cfg["wc_interp_data_path"] = str(interpolated_file)
    cfg["q_cleaned_data_path"] = args.discharge

    cfg["q_station_rename_map"] = {
        args.q_station_name: river
    }

    cfg["river"] = river

    cfg["river_coords"] = {
        "lat": args.latitude,
        "lon": args.longitude,
    }

    cfg["plots_output_dir"] = str(
        output_root / "fluxes" / "figures"
    )

    cfg["output_dir"] = str(
        output_root / "fluxes" / "data"
    )

    # The proxy-variable comments are river-specific in the template.
    for variable, station_comments in cfg.get("var_comments", {}).items():
        if station_comments:
            original_comment = next(iter(station_comments.values()))
            cfg["var_comments"][variable] = {
                river: original_comment
            }

    return cfg


def find_trend_source(cfg: dict, data_type: str) -> dict | None:
    for source in cfg.get("inputs", {}).get("daily", []):
        if source.get("data_type") == data_type:
            return source

    return None


def make_trend_config(
    args: argparse.Namespace,
    output_root: Path,
) -> dict:
    cfg = deepcopy(load_cfg(TREND_TEMPLATE))

    non_mass_vars = parse_variable_list(args.non_mass_vars)

    if non_mass_vars is not None:
        cfg["unit_options"]["non_mass_vars"] = non_mass_vars

    variables = parse_variable_list(args.variables)

    if variables is not None:
        cfg["variables"] = variables

    cfg["output_dir"] = str(output_root / "trends")

    river_source = find_trend_source(cfg, "river")
    marine_source = find_trend_source(cfg, "marine")

    def apply_aggregation_requirements(
            source: dict | None,
            monthly: int | None,
            annual: int | None,
            seasonal: int | None,
    ) -> None:
        if source is None:
            return

        requirements = source.setdefault("aggregation_requirements", {})

        if monthly is not None:
            requirements.setdefault("monthly", {})["min_obs_per_period"] = monthly

        if annual is not None:
            requirements.setdefault("annual", {})["min_obs_per_period"] = annual

        if seasonal is not None:
            requirements.setdefault("seasonal_by_season", {})[
                "min_obs_per_period"
            ] = seasonal

    apply_aggregation_requirements(
        river_source,
        args.river_min_obs_monthly,
        args.river_min_obs_annual,
        args.river_min_obs_seasonal,
    )

    apply_aggregation_requirements(
        marine_source,
        args.marine_min_obs_monthly,
        args.marine_min_obs_annual,
        args.marine_min_obs_seasonal,
    )

    use_river = "fluxes" in args.steps or bool(args.river_flux_dir)
    use_marine = bool(args.marine_input)

    if not use_river and not use_marine:
        raise ValueError(
            "Trend analysis requires either --river-flux-dir, "
            "--marine-input, or the fluxes step in the same run."
        )

    if use_river:
        if "fluxes" in args.steps:
            river_source["path"] = str(
                output_root / "fluxes" / "data" / "daily"
            )
        else:
            river_source["path"] = args.river_flux_dir

    if use_marine:
        marine_source["path"] = args.marine_input

    # Keep only the trend input types requested for this job.
    cfg["inputs"]["daily"] = [
        source
        for source in cfg["inputs"]["daily"]
        if (
                   source.get("data_type") == "river" and use_river
           ) or (
                   source.get("data_type") == "marine" and use_marine
           )
    ]

    if args.river_name:
        cfg.setdefault("stations", {})["river"] = [args.river_name]

    # Optional period override.
    period = cfg.setdefault("trend_options", {}).setdefault("period", {})

    if args.trend_start_year is not None:
        period["start_year"] = args.trend_start_year

    if args.trend_end_year is not None:
        period["end_year"] = args.trend_end_year

    if args.trend_require_full_period is not None:
        period["require_full_period"] = args.trend_require_full_period

    trend_options = cfg.setdefault("trend_options", {})

    if args.trend_alpha is not None:
        trend_options["alpha"] = args.trend_alpha

    min_points = trend_options.setdefault("min_points", {})

    if args.trend_min_points_monthly is not None:
        min_points["monthly"] = args.trend_min_points_monthly

    if args.trend_min_points_annual is not None:
        min_points["annual"] = args.trend_min_points_annual

    if args.trend_min_points_seasonal is not None:
        min_points["seasonal_by_season"] = args.trend_min_points_seasonal


    if use_marine and marine_source is not None:
        depth_selection = marine_source.setdefault("depth_selection", {})

        marine_annual = (
            marine_source
            .setdefault("aggregation_requirements", {})
            .setdefault("annual", {})
        )

        if args.marine_require_all_seasons is not None:
            marine_annual["require_all_seasons"] = args.marine_require_all_seasons

        if args.marine_min_seasons_per_year is not None:
            marine_annual["min_seasons_per_year"] = args.marine_min_seasons_per_year

        if args.marine_station_dim is not None:
            marine_source["station_dim"] = args.marine_station_dim

        if args.marine_station_coord is not None:
            marine_source["station_coord"] = args.marine_station_coord

        if args.marine_depth_dim is not None:
            marine_source["depth_dim"] = args.marine_depth_dim

        if args.marine_depth_min is not None:
            depth_selection["min"] = args.marine_depth_min

        if args.marine_depth_max is not None:
            depth_selection["max"] = args.marine_depth_max

    return cfg


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    output_root = Path(args.output_dir)

    river_steps = {"interpolate", "fluxes"} & set(args.steps)

    if river_steps:
        require_river_arguments(args)

    if "trends" in args.steps:
        use_river = "fluxes" in args.steps or bool(args.river_flux_dir)
        use_marine = bool(args.marine_input)

        if not use_river and not use_marine:
            raise ValueError(
                "Trend analysis requires either --river-flux-dir, "
                "--marine-input, or the fluxes step in the same run."
            )

    output_root.mkdir(parents=True, exist_ok=True)

    interpolated_file = None

    if "interpolate" in args.steps:
        interpolation_cfg = make_interpolation_config(
            args,
            output_root,
        )

        interpolate(interpolation_cfg)

        interpolated_file = (
            output_root
            / "daily_estimates"
            / "data"
            / f"daily_water_chemistry_modeled_{args.river_name}.nc"
        )

    if "fluxes" in args.steps:
        if interpolated_file is None:
            if not args.interpolated_waterchem:
                raise ValueError(
                    "--interpolated-waterchem is required when fluxes "
                    "are run without the interpolation step."
                )

            interpolated_file = Path(args.interpolated_waterchem)

        flux_cfg = make_flux_config(
            args,
            output_root,
            interpolated_file,
        )

        flux(flux_cfg)

    if "trends" in args.steps:
        trend_cfg = make_trend_config(
            args,
            output_root,
        )

        analyze_trends(
            trend_cfg,
            frequency=args.trend_frequency,
            mk_mode=args.mk_mode,
        )


if __name__ == "__main__":
    main()
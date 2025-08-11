import json
import sys
from pathlib import Path
import uuid

import xarray as xr
import pandas as pd
import numpy as np

from src.utils import (
    load_raw_data,
    apply_reconstruction,
    apply_derivations,
    plot_quality_control,
    detect_outliers_per_station,
    dataframe_to_xarray,
    save_dataset
)

def load_config(config_path):
    """Load JSON configuration file."""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def preprocess(config_path):
    """Main preprocessing workflow."""
    cfg = load_config(config_path)

    # Get paths from config
    raw_files = list(Path(cfg["input_pattern"]).parent.glob(Path(cfg["input_pattern"]).name))
    fig_dir = Path(cfg["fig_dir"])
    output_dir = Path(cfg["output_dir"])
    fig_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    for fp in raw_files:
        print(f"Processing: {fp}")

        # Load raw NetCDF → DataFrame
        df = load_raw_data(fp)

        # Reconstruction
        df = apply_reconstruction(df, cfg["reconstruction_config"], fig_dir, fp.name)

        # Derived variables
        df = apply_derivations(df, cfg["derivation_config"], fp.name)

        # QC Plots
        plot_quality_control(df, cfg["plot_config"], fig_dir, fp.name)

        # Outlier detection
        df = detect_outliers_per_station(df, fp.stem, cfg["outlier_config"], fig_dir)

        # DataFrame → xarray Dataset
        ds = dataframe_to_xarray(
            df,
            cfg["pars_metadata"],
            cfg["standard_name_map"],
            cfg["var_comments"]
        )

        # Save to NetCDF with metadata
        save_dataset(
            ds,
            output_dir,
            cfg["global_metadata_config"],
            uuid.UUID(cfg["processed_namespace_uuid"])
        )

    print("✅ Preprocessing complete.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/01_preprocess.py <config_path>")
        sys.exit(1)
    preprocess(sys.argv[1])

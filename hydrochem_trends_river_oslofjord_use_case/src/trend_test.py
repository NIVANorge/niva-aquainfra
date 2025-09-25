import math
import os
from pathlib import Path
from typing import Any, Dict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymannkendall as mk
import seaborn as sns
from scipy.stats import linregress

# Plot style
plt.style.use("ggplot")


def plot_fluxes(cfg: Dict[str, Any]) -> None:
    """
    TODO: Add documentation here

    """
    daily_flux_dict = {}
    for site in cfg["site_li"]:
        file_path = os.path.join(cfg["flux_folder"], f'daily_fluxes_{site}.csv')
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, parse_dates=['Date'], index_col='Date')
            daily_flux_dict[site] = df
        else:
            print(f"File for {site} not found.")

    for station_name, df_station in daily_flux_dict.items():
        vars_to_plot = list(cfg["display_names"].keys())
        n_vars = len(vars_to_plot)
        n_cols = 3
        n_rows = math.ceil(n_vars / n_cols)

        fig, axes = plt.subplots(nrows=n_rows, ncols=n_cols, figsize=(6 * n_cols, 4 * n_rows))
        axes = axes.flatten()

        for i, var in enumerate(vars_to_plot):
            ax = axes[i]
            flux_col = var

            if flux_col not in df_station.columns:
                continue

            # Plot flux data
            ax.plot(
                df_station.index, df_station[flux_col], label='Flux', linestyle='-', color='salmon', alpha=0.7, zorder=2
            )

            ax.set_title(cfg["display_names"].get(var, var), fontsize=10)
            ax.tick_params(axis='x', rotation=45)
            ax.grid(True)
            ax.legend(fontsize=8)

        for j in range(i + 1, len(axes)):
            axes[j].axis('off')

        fig.suptitle(f"{station_name} Fluxes", fontsize=14)
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.show()
        # TODO: should we save daily plots?

    output_dir = Path(cfg["output_dir"])
    os.makedirs(output_dir, exist_ok=True)

    for site in cfg["site_li"]:
        df = daily_flux_dict.get(site)
        if df is None:
            continue

        for var, label in cfg["display_names"].items():
            if var in df.columns:
                plt.figure(figsize=(10, 4))
                plt.plot(df.index, df[var], label=label, color='tab:blue')
                plt.title(f"{site}", fontsize=14)
                plt.ylabel(f"{label}", fontsize=14)
                plt.xlabel("Date")
                plt.grid(True)
                plt.tight_layout()
                safe_var = var.replace('/', '_')
                filename = f"{site}_{safe_var}_flux.png"
                plt.savefig(os.path.join(cfg["output_dir"], filename), dpi=300)
                plt.close()

    for var, label in cfg["display_names"].items():
        plt.figure(figsize=(12, 4))
        for site in cfg["site_li"]:
            df = daily_flux_dict.get(site)
            if df is not None and var in df.columns:
                plt.plot(df.index, df[var].rolling(30, min_periods=1).mean(), label=site, color=cfg["colors"][site])

        plt.title(" ")
        plt.ylabel(f"{label} [T]")
        plt.xlabel(" ")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(f"../plots/rivers/river_flux_plots/comparison_{var.replace('/', '_')}.png", dpi=300)
        plt.close()

    annual_flux_dict = {}

    for site in cfg["site_li"]:
        df = daily_flux_dict.get(site)
        if df is not None:
            annual_flux_df = df.resample('YE').sum(min_count=350)
            annual_flux_df['Year'] = annual_flux_df.index.year
            annual_flux_dict[site] = annual_flux_df.set_index('Year')

    annual_output_dir = output_dir / "river_flux_plots"
    os.makedirs(annual_output_dir, exist_ok=True)
    # Loop over variables and generate plots
    for var, label in cfg["display_names"].items():
        fig, ax = plt.subplots(figsize=(10, 5))
        data = pd.DataFrame()

        for site in cfg["site_li"]:
            if site in annual_flux_dict and var in annual_flux_dict[site].columns:
                data[site] = annual_flux_dict[site][var]

        if data.empty:
            continue

        data = data.dropna(how='all')

        # Plot grouped bars
        data.plot(kind='bar', ax=ax, width=0.75)
        ax.set_title(" ")
        ax.set_ylabel(f"{label} [T]")
        ax.set_xlabel(" ")
        ax.legend(title="River")
        ax.grid(True, axis='y')

        # Perform Mann-Kendall test and add trend lines
        for site in data.columns:
            y = data[site].dropna()
            x = y.index

            if len(y) >= 5:
                result = mk.original_test(y)
                if result.trend in ['increasing', 'decreasing'] and result.p <= 0.05:
                    slope, intercept, _, _, _ = linregress(x, y)
                    trend_years = data.index
                    trend_vals = slope * trend_years + intercept
                    ax.plot(
                        range(len(trend_years)),
                        trend_vals,
                        linestyle='--',
                        label=f"{site} trend ({result.trend}, p={result.p:.3f})",
                    )

        plt.tight_layout()
        safe_var = var.replace('/', '_')
        plt.savefig(annual_output_dir / f"annual_totals_trend_{safe_var}.png", dpi=300)
        plt.close()

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import xarray as xr
from pathlib import Path
from datetime import datetime, timezone
import uuid
import math

def load_raw_data(fp):
    """Load raw NetCDF file into a DataFrame."""
    ds = xr.open_dataset(fp)
    df = ds.to_dataframe().reset_index()

    if "sample_date" in df.columns:
        df["sample_date"] = pd.to_datetime(df["sample_date"], errors="coerce")
    elif "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")

    return df

def plot_reconstruction(df, date_col, var_orig, var_final,
                        label_base="", station_label="", units="", output_path=None):
    """Plot before/after reconstruction."""
    colors = {'original': '#1b9e77', 'final': '#d95f02'}
    if var_orig not in df.columns or var_final not in df.columns:
        return

    plt.figure(figsize=(10, 4))
    plt.plot(df[date_col], df[var_orig], label=f'{label_base} (original)',
             color=colors['original'], marker='o', linestyle='none')
    plt.plot(df[date_col], df[var_final], label=f'{label_base} (reconstructed)',
             color=colors['final'], marker='.', linestyle='-', alpha=0.7)
    plt.title(f"{station_label}")
    plt.ylabel(f"{label_base} ({units})" if units else label_base)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path)
        plt.close()
    else:
        plt.show()

def apply_reconstruction(df, reconstruction_config, fig_dir, fname):
    """Apply variable reconstruction rules."""
    df = df.copy()
    date_col = "sample_date" if "sample_date" in df.columns else None
    if not date_col:
        return df

    station_name = df['station_name'].dropna().unique()
    station_id = df['station_id'].dropna().unique()
    station_str = station_name[0] if len(station_name) > 0 else fname
    station_id_str = f" ({station_id[0]})" if len(station_id) > 0 else ""
    station_label = f"{station_str}{station_id_str}"

    intermediate_cols = []

    for var, settings in reconstruction_config.items():
        fallback = settings.get("fallback")
        compute_from = settings.get("compute_from")
        formula = settings.get("formula")
        preserve_as = settings.get("preserve_as", f"{var}_orig")
        calc_temp_var = settings.get("calc_temp_var", f"{var}_calc")
        units = settings.get("units", "")

        if var in df.columns:
            df[preserve_as] = df[var]

        if var in df.columns and fallback and fallback in df.columns:
            df[var] = df[var].fillna(df[fallback])
            intermediate_cols.append(fallback)

        if compute_from and compute_from in df.columns:
            df[calc_temp_var] = eval(formula, {}, {compute_from: df[compute_from]})
            if var in df.columns:
                df[var] = df[var].fillna(df[calc_temp_var])
            else:
                df[var] = df[calc_temp_var]
            intermediate_cols.extend([compute_from, calc_temp_var])

        if preserve_as in df.columns and var in df.columns:
            output_file = fig_dir / f"{fname.rstrip('.nc')}_{var}_reconstruction.png"
            plot_reconstruction(df, date_col, preserve_as, var,
                                label_base=var, station_label=station_label,
                                units=units, output_path=output_file)

        intermediate_cols.append(preserve_as)

    df = df.drop(columns=[col for col in intermediate_cols if col in df.columns])
    return df

def apply_derivations(df, derivation_config, fname):
    """Apply derivation rules."""
    df = df.copy()
    for rule in derivation_config:
        op = rule["operation"]
        target = rule.get("target")

        if op == "scale" and target in df.columns:
            df[target] = df[target] * rule["factor"]

        elif op == "sum":
            if all(col in df.columns for col in rule["sources"]):
                df[target] = df[rule["sources"][0]] + df[rule["sources"][1]]

        elif op == "fill_from_other":
            if all(col in df.columns for col in rule["requires"]):
                context = {col: df[col] for col in df.columns}
                cond = eval(rule["condition"], {}, context)
                df.loc[cond, target] = eval(rule["expression"], {}, context)

        elif op == "rowwise_sum":
            if all(col in df.columns for col in rule["sources"]):
                df[target] = df.apply(
                    lambda row: sum(row[col] for col in rule["sources"])
                    if all(pd.notna(row[col]) for col in rule["sources"])
                    else np.nan,
                    axis=1
                )

        elif op == "difference":
            if all(col in df.columns for col in rule["requires"]):
                df[target] = df[rule["requires"][0]] - df[rule["requires"][1]]

        elif op == "mask_date_before":
            if rule["file_contains"] in fname and target in df.columns and rule["date_column"] in df.columns:
                mask = pd.to_datetime(df[rule["date_column"]]) < pd.Timestamp(rule["before"])
                df.loc[mask, target] = np.nan
    return df

def plot_scatter(ax, x, y, xlabel, ylabel, title, unit=None):
    ax.scatter(x, y, alpha=0.5)
    if pd.notna(x).any() and pd.notna(y).any():
        lims = [min(x.min(), y.min()), max(x.max(), y.max())]
        ax.plot(lims, lims, 'r--')
    unit_str = f" ({unit})" if unit else ""
    ax.set_xlabel(f"{xlabel}{unit_str}")
    ax.set_ylabel(f"{ylabel}{unit_str}")
    ax.set_title(title)

def plot_lines(ax, df, columns, labels, title, unit=None):
    for col, label in zip(columns, labels):
        if col in df.columns:
            ax.plot(df['sample_date'], df[col], label=label)
    ax.legend()
    ax.set_title(title)
    if unit:
        ax.set_ylabel(f'{unit}')
    else:
        ax.set_ylabel('##')

def plot_quality_control(df, plot_config, fig_dir, fname):
    station = df['station_name'].dropna().unique()
    title = station[0] if len(station) else fname
    fig, axs = plt.subplots(3, 2, figsize=(14, 16))
    fig.suptitle(f"Station: {title}", fontsize=16)
    plotted = False

    for plot in plot_config:
        if not all(col in df.columns for col in plot["required"]):
            continue
        row, col = plot["subplot"]
        plotted = True
        if plot["type"] == "scatter":
            plot_scatter(axs[row, col], df[plot["x"]], df[plot["y"]],
                         plot["xlabel"], plot["ylabel"], plot["title"], unit=plot.get("unit"))
        elif plot["type"] == "line":
            plot_lines(axs[row, col], df, plot["columns"], plot["labels"],
                       plot["title"], unit=plot.get("unit"))

    axs[2, 1].axis('off')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    if plotted:
        output_file = fig_dir / f"{fname.rstrip('.nc')}_derived_pars.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()

def detect_outliers_per_station(df, station_name, outlier_config, fig_dir):
    df = df.copy()
    q_low = outlier_config["lower_quantile"]
    q_high = outlier_config["upper_quantile"]
    variables = [
        col for col in df.columns
        if pd.api.types.is_numeric_dtype(df[col]) and col not in ['station_id', 'depth', 'longitude', 'latitude']
    ]
    variables = [var for var in variables if var in df.columns and not df[var].dropna().empty]
    n_vars = len(variables)
    if n_vars == 0:
        return df

    ncols = 2
    nrows = math.ceil(n_vars / ncols)
    fig, axs = plt.subplots(nrows=nrows, ncols=ncols, figsize=(14, 4 * nrows), squeeze=False)
    fig.suptitle(f"{station_name} – Outlier Detection (Q{q_low*100:.2f}–Q{q_high*100:.2f})", fontsize=16)

    for idx, var in enumerate(variables):
        row, col = divmod(idx, ncols)
        ax = axs[row][col]
        series = df[var].dropna()
        p5 = series.quantile(q_low)
        p95 = series.quantile(q_high)
        outliers = (df[var] < p5) | (df[var] > p95)
        ax.scatter(df['sample_date'], df[var], color='gray', alpha=0.7)
        ax.scatter(df.loc[outliers, 'sample_date'], df.loc[outliers, var], color='red')
        ax.set_title(var)
        df.loc[outliers, var] = np.nan

    for i in range(n_vars, nrows * ncols):
        row, col = divmod(i, ncols)
        axs[row][col].axis('off')

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    output_path = fig_dir / f"{station_name}_detected_outliers.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return df

def dataframe_to_xarray(df, pars_metadata, standard_name_map, var_comments):
    df = df.copy()
    df["sample_date"] = pd.to_datetime(df["sample_date"])
    df = df.set_index("sample_date")

    lat = float(df["latitude"].iloc[0])
    lon = float(df["longitude"].iloc[0])
    station_id = int(df["station_id"].iloc[0])
    station_code = str(df["station_code"].iloc[0])
    station_name = str(df["station_name"].iloc[0])
    station_type = str(df["station_type"].iloc[0])

    df_clean = df.drop(columns=["latitude", "longitude", "station_id", "station_code", "station_name", "station_type"])
    ds = xr.Dataset.from_dataframe(df_clean)

    ds = ds.assign_coords(sample_date=("sample_date", df.index))
    ds = ds.assign_coords(
        latitude=xr.DataArray(lat, dims=(), attrs={"standard_name": "latitude", "units": "degree_north"}),
        longitude=xr.DataArray(lon, dims=(), attrs={"standard_name": "longitude", "units": "degree_east"})
    )
    ds = ds.set_coords(["latitude", "longitude"])
    ds["station_id"] = xr.DataArray(station_id, dims=())
    ds["station_code"] = xr.DataArray(station_code, dims=())
    ds["station_name"] = xr.DataArray(station_name, dims=())
    ds["station_type"] = xr.DataArray(station_type, dims=())

    for var in ds.data_vars:
        match = [m for m in pars_metadata if m["parameter_name"] == var]
        if match:
            ds[var].attrs["units"] = match[0]["unit"]
            ds[var].attrs["long_name"] = standard_name_map.get(var, var)
        if var in var_comments:
            ds[var].attrs["comment"] = var_comments[var]
    return ds

def save_dataset(ds, output_dir, global_metadata_config, processed_namespace_uuid):
    station_id = int(ds["station_id"].values.item())
    station_name = str(ds["station_name"].values.item())
    station_code = str(ds["station_code"].values.item())
    lat = float(ds["latitude"].values.item())
    lon = float(ds["longitude"].values.item())

    unique_id = f"no.niva:{uuid.uuid5(processed_namespace_uuid, str(station_id))}"
    dataset_metadata = global_metadata_config.copy()
    dataset_metadata.update({
        "id": unique_id,
        "title": f"Cleaned water chemistry measurements at station {station_name}",
        "summary": f"Cleaned long-term water chemistry monitoring at station {station_name} (code: {station_code})",
        "date_created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "time_coverage_start": np.datetime_as_string(ds.sample_date.min().values, unit="s", timezone="UTC"),
        "time_coverage_end": np.datetime_as_string(ds.sample_date.max().values, unit="s", timezone="UTC"),
        "geospatial_lat_min": lat,
        "geospatial_lat_max": lat,
        "geospatial_lon_min": lon,
        "geospatial_lon_max": lon
    })
    ds.attrs = {k: str(v) for k, v in dataset_metadata.items()}

    encoding = {
        "sample_date": {"dtype": "int32", "_FillValue": None, "units": "seconds since 1970-01-01 00:00:00"},
        "latitude": {"_FillValue": None},
        "longitude": {"_FillValue": None},
        "station_id": {"dtype": "int32", "_FillValue": -9999}
    }
    output_path = output_dir / f"cleaned_riverchem_{station_id}.nc"
    ds.to_netcdf(output_path, encoding=encoding, format="NETCDF4")
    print(f"Saved NetCDF: {output_path}")

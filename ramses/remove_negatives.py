#%%
import os
import glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

#%%
months = ["june", "july", "august", "september"]

#%%
dfs = []
for month in months:
    path = f"./FA_ramses_data/{month}"
    all_files = glob.glob(os.path.join(path, "*.csv"))
    df_from_each_file = (pd.read_csv(f, sep="\t", engine="python") for f in all_files)
    rrs = pd.concat(df_from_each_file, ignore_index=True)
    dfs.append(rrs)
df = pd.concat(dfs, ignore_index=True)
# drop wrong coordinates from pyniva for ramses and replace with correct coordinates from flags
df.drop(columns=["longitude", "latitude"], inplace=True)
print(df.shape)

#%%
flags = []
for month in months:
    path = f"./FA_ramses_data/{month}/flags"
    all_files = glob.glob(os.path.join(path, "*.csv"))
    df_from_each_file = (pd.read_csv(f, sep="\t", engine="python") for f in all_files)
    flag = pd.concat(df_from_each_file, ignore_index=True)
    flags.append(flag)
position = pd.concat(flags, ignore_index=True)

position["longitude"] = position["longitude"].interpolate()
position["latitude"] = position["latitude"].interpolate()
position["time"] = pd.to_datetime(position["time"])
position.drop(columns=[
    "index",
    "FA/RAMSES/DERIVED/RRS/CALIBRATED/SHIP_SHADDOW",
    "FA/RAMSES/DERIVED/RRS/CALIBRATED/SUN_GLINT",
    "FA/RAMSES/DERIVED/RRS/CALIBRATED/SUN_HEIGHT"
    ], inplace=True)
df['time'] = pd.to_datetime(df['time'])
df_correct_position = df.merge(position, on="time", how="left")
print(df_correct_position.shape)

#%%
rrs_wls = df_correct_position.drop(columns=[
    "time", "longitude", "latitude"], inplace=False)
rrs_wls_np = np.array(rrs_wls)

# OBS: Takes a long time to plot!
plt.figure(figsize=(12, 8))
for i in range(rrs_wls_np.shape[0]):
    plt.plot(rrs_wls_np[i])
plt.axhline(0, color="grey", linestyle="--", linewidth=1)
plt.xlabel("Wavelength (nm)")
plt.xticks(np.arange(0, len(rrs_wls.columns), 15), rrs_wls.columns[::15])
plt.ylabel("Rrs")
plt.ylim(bottom=-0.025)
plt.title("All Rrs values Jun-Sep 2023")
plt.savefig("./figs/all_rrs.png", dpi=300, bbox_inches="tight")
plt.show()

#%%
# Remove negative values
df_noneg = df_correct_position.set_index("time", inplace=False)
df_noneg = df_noneg[(df_noneg >= 0).all(1)]
df_noneg.reset_index(inplace=True)
print(df_noneg.shape)
df_noneg.to_csv("./datasets/noneg_rrs.csv", sep="\t", index=False)

print("Records removed:", len(df_correct_position) - len(df_noneg))
print("Fraction removed: {:.1%}".format(
    (len(df_correct_position) - len(df_noneg)) / len(df_correct_position)
))

#%%
rrs_noneg_wls = df_noneg.drop(columns=[
    "time", "longitude", "latitude"], inplace=False)
rrs_noneg_wls_np = np.array(rrs_noneg_wls)

plt.figure(figsize=(12, 8))
for i in range(rrs_noneg_wls_np.shape[0]):
    plt.plot(rrs_noneg_wls_np[i])
plt.xlabel("Wavelength (nm)")
plt.xticks(np.arange(0, len(rrs_noneg_wls.columns), 15), rrs_noneg_wls.columns[::15])
plt.ylabel("Rrs")
plt.title("Non-negative Rrs values Jun-Sep 2023")
plt.savefig("./figs/noneg_rrs.png", dpi=300, bbox_inches="tight")
plt.show()

#%%
# Coverage differences
def coverage_diffs(df):
    tmin, tmax = df["time"].min(), df["time"].max()
    latmin, latmax = df["latitude"].min(), df["latitude"].max()
    lonmin, lonmax = df["longitude"].min(), df["longitude"].max()
    return {
        "time_span": tmax - tmin,
        "lat_extent": latmax - latmin,
        "lon_extent": lonmax - lonmin
    }

orig_coverage = coverage_diffs(df_correct_position)
noneg_coverage = coverage_diffs(df_noneg)

print("Original coverage:", orig_coverage)
print("Cleaned coverage:", noneg_coverage)

#%%
# Temporal resolution
for name, df in [("All spectra", df_correct_position), ("Non-negative spectra", df_noneg)]:
    dt = df["time"].sort_values().diff().dt.total_seconds().dropna()
    print(f"\n{name} Δt (s) summary:")
    print(dt.describe(percentiles=[.01, .1, .5, .9, .99]))

    sorted_dt = np.sort(dt)
    cdf = np.arange(1, len(dt)+1) / len(dt)
    plt.plot(sorted_dt, cdf)
    plt.xscale("log")
    plt.xlabel("Δt (s)")
    plt.ylabel("Fraction ≤ Δt")
    plt.grid()
    plt.title(f"{name} empirical CDF of Δt")
    plt.savefig(f"./figs/{'_'.join(name.split())}_time_diffs_cdf.png", dpi=300, bbox_inches="tight")
    plt.show()


#%%
orig = df_correct_position.assign(source="orig")
noneg = df_noneg.assign(source="noneg")
both = pd.concat([orig, noneg], ignore_index=True, sort=False)

removed = orig.merge(noneg, on=["time","latitude","longitude"],
                     how="left", indicator=True)
removed = removed[removed["_merge"]=="left_only"]

plt.figure(figsize=(10,3))
removed["time"].dt.to_period("M").value_counts().sort_index().plot(kind="bar")
plt.title("Removed records per month")
plt.xlabel("month")
plt.ylabel("count removed")
plt.tight_layout()
plt.show()

plt.figure(figsize=(10,3))
removed["time"].dt.to_period("W").value_counts().sort_index().plot(kind="bar")
plt.title("Removed records per week")
plt.xlabel("month")
plt.ylabel("count removed")
plt.tight_layout()
plt.show()

#%%
for freq, label in [("M", "Month"), ("W", "Week")]:
    orig_period = df_correct_position["time"].dt.to_period(freq)
    clean_period = df_noneg["time"].dt.to_period(freq)
    orig_counts = orig_period.value_counts().sort_index().rename("All spectra")
    clean_counts = clean_period.value_counts().sort_index().rename("Non-negative spectra")
    df = pd.concat([orig_counts, clean_counts], axis=1).fillna(0)
    df["Removed spectra"] = (df["All spectra"] - df["Non-negative spectra"]) / df["All spectra"] * 100
    index_ts = df.index.to_timestamp()

    plt.figure(figsize=(10, 4))
    plt.bar(index_ts, df['Removed spectra'], width=0.8 * (index_ts[1] - index_ts[0]))
    plt.title(f'Removal Percentage per {label}')
    plt.xlabel(f'{label}')
    plt.ylabel('Removal %')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

#%%
# Plot the locations of the spectra
fig = plt.figure(figsize=(10, 8))
ax = plt.axes(projection=ccrs.PlateCarree())

ax.add_feature(cfeature.LAND)
ax.add_feature(cfeature.OCEAN)
ax.add_feature(cfeature.COASTLINE)
ax.add_feature(cfeature.BORDERS, linestyle=":", edgecolor="black")

ax.scatter(df_correct_position['longitude'], df_correct_position['latitude'],
           s=15,
           facecolor='red',
           edgecolor='blue',
           alpha=0.8,
           transform=ccrs.PlateCarree())

buffer = 0.7
min_lon, max_lon = df_correct_position.longitude.min(), df_correct_position.longitude.max()
min_lat, max_lat = df_correct_position.latitude.min(), df_correct_position.latitude.max()
ax.set_extent([min_lon-buffer, max_lon+buffer,
               min_lat-buffer, max_lat+buffer],
              crs=ccrs.PlateCarree())

ax.set_title("All spectra locations")
plt.savefig("./figs/all_rrs_locations.png", dpi=300, bbox_inches="tight")
plt.show()

#%%
fig = plt.figure(figsize=(10, 8))
ax = plt.axes(projection=ccrs.PlateCarree())

ax.add_feature(cfeature.LAND)
ax.add_feature(cfeature.OCEAN)
ax.add_feature(cfeature.COASTLINE)
ax.add_feature(cfeature.BORDERS, linestyle=":", edgecolor="black")

ax.scatter(df_noneg['longitude'], df_noneg['latitude'],
           s=15,
           facecolor='red',
           edgecolor='blue',
           alpha=0.8,
           transform=ccrs.PlateCarree())

buffer = 0.7
min_lon, max_lon = df_noneg.longitude.min(), df_noneg.longitude.max()
min_lat, max_lat = df_noneg.latitude.min(), df_noneg.latitude.max()
ax.set_extent([min_lon-buffer, max_lon+buffer,
               min_lat-buffer, max_lat+buffer],
              crs=ccrs.PlateCarree())

ax.set_title("Non-negative spectra locations")
plt.savefig("./figs/noneg_rrs_locations.png", dpi=300, bbox_inches="tight")
plt.show()

# %%

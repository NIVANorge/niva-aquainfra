#%%
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.decomposition import PCA
from sklearn.covariance import EllipticEnvelope

#%%
df = pd.read_csv(f"./cleaned_datasets/noneg_rrs.csv", sep="\t", engine="python")
print(df.head())
print(df.shape)

# Remove spikes - whole spectra where spike at the end
#%%
df['slope1'] = df['899'] - df['896']
df['slope2'] = df['896'] - df['893']
df['slope3'] = df['893'] - df['890']
print(df['slope1'].describe())
print(df['slope2'].describe())
print(df['slope3'].describe())
# remove rows where slope1, slope2, slope3 > 0.002
dropped_rows = df[(df['slope1'] > 0.002) | (df['slope2'] > 0.002) | (df['slope3'] > 0.002)]
print(dropped_rows["time"])
df = df[(df['slope1'] <= 0.002) & (df['slope2'] <= 0.002) & (df['slope3'] <= 0.002)]
df.drop(columns=['slope1', 'slope2', 'slope3'], inplace=True)
print(df.shape)

#%%
rrs_wls = df.drop(columns=[
    "time", "longitude", "latitude"], inplace=False)
rrs_wls_np = np.array(rrs_wls)

#%%
plt.figure(figsize=(12, 8))
for i in range(rrs_wls_np.shape[0]):
    plt.plot(rrs_wls_np[i])
plt.xlabel("Wavelength (nm)")
# make the x axis labels the wavelengths from rrs.columns with 15 nm intervals
plt.xticks(np.arange(0, len(rrs_wls.columns), 15), rrs_wls.columns[::15])
plt.ylabel("Rrs")
plt.title(f"Non-negative no spikes Rrs values Jun-Sep 2023")
plt.savefig(f"./figs/non-negative_no-spikes_ramses_rrs.png", dpi=300, bbox_inches='tight')
plt.show()

#%%
# Remove outliers
pca = PCA(n_components=2).fit(rrs_wls)
scores = pca.transform(rrs_wls)
envelope = EllipticEnvelope(contamination=0.01).fit(scores)
inliers = envelope.predict(scores)
df_cleaned = df[inliers == 1]
print(df_cleaned.shape)

outliers = df[inliers == -1]
print(outliers.shape)
print(outliers["time"])
#%%
outliers.loc[:, "time"] = pd.to_datetime(outliers["time"])
summary_by_day = outliers["time"].dt.floor('D').value_counts().sort_index()
print("Summary by day:")
print(summary_by_day)

summary_by_week = outliers["time"].dt.to_period('W').value_counts().sort_index()
print("\nSummary by week:")
print(summary_by_week)

summary_by_month = outliers["time"].dt.to_period('M').value_counts().sort_index()
print("\nSummary by month:")
print(summary_by_month)

outliers = outliers.sort_values(by="time")
time_diffs = outliers["time"].diff().dt.total_seconds()
close_timestamps = outliers[time_diffs <= 60]
print("Timestamps close to each other (<= 60 seconds):")
print(close_timestamps)

#%%
rrs_wls_cleaned = df_cleaned.drop(columns=[
    "time", "longitude", "latitude"], inplace=False)
rrs_wls_cleaned_np = np.array(rrs_wls_cleaned)

#%%
plt.figure(figsize=(12, 8))
for i in range(rrs_wls_cleaned_np.shape[0]):
    plt.plot(rrs_wls_cleaned_np[i])
plt.xlabel("Wavelength (nm)")
# make the x axis labels the wavelengths from rrs.columns with 15 nm intervals
plt.xticks(np.arange(0, len(rrs_wls_cleaned.columns), 15), rrs_wls_cleaned.columns[::15])
plt.ylabel("Rrs")
plt.title(f"Cleaned Rrs values Jun-Sep 2023")
plt.savefig(f"./figs/cleaned/cleaned_ramses_rrs.png", dpi=300, bbox_inches='tight')
plt.show()

############################### Resampled Rrs Data Analysis ##############################
#%%
# convert to average per minute
df_to_resample = df_cleaned.copy()
df_to_resample['time'] = pd.to_datetime(df_to_resample['time'])
resampled_rrs = df_to_resample.set_index("time", inplace=False)
resampled_rrs = resampled_rrs.resample("1min").mean().reset_index()
print(resampled_rrs.head())
print(resampled_rrs.shape)
#%%
# drop Nans
resampled_rrs.dropna(inplace=True)
print(resampled_rrs.shape)
resampled_rrs.to_csv(f"./cleaned_datasets/resampled_rrs.csv", sep="\t", index=False)

#%%
resampled_rrs_wls = resampled_rrs.drop(columns=[
    "time", "longitude", "latitude"], inplace=False)
resampled_rrs_wls_np = np.array(resampled_rrs_wls)

#%%
plt.figure(figsize=(12, 8))
for i in range(resampled_rrs_wls_np.shape[0]):
    plt.plot(resampled_rrs_wls_np[i])
plt.xlabel("Wavelength (nm)")
# make the x axis labels the wavelengths from rrs.columns with 15 nm intervals
plt.xticks(np.arange(0, len(resampled_rrs_wls.columns), 15), resampled_rrs_wls.columns[::15])
plt.ylabel("Rrs")
plt.title(f"Resampled (average per minute) cleaned Rrs values in Jun-Sep 2023")
plt.savefig(f"./figs/cleaned/resampled_cleaned_ramses_rrs.png", dpi=300, bbox_inches='tight')
plt.show()
##########################################################################################

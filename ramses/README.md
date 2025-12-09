# Notebooks for AquaINFRA Ramses data cleaning

The FA Ramses Reflectances for June-September 2023 are in FA_ramses_data folder and were obtained using [pyniva](https://github.com/NIVANorge/pyniva).

The [remove_negatives.py](remove_negatives.py) notebook discards all negative spectra and assesses the impacts. The [clean_noneg_dataset.py](clean_noneg_dataset.py) removes the spikes and outliers. Figures are saved to the [figs](figs) folder and used in the summary slides in [FA_ramses_dataset_cleaning.pptx](docs/FA_ramses_dataset_cleaning.pptx). The produced datasets are saved to the [cleaned_datasets](cleaned_datasets) folder in csv format with tab separator.

## Local dev
To install dependencies:
```
poetry install
```

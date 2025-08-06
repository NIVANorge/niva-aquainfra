import numpy as np
import pandas as pd
from sqlalchemy import types

SI_PREFIX_DICT = {
    "m": 1000.0,
    "µ": 1.0,
}

VALENCY_DICT = {
    "Ca": 2.0,
    "Mg": 2.0,
    "Na": 1.0,
    "K": 1.0,
    "NH4-N": 1.0,
    "SO4": 2.0,
    "Cl": 1.0,
    "NO3-N": 1.0,
}
MOLAR_MASS_DICT = {
    "Ca": 40.08,
    "Mg": 24.312,
    "Na": 22.9898,
    "K": 39.102,
    "NH4-N": 14.0,
    "SO4": 96.0616,
    "Cl": 35.453,
    "NO3-N": 14.0,
}


def read_data_template(file_path):
    """Read the ICPW template. An example of the template is here:
           ../data/icpw_input_template_chem_v0-3.xlsx
    Args
        file_path:  Raw str. Path to Excel template
        sheet_name: Str. Name of sheet to read
    Returns
        Dataframe.
    """
    df = pd.read_excel(
        file_path,
        sheet_name="Data",
        skiprows=1,
        header=[0, 1],
        parse_dates=[2],
        date_parser=lambda x: pd.to_datetime(x, format="%Y.%m.%d"),
    )
    df = merge_multi_header(df)

    return df


def merge_multi_header(df):
    """Merge the parameter and unit rows of the template into a single header.
    Args
        df: Raw dataframe read from the template
    Returns
        Dataframe with single, tidied header.
    """
    df.columns = [f"{i[0]}_{i[1]}" for i in df.columns]
    df.columns = [i.replace("_-", "") for i in df.columns]

    return df


def map_method_ids(df):
    """Change columns from template parameter names & units to RESA2 method IDs.

    Args
        df:  Dataframe. Read from template

    Returns
        Dataframe.
    """
    meth_map = {
        "Code": "code",
        "Date": "date",
        "pH": 10268,
        "Cond25_mS/m at 25C": 10260,
        "NH4-N_µgN/L": 10264,
        "Ca_mg/L": 10251,
        "Mg_mg/L": 10261,
        "Na_mg/L": 10263,
        "K_mg/L": 10258,
        "Alk_µeq/L": 10298,
        "SO4_mg/L": 10271,
        "NO3-N_µgN/L": 10265,
        "Cl_mg/L": 10253,
        "F_µg/L": 11121,
        "TOTP_µgP/L": 10275,
        "TOTN_µgN/L": 10274,
        "ORTP_µgP/L": 10279,
        "OKS_mgO/L": 10277,
        "SiO2_mgSiO2/L": 10270,
        "DOC_mgC/L": 10294,
        "TOC_mgC/L": 10273,
        "PERM_mgO/L": 10267,
        "TAl_µg/L": 10249,
        "RAl_µg/L": 10269,
        "ILAl_µg/L": 10257,
        "LAl_µg/L": 10292,
        "Fe_Total_µg/L": 10256,
        "Mn_Total_µg/L": 10262,
        "Cd_Total_µg/L": 10252,
        "Zn_Total_µg/L": 10276,
        "Cu_Total_µg/L": 10254,
        "Ni_Total_µg/L": 10281,
        "Pb_Total_µg/L": 10266,
        "Cr_Total_µg/L": 10285,
        "As_Total_µg/L": 10293,
        "Hg_Total_ng/L": 10921,
        "Fe_Filt_µg/L": 11122,
        "Mn_Filt_µg/L": 11123,
        "Cd_Filt_µg/L": 11124,
        "Zn_Filt_µg/L": 11125,
        "Cu_Filt_µg/L": 11126,
        "Ni_Filt_µg/L": 11127,
        "Pb_Filt_µg/L": 11128,
        "Cr_Filt_µg/L": 11129,
        "As_Filt_µg/L": 11130,
        "Hg_Filt_ng/L": 11131,
        "COLOUR_mgPt/L": 10278,
        "TURB_FTU": 10284,
        "TEMP_C": 10272,
        "RUNOFF_m3/s": 10288,
    }
    df.rename(meth_map, axis="columns", inplace=True)

    return df


def map_station_ids(df, eng):
    """Convert stations codes to RESA2 IDs.

    Args
        df:  Dataframe. Read from template
        eng: Obj. Active database connection object

    Returns
        Dataframe.
    """
    sql = "SELECT station_code AS code, station_id FROM resa2.stations"
    stn_df = pd.read_sql(sql, eng)
    df = pd.merge(df, stn_df, how="left", on="code")

    assert (
        pd.isna(df["station_id"]).sum() == 0
    ), "Some station codes cannot be matched to IDs."

    del df["code"]

    return df


def wide_to_long(df):
    """Convert from wide to long format.

    Args
        df:  Dataframe. Read from template

    Returns
        Dataframe.
    """
    df = pd.melt(df, id_vars=["station_id", "date"], var_name="method_id")
    df.dropna(subset="value", inplace=True)
    df["method_id"] = df["method_id"].astype(int)

    return df


def extract_lod_flags(df):
    """Extract LOD flags ('<' or '>') as a separate column and convert
    the value column to float.

    Args
        df:  Dataframe. Read from template

    Returns
        Dataframe.
    """

    def f(row):
        if "<" in str(row["value"]):
            val = "<"
        elif ">" in str(row["value"]):
            val = ">"
        else:
            val = np.nan
        return val

    df["flag1"] = df.apply(f, axis=1)
    df["value"] = (
        df["value"].astype(str).str.extract("([-+]?\d*\.\d+|\d+)", expand=True)
    )
    df["value"] = df["value"].astype(float)

    return df


def remove_duplicates(df, how="mean"):
    """Remove duplicated values for station_code-date combinations. Either averages
    or drops the duplicates (i.e. keeps only the first). Default is to average.

    Note that when averaging is performed, LOD flags are handled arbitrarily in cases
    where some duplicates are above the LOD and some are below.

    IMPORTANT: Duplicates should be assessed and corrected in the app first, if
    possible.

    Args
        df:  Dataframe. Read from template
        how: Str. Either 'mean' or 'drop'.

    Returns
        Dataframe.
    """
    assert how in ("mean", "drop"), "'how' must be either 'mean' or 'drop'."

    if how == "mean":
        df = (
            df.groupby(["station_id", "date", "method_id"])
            .aggregate({"value": "mean", "flag1": "first"})
            .reset_index()
        )
    else:
        # Drop
        df = df.drop_duplicates(
            subset=["station_id", "date", "method_id"], keep="first"
        )

    return df


def upload_samples(df, eng, dry_run=True):
    """Compiles a table of water samples to be added and uploads them to
    'resa2.water_samples'. All samples are assumed to be collected at the surface
    (i.e. depth1 = depth2 = 0 m).

    Args
        df:      Dataframe. Read from template
        eng:     Obj. Active database connection object
        dry_run: Bool. Default True. If True, performs most of the processing without
                 adding anything to the database.

    Returns
        Tuple of dataframes (ws_df, df). 'df' has RESA2 sample IDs appended and
        samples are added to the database.
    """
    ws_df = df[["station_id", "date"]].drop_duplicates().reset_index(drop=True)
    ws_df["depth1"] = 0
    ws_df["depth2"] = 0

    if not dry_run:
        ws_df.rename({"date": "sample_date"}, axis="columns", inplace=True)
        ws_df.to_sql(
            name="water_samples",
            schema="resa2",
            con=eng,
            if_exists="append",
            index=False,
        )

    # Get the sample IDs back from the db
    stn_ids = ws_df["station_id"].unique()
    if len(stn_ids) == 1:
        sql = (
            'SELECT water_sample_id, station_id, sample_date AS "date" '
            "FROM resa2.water_samples "
            "WHERE station_id = %s" % stn_ids[0]
        )
    else:
        stn_ids = str(tuple(stn_ids))
        sql = (
            'SELECT water_sample_id, station_id, sample_date AS "date" '
            "FROM resa2.water_samples "
            "WHERE station_id IN %s" % stn_ids
        )
    ws_df = pd.read_sql_query(sql, eng)

    # Add IDs to df
    df = pd.merge(
        df,
        ws_df,
        how="left",
        on=["station_id", "date"],
    )

    ws_df = (
        df[["water_sample_id", "station_id", "date"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    if not dry_run:
        assert pd.isna(df["water_sample_id"]).sum() == 0

    return (ws_df, df)


def upload_chemistry(df, eng, dry_run=True):
    """Upload chemistry data.

    Args
        df:      Dataframe. Read from template
        eng:     Obj. Active database connection object
        dry_run: Bool. Default True. If True, performs most of the processing without
                 adding anything to the database.

    Returns
        Dataframe. Data are uploaded to RESA2.
    """
    if not dry_run:
        assert pd.isna(df["method_id"]).sum() == 0
        assert pd.isna(df["water_sample_id"]).sum() == 0
        assert pd.isna(df["value"]).sum() == 0

    del df["station_id"], df["date"]

    # Improve performance by explicitly setting dtypes. See
    # https://stackoverflow.com/a/42769557/505698
    dtypes = {
        c: types.VARCHAR(df[c].str.len().max())
        for c in df.columns[df.dtypes == "object"].tolist()
    }

    if not dry_run:
        df.rename({"water_sample_id": "sample_id"}, axis="columns", inplace=True)
        df.to_sql(
            name="water_chemistry_values2",
            schema="resa2",
            con=eng,
            if_exists="append",
            index=False,
            dtype=dtypes,
        )

    return df


def process_template(xl_path, eng, dups="mean", dry_run=True):
    """Main function for processing the ICPW data template.

    NOTE: Be sure to check the template using the ICPW QC app and fix any
    issues hightlighted before using this function.

    Args
        xl_path: Str. Path to template
        eng:     Obj. Active database connection object
        dups:    Str. How to handle duplicates. Either 'mean' or 'drop'. See docsring
                 of 'remove_duplicates' for details
        dry_run: Bool. Default True. If True, performs most of the processing without
                 adding anything to the database.

    Returns
        Tuple of dataframes (water_samples, chem_values).
    """
    df = read_data_template(xl_path)
    del df["Name"]
    df = map_method_ids(df)
    df = map_station_ids(df, eng)
    df = wide_to_long(df)
    df = extract_lod_flags(df)
    df = remove_duplicates(df, how=dups)
    ws_df, df = upload_samples(df, eng, dry_run=dry_run)
    df = upload_chemistry(df, eng, dry_run=dry_run)

    return (ws_df, df)


def convert_to_microequivalents(df, col):
    """Basic conversion from mass/l to microequivalents/l.

    Args
        df:  Dataframe
        col: Str. Column in 'df' named 'par_unit'

    Returns
        A new column is added to 'df' with values in ueq/l.
    """
    if col in df.columns:
        # Separate par and unit
        parts = col.split("_")
        par = "_".join(parts[:-1])
        unit = parts[-1]

        # Determine unit factor
        if unit[0] not in SI_PREFIX_DICT.keys():
            raise ValueError("Unit factor could not be identified.")
        else:
            factor = SI_PREFIX_DICT[unit[0]]

        df[f"{par}_µeq/l"] = df[col] * VALENCY_DICT[par] * factor / MOLAR_MASS_DICT[par]

    return df


def calculate_anc(df, anc_oaa=False):
    """Calculate ANC (and ANCoaa, if desired). NaN values for NH4 and K are
    filled with zeros, but all other parameters must be present in df for the
    calculation to run.

    Args
        df:      Dataframe
        anc_oaa: Bool. Default False. Whether to calculate ANCoaa in addition
                 to ANC

    Returns
        New column(s) added to 'df'.
    """
    for par in ["NH4-N_µeq/l", "K_µeq/l"]:
        if par in df.columns:
            df[par + "_temp"] = df[par].fillna(0)
        else:
            df[par + "_temp"] = 0

    df["ANC_µeq/l"] = (
        df["Ca_µeq/l"]
        + df["Mg_µeq/l"]
        + df["Na_µeq/l"]
        + df["K_µeq/l_temp"]
        + df["NH4-N_µeq/l_temp"]
        - df["Cl_µeq/l"]
        - df["SO4_µeq/l"]
        - df["NO3-N_µeq/l"]
    )

    del df["NH4-N_µeq/l_temp"], df["K_µeq/l_temp"]

    if anc_oaa:
        df["ANCoaa_µeq/l"] = df["ANC_µeq/l"] - 3.4 * df["TOC_mg C/l"]

    return df


def calculate_organic_anions(df, site_density, pK1=3.04, pK2=4.42, pK3=6.70):
    """Estimate organic anions from pH and TOC using the model of Hruška et al.
    (2003) https://doi.org/10.1021/es0201552.

    Args
        df:           Dataframe. Must contain columns named 'pH_' and 'TOC_mg C/l'
        site_density: Float. The number of acidic functional groups
                      per milligram of organic carbon. Often assumed to be 10.2,
                      but may vary considerably. See
                      https://github.com/JamesSample/icpw2/issues/3
        pK1:          Float. Deafult 3.04. Dissociation Constant (pKa) for a
                      triprotic model of organic acid dissociation
        pK2:          Float. Deafult 4.42. Dissociation Constant (pKa) for a
                      triprotic model of organic acid dissociation
        pK3:          Float. Deafult 6.70. Dissociation Constant (pKa) for a
                      triprotic model of organic acid dissociation

    Returns
        Dataframe. Column 'OrgAnions_µeq/l' is added to 'df'.
    """
    K1, K2, K3 = 10**-pK1, 10**-pK2, 10**-pK3

    df["H+"] = 10 ** -df["pH_"]
    df["H3A"] = df["H+"] ** 3 / (
        df["H+"] ** 3 + K1 * df["H+"] ** 2 + K1 * K2 * df["H+"] + K1 * K2 * K3
    )
    df["H2A-"] = K1 * df["H3A"] / df["H+"]
    df["HA2-"] = K1 * K2 * df["H3A"] / (df["H+"] ** 2)
    df["A3-"] = K1 * K2 * K3 * df["H3A"] / (df["H+"] ** 3)
    df["Hruska_Factor"] = (
        site_density * (df["H2A-"] + 2 * df["HA2-"] + 3 * df["A3-"]) / 3
    )
    df["OrgAnions_µeq/l"] = df["Hruska_Factor"] * df["TOC_mg C/l"]

    df.drop(
        ["H+", "H3A", "H2A-", "HA2-", "A3-", "Hruska_Factor"],
        axis="columns",
        inplace=True,
    )

    return df


def calculate_bicarbonate(df):
    """Calculate bicarbonate using the ion balance method:

           [HCO3] = ANC - OrgAnions + H.

    If the [HCO3] < 0 it is set to 0.
    """
    df["HCO3_µeq/l"] = df["ANC_µeq/l"] + df["H_µeq/l"] - df["OrgAnions_µeq/l"]
    df["HCO3_µeq/l"] = np.where(df["HCO3_µeq/l"] < 0, 0, df["HCO3_µeq/l"])

    return df


def double_mad_from_median(data, thresh=3.5):
    """Simple test for outliers in 1D data. Based on the standard MAD approach, but
    modified slightly to allow for skewed datasets. See the example in R here:
    http://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/
    (especially the section "Unsymmetric Distributions and the Double MAD". The
    Python code is based on this post
    
        https://stackoverflow.com/a/29222992/505698
    
    See also here
    
        https://stackoverflow.com/a/22357811/505698
    
    Args
        data:   Array-like. 1D array of values.
        thresh: Float. Default 3.5. Larger values detect fewer outliers. See the
                section entitled "Z-Scores and Modified Z-Scores" here
                https://www.itl.nist.gov/div898/handbook/eda/section3/eda35h.htm
    Returns
        Array of Bools where ones indicate outliers.
    """
    m = np.nanmedian(data)
    abs_dev = np.abs(data - m)
    left_mad = np.median(abs_dev[data <= m])
    right_mad = np.median(abs_dev[data >= m])
    if (left_mad == 0) or (right_mad == 0):
        # Don't identify any outliers. Not strictly correct - see links above!
        return np.zeros_like(data, dtype=bool)

    data_mad = left_mad * np.ones(len(data))
    data_mad[data > m] = right_mad
    modified_z_score = 0.6745 * abs_dev / data_mad
    modified_z_score[data == m] = 0

    return modified_z_score > thresh

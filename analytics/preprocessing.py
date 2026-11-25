import h5py
from pathlib import Path
import json

import numpy as np
import pandas as pd

# --------------------------------------------------
# PROJECT PATHS
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DIR = PROJECT_ROOT / "datasets" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "datasets" / "processed"
REPORT_DIR = PROJECT_ROOT / "analytics" / "reports"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

DATASETS = {
    "METR-LA": RAW_DIR / "metr-la.h5",
    "PEMS-BAY": RAW_DIR / "pems-bay.h5",
}


# --------------------------------------------------
# LOAD DATASET
# --------------------------------------------------


def load_dataset(dataset_name, file_path):

    print(f"\nLoading {dataset_name}...")

    # Read HDF5 directly using h5py.
    with h5py.File(file_path, "r") as hdf:

        # METR-LA and PEMS-BAY use different HDF5 groups.
        group_name = "df" if dataset_name == "METR-LA" else "speed"

        group = hdf[group_name]

        # Extract sensor IDs.
        sensor_ids = group["axis0"][:]

        # Decode byte-based sensor IDs (METR-LA).
        columns = [
            sensor.decode("utf-8") if isinstance(sensor, bytes)
            else sensor
            for sensor in sensor_ids
        ]

        # Extract timestamps stored as nanoseconds.
        timestamps = group["axis1"][:]

        # Extract traffic speed measurements.
        values = group["block0_values"][:]

    # Convert timestamps into UTC datetime values.
    timestamps = pd.to_datetime(
        timestamps,
        unit="ns",
        utc=True
    )

    # Create the DataFrame.
    df = pd.DataFrame(
        values,
        index=timestamps,
        columns=columns
    )

    print("Dataset loaded:", df.shape)
    print("First timestamp:", df.index[0])
    print("Last timestamp:", df.index[-1])

    return df


# --------------------------------------------------
# DATA QUALITY ASSESSMENT
# --------------------------------------------------

def generate_quality_report(df, dataset_name):

    total = int(df.size)

    missing = int(df.isna().sum().sum())
    infinite = int(np.isinf(df.to_numpy()).sum())
    zero = int(df.eq(0).sum().sum())
    negative = int(df.lt(0).sum().sum())

    all_zero_timestamps = int(df.eq(0).all(axis=1).sum())

    report = {
        "dataset": dataset_name,
        "rows": int(df.shape[0]),
        "sensors": int(df.shape[1]),
        "total_measurements": total,
        "missing_values": missing,
        "infinite_values": infinite,
        "zero_readings": zero,
        "negative_readings": negative,
        "all_sensor_zero_timestamps": all_zero_timestamps,
        "zero_percentage": round(zero / total * 100, 4),
        "start_timestamp": str(df.index.min()),
        "end_timestamp": str(df.index.max()),
    }

    return report


# --------------------------------------------------
# FLAG SUSPICIOUS READINGS
# --------------------------------------------------

def flag_invalid_readings(df):

    # Preserve the original values in a separate DataFrame.
    invalid_mask = (
        df.isna()
        | np.isinf(df)
        | df.lt(0)
    )

    # Negative and infinite values are invalid.
    cleaned = df.replace([np.inf, -np.inf], np.nan)
    cleaned = cleaned.mask(cleaned < 0)

    # Zero readings are preserved.
    # They are not automatically treated as missing traffic.

    return cleaned, invalid_mask


# --------------------------------------------------
# FLAG NETWORK-WIDE ZERO PERIODS
# --------------------------------------------------

def detect_network_outages(df):

    all_zero = df.eq(0).all(axis=1)

    groups = all_zero.ne(all_zero.shift()).cumsum()

    outages = []

    for _, streak in all_zero.groupby(groups):

        if bool(streak.iloc[0]):

            intervals = len(streak)

            outages.append({
                "start": str(streak.index[0]),
                "end": str(streak.index[-1]),
                "intervals": intervals,
                "elapsed_minutes": (intervals - 1) * 5,
                "slot_minutes": intervals * 5,
            })

    return outages


# --------------------------------------------------
# SAVE PROCESSED DATA
# --------------------------------------------------

def save_processed_data(df, dataset_name):

    filename = dataset_name.lower().replace("-", "_") + "_cleaned.csv"

    output_path = PROCESSED_DIR / filename

    df.to_csv(output_path, index_label="timestamp")

    print("Processed dataset saved:", output_path)


# --------------------------------------------------
# SAVE QUALITY REPORT
# --------------------------------------------------

def save_quality_report(report, outages, dataset_name):

    output = {
        "quality_report": report,
        "network_wide_zero_periods": outages,
        "cleaning_policy": {
            "missing_values": "Preserved as missing",
            "infinite_values": "Converted to missing",
            "negative_values": "Converted to missing",
            "zero_values": "Preserved and flagged for investigation",
        }
    }

    filename = dataset_name.lower().replace("-", "_") + "_quality.json"

    output_path = REPORT_DIR / filename

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(output, file, indent=4)

    print("Quality report saved:", output_path)


# --------------------------------------------------
# MAIN PREPROCESSING PIPELINE
# --------------------------------------------------

def process_dataset(dataset_name, file_path):

    print("\n" + "=" * 60)
    print("PREPROCESSING:", dataset_name)
    print("=" * 60)

    df = load_dataset(dataset_name, file_path)

    # Step 1: Original quality report.
    report = generate_quality_report(df, dataset_name)

    print("\nOriginal Data Quality:")
    for key, value in report.items():
        print(f"{key}: {value}")

    # Step 2: Detect network-wide zero periods.
    outages = detect_network_outages(df)

    print("\nNetwork-wide zero periods:", len(outages))

    # Step 3: Flag invalid readings and clean.
    cleaned_df, invalid_mask = flag_invalid_readings(df)

    invalid_count = int(invalid_mask.sum().sum())

    print("Invalid readings flagged:", invalid_count)

    # Step 4: Save outputs.
    save_processed_data(cleaned_df, dataset_name)

    save_quality_report(report, outages, dataset_name)

    print("\nCompleted:", dataset_name)


# --------------------------------------------------
# EXECUTION
# --------------------------------------------------

if __name__ == "__main__":

    for dataset_name, file_path in DATASETS.items():

        if not file_path.exists():
            print(f"\nDataset not found: {file_path}")
            continue

        process_dataset(dataset_name, file_path)

    print("\n" + "=" * 60)
    print("PREPROCESSING PIPELINE COMPLETED")
    print("=" * 60)


from pathlib import Path

import h5py
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "datasets" / "raw"

DATASET_CONFIG = {
    "metr-la": {"file": "metr-la.h5", "group": "df"},
    "pems-bay": {"file": "pems-bay.h5", "group": "speed"},
}


def load_traffic_dataset(dataset_name):

    if dataset_name not in DATASET_CONFIG:
        raise ValueError("Use 'metr-la' or 'pems-bay'.")

    config = DATASET_CONFIG[dataset_name]
    file_path = RAW_DATA_DIR / config["file"]

    if not file_path.exists():
        raise FileNotFoundError(file_path)

    print(f"\nReading file: {file_path.name}")

    with h5py.File(file_path, "r") as file:
        group = file[config["group"]]

        sensor_ids = group["axis0"][:]
        timestamps = group["axis1"][:]
        values = group["block0_values"][:]

    sensor_ids = [
        value.decode("utf-8")
        if isinstance(value, bytes)
        else str(value)
        for value in sensor_ids
    ]

    index = pd.to_datetime(timestamps, unit="ns", utc=True)

    df = pd.DataFrame(
        values,
        index=index,
        columns=sensor_ids
    )

    df.index.name = "timestamp"

    return df


def inspect_data_quality(df, dataset_name):

    values = df.to_numpy(dtype=float)
    finite = values[np.isfinite(values)]

    print("\n" + "=" * 60)
    print(f"DATA QUALITY: {dataset_name.upper()}")
    print("=" * 60)

    print("Shape:", df.shape)
    print("Total measurements:", df.size)
    print("Missing values:", int(np.isnan(values).sum()))
    print("Infinite values:", int(np.isinf(values).sum()))
    print("Zero readings:", int((values == 0).sum()))
    print("Negative readings:", int((values < 0).sum()))

    if finite.size:
        print("Minimum speed:", round(float(finite.min()), 2))
        print("Maximum speed:", round(float(finite.max()), 2))
        print("Average speed:", round(float(finite.mean()), 2))

    print("Start:", df.index.min())
    print("End:", df.index.max())



def calculate_zero_streaks(df):

    results = []

    for sensor in df.columns:

        zero_mask = df[sensor].eq(0)

        # Create a new group whenever zero/non-zero status changes.
        groups = zero_mask.ne(zero_mask.shift()).cumsum()

        # Calculate streak lengths for zero-only groups.
        streaks = zero_mask.groupby(groups).sum()

        zero_streaks = streaks[streaks.index.isin(
            groups[zero_mask].unique()
        )]

        results.append({
            "sensor_id": sensor,
            "total_zeros": int(zero_mask.sum()),
            "max_consecutive_zeros": int(zero_streaks.max())
                if not zero_streaks.empty else 0,
            "zero_percentage": round(
                zero_mask.mean() * 100, 2
            )
        })

    return pd.DataFrame(results).sort_values(
        "max_consecutive_zeros",
        ascending=False
    )




def find_dataset_wide_zero_periods(df):

    # Identify timestamps where every sensor has zero speed.
    all_zero = df.eq(0).all(axis=1)

    # Create a new group whenever the True/False value changes.
    groups = all_zero.ne(all_zero.shift()).cumsum()

    periods = []

    for _, streak in all_zero.groupby(groups):

        if bool(streak.iloc[0]):

            start = streak.index[0]
            end = streak.index[-1]
            intervals = len(streak)

            periods.append({
                "start": start,
                "end": end,
                "intervals": intervals,
                "duration_minutes": intervals * 5
            })

    return pd.DataFrame(periods)


def investigate_zero_readings(df, dataset_name):

    print("\n" + "=" * 60)
    print(f"ZERO READING INVESTIGATION: {dataset_name.upper()}")
    print("=" * 60)

    zero_mask = df.eq(0)
    timestamp_counts = zero_mask.sum(axis=1)

    # 1. Overall statistics
    total_zeros = int(zero_mask.sum().sum())

    print("\n1. OVERALL ZERO STATISTICS")
    print("-" * 60)
    print("Total zero readings:", total_zeros)
    print(
        "Zero percentage:",
        round(total_zeros / df.size * 100, 4), "%"
    )

    # 2. Sensor statistics
    print("\n2. TOP 10 SENSORS WITH MOST ZERO READINGS")
    print("-" * 60)

    sensor_counts = zero_mask.sum().sort_values(
        ascending=False
    )

    sensor_report = pd.DataFrame({
        "zero_count": sensor_counts,
        "zero_percentage": sensor_counts / len(df) * 100
    })

    print(sensor_report.head(10).round(2).to_string())

    # 3. Timestamp statistics
    print("\n3. TOP 10 TIMESTAMPS WITH MOST ZERO READINGS")
    print("-" * 60)

    affected = timestamp_counts[
        timestamp_counts > 0
    ].sort_values(ascending=False)

    print(affected.head(10).to_string())

    # 4. Simultaneous readings
    print("\n4. SIMULTANEOUS ZERO READINGS")
    print("-" * 60)

    print(
        "All sensors zero:",
        int((timestamp_counts == len(df.columns)).sum())
    )

    print(
        "More than half sensors zero:",
        int((timestamp_counts > len(df.columns) / 2).sum())
    )

    print(
        "At least one sensor zero:",
        int((timestamp_counts > 0).sum())
    )

    # 5. Consecutive zero streaks per sensor
    print("\n5. LONGEST CONSECUTIVE ZERO STREAKS")
    print("-" * 60)

    streak_report = calculate_zero_streaks(df)

    print(streak_report.head(10).to_string(index=False))

    # 6. Dataset-wide zero periods
    print("\n6. DATASET-WIDE ZERO PERIODS")
    print("-" * 60)

    periods = find_dataset_wide_zero_periods(df)

    if periods.empty:
        print("No dataset-wide zero periods found.")
    else:
        print("Number of periods:", len(periods))
        print(periods.to_string(index=False))

    print("\nInvestigation completed.")


if __name__ == "__main__":

    for dataset_name in ["metr-la", "pems-bay"]:

        print("\n\nPROCESSING:", dataset_name.upper())

        try:
            df = load_traffic_dataset(dataset_name)

            print("Dataset loaded:", df.shape)

            inspect_data_quality(df, dataset_name)

            investigate_zero_readings(df, dataset_name)

        except Exception as error:
            print("Error:", error)

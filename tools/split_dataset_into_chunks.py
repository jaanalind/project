"""
This script was run once to create weekly taxi trip csv files
"""

import polars as pl
from pathlib import Path
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description='Split taxi trip data into weekly chunks')
    parser.add_argument('input_csv', type=str, help='Path to the input CSV file')
    return parser.parse_args()

def main():
    args = parse_args()
    INPUT_CSV = args.input_csv
    OUTPUT_DIR = Path.cwd() / "chunks"
    TIMESTAMP_COLUMN = "Trip Start Timestamp"

    csv_schema = {
        "Trip ID": pl.Utf8,
        "Taxi ID": pl.Utf8,
        "Trip Start Timestamp": pl.Utf8,
        "Trip End Timestamp": pl.Utf8,
        "Trip Seconds": pl.Int64,
        "Trip Miles": pl.Float64,
        "Pickup Census Tract": pl.Utf8,
        "Dropoff Census Tract": pl.Utf8,
        "Pickup Community Area": pl.Utf8,
        "Dropoff Community Area": pl.Utf8,
        "Fare": pl.Float64,
        "Tips": pl.Float64,
        "Tolls": pl.Float64,
        "Extras": pl.Float64,
        "Trip Total": pl.Float64,
        "Payment Type": pl.Utf8,
        "Company": pl.Utf8,
        "Pickup Centroid Latitude": pl.Float64,
        "Pickup Centroid Longitude": pl.Float64,
        "Pickup Centroid Location": pl.Utf8,
        "Dropoff Centroid Latitude": pl.Float64,
        "Dropoff Centroid Longitude": pl.Float64,
        "Dropoff Centroid Location": pl.Utf8
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    datetime_format = "%m/%d/%Y %I:%M:%S %p"

    df = pl.read_csv(
        INPUT_CSV,
        schema=csv_schema,
        try_parse_dates=False  # We'll parse dates separately since we have a specific format
    )

    df = df.with_columns([
        pl.col(TIMESTAMP_COLUMN)
        .str.strptime(pl.Datetime, format=datetime_format)
        .dt.truncate("1w")
        .alias("week_start")
    ])

    for week_start in df.get_column("week_start").unique().sort():
        week_str = week_start.strftime("%Y_%m_%d")
        week_df = df.filter(pl.col("week_start") == week_start)

        output_file = OUTPUT_DIR / f"week_{week_str}.csv"

        week_df.drop("week_start").write_csv(output_file)
        print(f"Saved {output_file} with {week_df.height} records")

if __name__ == "__main__":
    main()
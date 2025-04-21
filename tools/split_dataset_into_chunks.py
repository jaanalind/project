import pandas as pd
from pathlib import Path
import numpy as np
# Config
INPUT_CSV = "/home/kaspar/PycharmProjects/project/Taxi_Trips_-_2024_20240408.csv"  # Change if needed
OUTPUT_DIR = Path("/home/kaspar/PycharmProjects/project/chunks")
TIMESTAMP_COLUMN = "Trip Start Timestamp"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

datetime_format = "%m/%d/%Y %I:%M:%S %p"
df = pd.read_csv(INPUT_CSV, parse_dates=[TIMESTAMP_COLUMN], date_format=datetime_format)

# Floor timestamps to the week (Monday as start of week)
df['week_start'] = df[TIMESTAMP_COLUMN].dt.to_period('W').apply(lambda r: r.start_time)

# Group by week and export each as a separate file
for week_start, group in df.groupby('week_start'):
    week_str = week_start.strftime("%Y_%m_%d")
    output_file = OUTPUT_DIR / f"week_{week_str}.csv"
    group.drop(columns=['week_start']).to_csv(output_file, index=False)
    print(f"Saved {output_file} with {len(group)} records")

import datetime
import shutil

import polars as pl
from pathlib import Path
from airflow import DAG
from airflow.decorators import task
from logging import getLogger

logger = getLogger(__name__)
with DAG(
        dag_id="process_taxi_rides",
        start_date=datetime.datetime(2021, 1, 1),
        schedule="@weekly",
):
    @task()
    def extract_raw_taxi_rides(**context):
        # Save raw CSV exactly as is
        data_interval_start = context['data_interval_start']
        file_date = data_interval_start.strftime("%Y_%m_%d")
        chunks_dir = Path("chunks")
        raw_dir = Path("raw_data")
        raw_dir.mkdir(exist_ok=True)

        source_file = chunks_dir / f"week_{file_date}.csv"
        raw_file = raw_dir / f"week_{file_date}.csv"

        shutil.copy2(source_file, raw_file)
        logger.info(f"Saved raw data: {raw_file}")




    extract_task = extract_raw_taxi_rides()


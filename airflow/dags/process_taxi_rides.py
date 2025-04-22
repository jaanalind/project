import datetime
from airflow import DAG
from airflow.decorators import task


# Database connection string - should be in environment variables or Airflow connections
DB_CONNECTION = "postgresql://admin:admin@postgres/postgres"

with DAG(
        dag_id="process_taxi_rides",
        start_date=datetime.datetime(2024, 1, 1),
        schedule="0 0 * * 1",
):
    @task()
    def extract_raw_taxi_rides(**context):
        import shutil
        from pathlib import Path
        from logging import getLogger

        logger = getLogger(__name__)
        # Save raw CSV exactly as is
        data_interval_start = context['data_interval_start']
        file_date = data_interval_start.strftime("%Y_%m_%d")
        chunks_dir = Path("/opt/airflow/chunks")
        raw_dir = Path("/opt/airflow/raw_data")
        raw_dir.mkdir(parents=True, exist_ok=True)
        source_file = chunks_dir / f"week_{file_date}.csv"
        raw_file = raw_dir / f"week_{file_date}.csv"

        shutil.copy2(source_file, raw_file)
        logger.info(f"Saved raw data: {raw_file}")

        return str(raw_file)
        
    @task()
    def validate_and_load_taxi_rides(raw_file: str):
        from schemas.taxi_schema import TaxiRideSchema
        from logging import getLogger

        logger = getLogger(__name__)
        import polars as pl

        # Read CSV with Polars
        df = pl.read_csv(raw_file)
        df = df.select([
            pl.col(col).alias(col.lower().replace(' ', '_'))
            for col in df.columns
        ])

        try:
            # Validate with Pandera directly on Polars DataFrame
            validated_df = TaxiRideSchema.validate(df, lazy=True)
            logger.info(f"Data validation successful for {raw_file}")


        except Exception as e:
            logger.error(f"Error validating file {raw_file}: {str(e)}")
            raise

        try:
            # Write directly to database using Polars
            rows_written = validated_df.write_database(
                table_name="taxi_rides",
                connection=DB_CONNECTION,
                if_table_exists="append",
                engine="sqlalchemy"
            )

            logger.info(f"Successfully loaded {rows_written} rows to database")
            return rows_written

        except Exception as e:
            logger.error(f"Error loading data to database: {str(e)}")
            raise


    extract_task = extract_raw_taxi_rides()
    validate_and_load_task = validate_and_load_taxi_rides(extract_task)

    # Set task dependencies
    extract_task >> validate_and_load_task

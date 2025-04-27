from airflow import DAG
from airflow.decorators import task
import datetime

DB_CONNECTION = "postgresql://admin:admin@app-postgres/postgres"
PATH_TO_DBT_PROJECT = "/opt/airflow/taxi_trip_processor_dbt"
PATH_TO_DBT_VENV = "/opt/airflow/dbt_venv/bin/activate"

with DAG(
        dag_id="process_taxi_rides",
        start_date=datetime.datetime(2024, 1, 1),
        is_paused_upon_creation=False,
        schedule="0 0 * * 1",
):
    @task()
    def ensure_weekly_partitions(**context):
        """
        Creates partition for following week
        """
        from sqlalchemy import create_engine, text
        from logging import getLogger

        logger = getLogger(__name__)
        engine = create_engine(DB_CONNECTION)

        start_date = context['data_interval_start']

        end_date = context['data_interval_end']
        partition_name = f"taxi_rides_{start_date.strftime('%Y_%m_%d')}"

        logger.info(f"Creating partition from {start_date} to {end_date}")

        with engine.begin() as conn:
            check_sql = f"""
                                SELECT EXISTS (
                                    SELECT FROM pg_tables
                                    WHERE schemaname = 'public'
                                    AND tablename = '{partition_name}'
                                );
                            """
            if not conn.execute(check_sql).fetchone()[0]:
                create_partition_sql = text(f"""
                        CREATE TABLE {partition_name} PARTITION OF taxi_rides
                        FOR VALUES FROM ('{start_date.isoformat()}') TO ('{end_date.isoformat()}')
                    """)
                conn.execute(create_partition_sql)
                logger.info(f"Created partition {partition_name}")

        return partition_name


    @task()
    def extract_raw_taxi_rides(**context):
        """
        Imitates reading files from outside source and saving them locally
        """
        import shutil
        from pathlib import Path
        from logging import getLogger

        logger = getLogger(__name__)
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
    def validate_and_load_taxi_rides(raw_file: str, **context):
        from schemas.taxi_schema import TaxiRideSchema
        from logging import getLogger
        from sqlalchemy import create_engine, text
        import datetime
        import polars as pl

        logger = getLogger(__name__)

        data_interval_start = context['data_interval_start']
        file_date = data_interval_start.strftime("%Y_%m_%d")
        file_name = f"week_{file_date}.csv"

        engine = create_engine(DB_CONNECTION)
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM processed_files WHERE file_name = :file_name"),
                {"file_name": file_name}
            ).fetchone()

            if result:
                logger.info(f"File {file_name} has already been processed, skipping")
                return

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
        df = pl.read_csv(raw_file, schema=csv_schema)

        logger.info(f"Loaded raw data with {df.height} rows and {df.width} columns")

        column_mapping = {
            "Trip ID": "trip_id",
            "Taxi ID": "taxi_id",
            "Trip Start Timestamp": "trip_start_timestamp",
            "Trip End Timestamp": "trip_end_timestamp",
            "Trip Seconds": "trip_seconds",
            "Trip Miles": "trip_miles",
            "Pickup Census Tract": "pickup_census_tract",
            "Dropoff Census Tract": "dropoff_census_tract",
            "Pickup Community Area": "pickup_community_area",
            "Dropoff Community Area": "dropoff_community_area",
            "Fare": "fare",
            "Tips": "tips",
            "Tolls": "tolls",
            "Extras": "extras",
            "Trip Total": "trip_total",
            "Payment Type": "payment_type",
            "Company": "company",
            "Pickup Centroid Latitude": "pickup_centroid_latitude",
            "Pickup Centroid Longitude": "pickup_centroid_longitude",
            "Pickup Centroid Location": "pickup_centroid_location",
            "Dropoff Centroid Latitude": "dropoff_centroid_latitude",
            "Dropoff Centroid Longitude": "dropoff_centroid_longitude",
            "Dropoff Centroid Location": "dropoff_centroid_location"
        }
        df = df.rename(column_mapping)

        with engine.begin() as conn:
            validated_df = TaxiRideSchema.validate(df, lazy=True)
            logger.info(f"Data validation successful for {raw_file}")

            validated_df.write_database(
                table_name="taxi_rides",
                connection=DB_CONNECTION,
                if_table_exists="append",
                engine="sqlalchemy"
            )

            conn.execute(
                text("""
                     INSERT INTO processed_files (file_name, processed_at)
                     VALUES (:file_name, :processed_at)
                     """),
                {
                    "file_name": file_name,
                    "processed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
                }
            )


    @task.bash(
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV}, 
        cwd=PATH_TO_DBT_PROJECT,
        pool="default_pool",
        pool_slots=128
    )
    def dbt_taxi_driver_swap_model() -> str:
        return "source $PATH_TO_DBT_VENV && dbt run --profiles-dir=/opt/airflow/taxi_trip_processor_dbt/.dbt"


    ensure_partitions = ensure_weekly_partitions()
    extract_task = extract_raw_taxi_rides()
    validate_and_load_task = validate_and_load_taxi_rides(extract_task)
    dbt_taxi_driver_swap_model_task = dbt_taxi_driver_swap_model()

    ensure_partitions >> extract_task >> validate_and_load_task >> dbt_taxi_driver_swap_model_task

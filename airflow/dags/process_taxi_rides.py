import datetime
from airflow import DAG
from airflow.decorators import task

# Database connection string - should be in environment variables or Airflow connections
DB_CONNECTION = "postgresql://admin:admin@app-postgres/postgres"

with DAG(
        dag_id="process_taxi_rides",
        start_date=datetime.datetime(2024, 1, 1),
        schedule="0 0 * * 1",
):
    @task()
    def ensure_weekly_partitions(weeks_ahead: int = 12, **context):
        from sqlalchemy import create_engine, text
        from datetime import datetime, timedelta
        from logging import getLogger

        logger = getLogger(__name__)
        engine = create_engine(DB_CONNECTION)

        # Get the data interval start from the context
        data_interval_start = context['data_interval_start']
        # Calculate date range - start from the week of data being processed
        start_date = data_interval_start - timedelta(days=data_interval_start.weekday())
        end_date = start_date + timedelta(weeks=weeks_ahead)

        logger.info(f"Ensuring partitions exist from {start_date} to {end_date}")

        with engine.connect() as conn:
            current = start_date
            partitions_created = 0

            while current < end_date:
                next_week = current + timedelta(weeks=1)
                partition_name = f"taxi_rides_{current.strftime('%Y_%m_%d')}"

                # Check if partition exists
                exists = conn.execute(text(
                    "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = :name)"
                ), {"name": partition_name.lower()}).scalar()

                if not exists:
                    # Create weekly partition
                    create_partition_sql = text("""
                        CREATE TABLE IF NOT EXISTS :partition_name 
                        PARTITION OF taxi_rides 
                        FOR VALUES FROM (:start_date) TO (:end_date)
                    """).bindparams(
                        partition_name=partition_name,
                        start_date=current,
                        end_date=next_week
                    )
                    conn.execute(create_partition_sql)
                    partitions_created += 1
                    logger.info(f"Created partition {partition_name}")

                current = next_week

            # Ensure future partition exists
            future_partition_sql = text("""
                CREATE TABLE IF NOT EXISTS taxi_rides_future 
                PARTITION OF taxi_rides 
                FOR VALUES FROM (:end_date) TO (MAXVALUE)
            """).bindparams(end_date=end_date)
            
            conn.execute(future_partition_sql)

            logger.info(f"Created {partitions_created} new partitions")
            return partitions_created


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
        # Read CSV with Polars
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
        try:
            # Validate with Pandera directly on Polars DataFrame
            validated_df = TaxiRideSchema.validate(df, lazy=True)
            logger.info(f"Data validation successful for {raw_file}")


        except Exception as e:
            logger.error(f"Error validating file {raw_file}: {str(e)}")
            raise

  #      # Count validation errors
  #      error_counts = defaultdict(int)
  #      try:
  #          TaxiRideSchema.validate(df, lazy=True)
  #      except pa.errors.SchemaErrors as e:
  #          error_data = e.message
  #          print(error_data)
  #          for error in error_data["DATA"]["DATAFRAME_CHECK"]:
  #              error_key = f"{error['column']}: {error['check']}"
  #              error_counts[error_key] += 1
#
  #          # Log error summary
  #      if error_counts:
  #          logger.info("Validation error summary:")
  #          for error_type, count in error_counts.items():
  #              logger.info(f"- {error_type}: {count} occurrences")

        logger.info(len(validated_df))
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

    ensure_partitions = ensure_weekly_partitions()
    extract_task = extract_raw_taxi_rides()
    validate_and_load_task = validate_and_load_taxi_rides(extract_task)

    # Set task dependencies
    ensure_partitions >> extract_task >> validate_and_load_task

FROM apache/airflow:2.10.5-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.9.4 \
        dbt-postgres==1.9.0 \
        psycopg2-binary==2.9.10

COPY airflow/library /opt/airflow/library

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/library"

COPY --chown=airflow:airflow taxi_trip_processor_dbt /opt/airflow/taxi_trip_processor_dbt

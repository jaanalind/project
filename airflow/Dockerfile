FROM apache/airflow:2.10.5-python3.12

USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy library code
COPY library /opt/airflow/library

# Add library to Python path
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/library" 
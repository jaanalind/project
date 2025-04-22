# Docker Setup for Airflow

This directory contains the Docker configuration for running Apache Airflow with PostgreSQL.

## Directory Structure

```
devops/docker/
├── docker-compose.yml    # Main Docker Compose configuration
└── README.md            # This file
```

## Prerequisites

- Docker
- Docker Compose

## Usage

1. Start the services:
```bash
docker-compose up -d
```

2. Access Airflow UI:
- URL: http://localhost:8080
- Username: admin
- Password: admin

## Services

- **postgres**: PostgreSQL database
- **airflow-init**: Initializes Airflow database and creates admin user
- **airflow-webserver**: Airflow web interface
- **airflow-scheduler**: Airflow scheduler

## Volumes

- `postgres_data`: Persistent storage for PostgreSQL
- `airflow/dags`: Airflow DAGs directory
- `airflow/library`: Airflow library code
- `chunks`: Directory for input data chunks
- `raw_data`: Directory for raw data storage

## Environment Variables

All necessary environment variables are set in the docker-compose.yml file. The main ones are:

- `AIRFLOW__CORE__EXECUTOR`: Set to LocalExecutor
- `AIRFLOW__CORE__SQL_ALCHEMY_CONN`: PostgreSQL connection string
- `AIRFLOW__CORE__FERNET_KEY`: Empty for development
- `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`: Set to True
- `AIRFLOW__CORE__LOAD_EXAMPLES`: Set to False

## Stopping Services

```bash
docker-compose down
```

To remove volumes as well:
```bash
docker-compose down -v
``` 
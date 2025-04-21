"""initial taxi rides

Revision ID: d6b4393c5b33
Revises: 
Create Date: 2025-04-21 22:28:19.175356

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from datetime import datetime, timedelta


# revision identifiers, used by Alembic.
revision: str = 'd6b4393c5b33'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



def upgrade():
    # Create the main partitioned table
    op.execute("""
               CREATE TABLE taxi_rides
               (
                   id                         SERIAL,
                   trip_id                    VARCHAR(64)              NOT NULL,
                   taxi_id                    VARCHAR(128)             NOT NULL,
                   trip_start_timestamp       TIMESTAMP WITH TIME ZONE NOT NULL,
                   trip_end_timestamp         TIMESTAMP WITH TIME ZONE NOT NULL,
                   trip_seconds               INTEGER                  NOT NULL,
                   trip_miles                 NUMERIC(10, 2)           NOT NULL,
                   pickup_census_tract        VARCHAR(20),
                   dropoff_census_tract       VARCHAR(20),
                   pickup_community_area      INTEGER,
                   dropoff_community_area     INTEGER,
                   fare                       NUMERIC(10, 2)           NOT NULL,
                   tips                       NUMERIC(10, 2)           NOT NULL,
                   tolls                      NUMERIC(10, 2)           NOT NULL,
                   extras                     NUMERIC(10, 2)           NOT NULL,
                   trip_total                 NUMERIC(10, 2)           NOT NULL,
                   payment_type               VARCHAR(50)              NOT NULL,
                   company                    VARCHAR(100)             NOT NULL,
                   pickup_centroid_latitude   NUMERIC(10, 6),
                   pickup_centroid_longitude  NUMERIC(10, 6),
                   pickup_centroid_location   TEXT,
                   dropoff_centroid_latitude  NUMERIC(10, 6),
                   dropoff_centroid_longitude NUMERIC(10, 6),
                   dropoff_centroid_location  TEXT,
                   PRIMARY KEY (id, trip_start_timestamp)
               ) PARTITION BY RANGE (trip_start_timestamp);
               """)

    op.execute("""
               CREATE INDEX idx_trip_start_timestamp ON taxi_rides (trip_start_timestamp);
               CREATE INDEX idx_taxi_id ON taxi_rides (taxi_id);
               CREATE INDEX idx_trip_id ON taxi_rides (trip_id);
               """)

def downgrade():
    # Drop the table and all its partitions
    op.execute("DROP TABLE IF EXISTS taxi_rides CASCADE")
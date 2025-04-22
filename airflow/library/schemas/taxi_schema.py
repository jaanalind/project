import pandera.polars as pa
from datetime import datetime
from typing import Optional

class TaxiRideSchema(pa.DataFrameModel):
    trip_id: str = pa.Field(str_length={'min_value': 1, 'max_value': 64})
    taxi_id: str = pa.Field(str_length={'min_value': 1, 'max_value': 128})
    trip_start_timestamp: datetime
    trip_end_timestamp: datetime
    trip_seconds: int = pa.Field(ge=0)
    trip_miles: float = pa.Field(ge=0)
    pickup_census_tract: Optional[str] = pa.Field(nullable=True)
    dropoff_census_tract: Optional[str] = pa.Field(nullable=True)
    pickup_community_area: Optional[int] = pa.Field(nullable=True)
    dropoff_community_area: Optional[int] = pa.Field(nullable=True)
    fare: float = pa.Field(ge=0)
    tips: float = pa.Field(ge=0)
    tolls: float = pa.Field(ge=0)
    extras: float = pa.Field(ge=0)
    trip_total: float = pa.Field(ge=0)
    payment_type: str = pa.Field(str_length={'min_value': 1, 'max_value': 50})
    company: str = pa.Field(str_length={'min_value': 1, 'max_value': 100})
    pickup_centroid_latitude: Optional[float] = pa.Field(nullable=True, ge=-90, le=90)
    pickup_centroid_longitude: Optional[float] = pa.Field(nullable=True, ge=-180, le=180)
    pickup_centroid_location: Optional[str] = pa.Field(nullable=True)
    dropoff_centroid_latitude: Optional[float] = pa.Field(nullable=True, ge=-90, le=90)
    dropoff_centroid_longitude: Optional[float] = pa.Field(nullable=True, ge=-180, le=180)
    dropoff_centroid_location: Optional[str] = pa.Field(nullable=True) 
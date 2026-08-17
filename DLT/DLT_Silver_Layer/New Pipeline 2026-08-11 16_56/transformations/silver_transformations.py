"""DLT Pipeline for Flight Booking Data Processing.

This module defines a Delta Live Tables pipeline that processes flight bookings
and flight data through bronze to silver layer transformations with data quality rules.
"""

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Bookings Data

@dlt.table(
    name="stage_bookings"
)
def stage_bookings():
    """Create staging table for bookings data.
    
    Reads streaming Delta data from the bronze layer volume containing raw bookings.
    This serves as the entry point for bookings data into the pipeline.
    
    Returns:
        DataFrame: Streaming DataFrame with raw bookings data from bronze layer.
    """
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df

@dlt.temporary_view(
    name="trans_bookings"
)
def trans_bookings():
    """Transform bookings data with type casting and timestamp.
    
    Applies transformations to the staged bookings data:
    - Casts amount column to DoubleType for numerical operations
    - Adds modifiedDate timestamp for tracking
    - Converts booking_date string to proper date type
    - Removes rescued data column from schema inference
    
    Returns:
        DataFrame: Transformed streaming DataFrame with cleaned bookings data.
    """
    df = spark.readStream.table("stage_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("modifiedDate", current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")
    return df

rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}

@dlt.table(
    name="silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    """Create silver layer bookings table with data quality rules.
    
    Applies data quality expectations and drops records that don't meet the rules:
    - rule1: booking_id must not be null
    - rule2: passenger_id must not be null
    
    Returns:
        DataFrame: Cleaned streaming DataFrame containing only valid bookings.
    """
    df = spark.readStream.table("trans_bookings")
    return df

# -------------------------------------------
# Flights Data
@dlt.view(
    name="trans_flights"
)
def trans_flights():
    """Create view for flights data from bronze layer.
    
    Reads streaming Delta data from the bronze layer volume containing flight information.
    This view serves as the source for CDC (Change Data Capture) processing.
    
    Returns:
        DataFrame: Streaming DataFrame with flights data from bronze layer.
    """
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
    df = df.drop("_rescued_data")\
        .withColumn("modifiedDate", current_timestamp())
    return df

dlt.create_streaming_table("silver_flights")

dlt.apply_changes(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)

# -------------------------------------------
# Customers Data
@dlt.view(
    name="trans_passengers"
)
def trans_passengers():
    """Create view for passengers data from bronze layer.
    
    Reads streaming Delta data from the bronze layer volume containing passenger information.
    This view serves as the source for CDC (Change Data Capture) processing.
    
    Returns:
        DataFrame: Streaming DataFrame with passengers data from bronze layer.
    """
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
    df = df.drop("_rescued_data")\
        .withColumn("modifiedDate", current_timestamp())
    return df

dlt.create_streaming_table("silver_passengers")

dlt.apply_changes(
    target="silver_passengers",
    source="trans_passengers",
    keys=["passenger_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)

# -------------------------------------------
# Airports Data
@dlt.view(
    name="trans_airports"
)
def trans_airports():
    """Create view for airports data from bronze layer.
    
    Reads streaming Delta data from the bronze layer volume containing airport information.
    This view serves as the source for CDC (Change Data Capture) processing.
    
    Returns:
        DataFrame: Streaming DataFrame with airports data from bronze layer.
    """
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
    df = df.drop("_rescued_data")\
        .withColumn("modifiedDate", current_timestamp())
    return df

dlt.create_streaming_table("silver_airports")

dlt.apply_changes(
    target="silver_airports",
    source="trans_airports",
    keys=["airport_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)

# -------------------------------------------
# Silver Business View
@dlt.table(
    name="silver_business"
)
def silver_business():
    """Create silver layer business view with joined tables.
    
    Joins bookings with flights, passengers, and airports data.
    Handles duplicate modifiedDate columns by selecting only the bookings modifiedDate.
    
    Returns:
        DataFrame: Streaming DataFrame with comprehensive booking information.
    """
    bookings = spark.readStream.table("silver_bookings")
    flights = spark.readStream.table("silver_flights").drop("modifiedDate")
    passengers = spark.readStream.table("silver_passengers").drop("modifiedDate")
    airports = spark.readStream.table("silver_airports").drop("modifiedDate")
    
    df = bookings\
        .join(flights, ["flight_id"])\
        .join(passengers, ["passenger_id"])\
        .join(airports, ["airport_id"])
    return df
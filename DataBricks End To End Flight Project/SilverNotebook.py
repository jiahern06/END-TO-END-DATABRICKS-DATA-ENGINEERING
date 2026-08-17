# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.silver_airports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC comparison with the original table

# COMMAND ----------

df=spark.read.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
display(df)

# COMMAND ----------

# DBTITLE 1,Data Transformation
# MAGIC %md
# MAGIC ## Data Transformation
# MAGIC
# MAGIC  cleaning bookings data:
# MAGIC
# MAGIC * Converts the `amount` column from string to numeric type for mathematical operations
# MAGIC * Creates a `modifiedDate` column with the current timestamp to track when data was processed
# MAGIC * Transforms the `booking_date` from string to proper date type for date-based queries
# MAGIC * Drops the `_rescued_data` column (used by Delta for schema validation) since all data loaded successfully

# COMMAND ----------

df = df.withColumn("amount",col("amount").cast(DoubleType()))\
    .withColumn("modifiedDate",current_timestamp())\
        .withColumn("booking_date",to_date(col("booking_date")))\
        .drop("_rescued_data")
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------
# MAGIC ####Above this are just some of the transformation i want to implement seeing how it actually looks on the table
# MAGIC --------------------------------------

# COMMAND ----------

from pyspark.sql.functions import*
from pyspark.sql.types import *

# COMMAND ----------

df=spark.read.format("delta").load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
df.withColumn("flight_date",to_date(col("flight_date"))).drop("_rescued_data")\
    .withColumn("modifiedDate",current_timestamp())

display(df)

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import*

# COMMAND ----------

# MAGIC %md
# MAGIC tells databrick that we want to create a streaming table

# COMMAND ----------

# DBTITLE 1,Streaming Pipeline
# MAGIC %md
# MAGIC ## Streaming Pipeline with DLT
# MAGIC
# MAGIC Three-stage streaming data pipeline using Delta Live Tables (Lakeflow Spark Declarative Pipelines):
# MAGIC
# MAGIC **Cell 10 - Stage Table (`stage_brookings`)**
# MAGIC * Creates a streaming table that continuously reads new data from the bronze Delta source
# MAGIC * Acts as the initial ingestion layer in the pipeline
# MAGIC * Uses `spark.readStream` to process data incrementally as it arrives
# MAGIC
# MAGIC **Cell 11 - Transformation View (`trans_bookings`)**
# MAGIC * Creates a DLT view that reads from the `stage_brookings` table
# MAGIC * Applies the same data transformations: type casting, timestamp addition, date conversion, and column cleanup
# MAGIC * Prepares clean, transformed data for downstream consumption
# MAGIC
# MAGIC **Cell 12 - Silver Table (`silver_bookings`)**
# MAGIC * the cleaned, transformed data as a managed Delta table
# MAGIC * This is the curated dataset that downstream analytics, dashboards, and gold layer pipelines will consume

# COMMAND ----------

@dlt.table(
    name="stage_brookings"
)
def stage_brookings():
    df=spark.readstream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df



# COMMAND ----------

@dlt.view(
    name="trans_bookings"
)
def trans_bookings():
    df=spark.readStream.table("stage_brookings")
    df=df.withColumn("amount",col("amount").cast(DoubleType()))\
    .withColumn("modifiedDate",current_timestamp())\
        .withColumn("booking_date",to_date(col("booking_date")))\
        .drop("_rescued_data")
    return df
    

# COMMAND ----------

rules ={
    "rule1: booking_id IS NOT NULL"
    "rule2: passenger_id IS NOT NULL"
}

# COMMAND ----------

# MAGIC %md
# MAGIC * expect_all = warning (default)
# MAGIC * expect_all_or_drop = drop
# MAGIC * expect_all_or_fail = fail

# COMMAND ----------

@dlt.table(
    name="silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df=spark.readStream.table("trans_bookings")
    return df

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df=spark.read.format("delta")\
    .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
df.drop ("_rescued_data")\
    .withColumn("modifiedDate", current_timestamp())

display(df)
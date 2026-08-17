# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.silver_flights

# COMMAND ----------

# MAGIC %md
# MAGIC ### PARAMETERS

# COMMAND ----------

# DBTITLE 1,Parameters Configuration
"""
PARAMETERS CONFIGURATION FOR DIMENSION TABLE ETL

This cell defines all the parameters needed for the incremental dimension load:
- Catalog, schema, and table names for source and target
- Key columns for matching records between source and target
- CDC (Change Data Capture) column for tracking modifications
- Surrogate key name for the dimension table
- Optional backdated refresh date to reprocess historical data
"""

# Catalog Name
catalog = "workspace"

# Key Cols List - Business key(s) used to identify unique records
key_cols = "['flight_id']"
key_cols_list = eval(key_cols)

# CDC Column - Timestamp column that tracks when records were last modified
cdc_col = "modifiedDate"

# Backdated Refresh - Set to a date string to reprocess from that date, leave empty for normal incremental
backdated_refresh = ""

# Source Object - Silver layer table name
source_object = "silver_flights"

# Source Schema
source_schema = "silver"

# Target Schema - Gold layer
target_schema = "gold"

# Target Object - Dimension table name
target_object = "DimFlights"

# Surrogate Key - Auto-incrementing integer key for the dimension table
surrogate_key = "DimFlightsKey"

# COMMAND ----------

 # Catalog Name
 catalog = "workspace"

 # Key Cols List
 key_cols = "['airport_id']"
 key_cols_list = eval(key_cols)

 # CDC Column
 cdc_col = "modifiedDate"

 # Backdated Refresh
 backdated_refresh = ""

 # Source Object
 source_object = "silver_airports"

 # Source Schema
 source_schema = "silver"

 # Target Schema 
 target_schema = "gold"

 # Target Object 
 target_object = "DimAirports"

 # Surrogate Key
 surrogate_key = "DimAirportsKey"

# COMMAND ----------

# DBTITLE 1,Cell 3
# Catalog Name
catalog = "workspace"

# Key Cols List
key_cols = "['passenger_id']"
key_cols_list = eval(key_cols)

# CDC Column
cdc_col = "modifiedDate"

# Backdated Refresh
backdated_refresh = ""

# Source Object
source_object = "silver_passengers"

# Source Schema
source_schema = "silver"

# Target Schema 
target_schema = "gold"

# Target Object 
target_object = "DimPassengers"

# Surrogate Key
surrogate_key = "DimPassengersKey"

# COMMAND ----------

key_cols_list

# COMMAND ----------

# MAGIC %md
# MAGIC ### INCREMENTAL DATA INGESTION

# COMMAND ----------

# MAGIC %md
# MAGIC Last Load Date

# COMMAND ----------

# DBTITLE 1,Determine Last Load Date
"""
DETERMINE LAST LOAD DATE FOR INCREMENTAL PROCESSING

This cell determines the starting point for the incremental load:
- If target table exists: Get the max CDC timestamp from existing dimension records
- If target table doesn't exist: Use a very old date to load all source records (initial load)
- If backdated_refresh is set: Use that date to reprocess from a specific point in time
"""

# No Back Dated Refresh - Normal incremental mode
if len(backdated_refresh) == 0:
  
  # If Table Exists In The Destination - Incremental Load
  if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    # Get the most recent modifiedDate from the target dimension table
    last_load = spark.sql(f"SELECT max({cdc_col}) FROM workspace.{target_schema}.{target_object}").collect()[0][0]
    
  else:
    # Target table doesn't exist - Initial Load
    last_load = "1900-01-01 00:00:00"

# Yes Back Dated Refresh - Reprocess from a specific date
else:
  last_load = backdated_refresh

# Test The Last Load 
last_load

# COMMAND ----------

# DBTITLE 1,Extract Changed Records from Source
"""
EXTRACT CHANGED RECORDS FROM SOURCE (CDC FILTER)

Fetch only records that have been modified since the last load.
This WHERE clause is the core of the incremental pattern - we only process
records with modifiedDate > last_load, avoiding full table scans.
"""

df_src = spark.sql(f"SELECT * FROM {source_schema}.{source_object} WHERE {cdc_col} > '{last_load}'")

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###OLD vs NEW RECORDS

# COMMAND ----------

# DBTITLE 1,Load Target DataFrame Explanation
# MAGIC %md
# MAGIC **Load Target DataFrame (`df_trg`)**
# MAGIC
# MAGIC This cell handles two scenarios:
# MAGIC
# MAGIC **Scenario 1: Incremental Load** (Table exists)
# MAGIC * Loads existing records from the target dimension table
# MAGIC * Retrieves the key columns, surrogate key, and audit timestamps (`create_date`, `update_date`)
# MAGIC * Used to match against source records and identify which are new vs. updates
# MAGIC
# MAGIC **Scenario 2: Initial Load** (Table does NOT exist)
# MAGIC * Creates an empty DataFrame with the correct schema
# MAGIC * Uses placeholder values (empty strings for keys, `0` for surrogate key, default dates)
# MAGIC * The `WHERE 1=0` clause ensures zero rows are returned
# MAGIC * This schema-only DataFrame allows the join logic to work without errors on the first run

# COMMAND ----------

# DBTITLE 1,Load Target Dimension Table
"""
LOAD TARGET DIMENSION TABLE (df_trg)

Two scenarios handled here:

1. INCREMENTAL LOAD (table exists):
   - Load all existing dimension records with their business keys, surrogate keys, and audit timestamps
   - This allows us to match incoming source records against existing dimension records
   - Matching records = OLD (updates), Non-matching records = NEW (inserts)

2. INITIAL LOAD (table doesn't exist):
   - Create an empty DataFrame with the correct schema (business keys, surrogate key, timestamps)
   - WHERE 1=0 ensures zero rows returned - we just need the schema structure
   - On the LEFT JOIN, all source records will have NULL for target columns = all NEW records
"""

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"): 

  # Key Columns String For Incremental
  key_cols_string_incremental = ", ".join(key_cols_list)

  # Load existing dimension records with their keys and timestamps
  df_trg = spark.sql(f"""SELECT {key_cols_string_incremental}, {surrogate_key}, create_date, update_date 
                      FROM {catalog}.{target_schema}.{target_object}""")


else:

  # Key Columns String For Initial - Create empty string placeholders for keys
  key_cols_string_init = [f"'' AS {i}" for i in key_cols_list]
  key_cols_string_init = ", ".join(key_cols_string_init)
  
  # Create schema-only DataFrame with zero rows (WHERE 1=0)
  df_trg = spark.sql(f"""SELECT {key_cols_string_init}, CAST('0' AS INT) AS {surrogate_key}, CAST('1900-01-01 00:00:00' AS timestamp) AS          create_date, CAST('1900-01-01 00:00:00' AS timestamp) AS update_date WHERE 1=0""")

# COMMAND ----------

spark.sql(f"SELECT '' AS flight_id, ''AS DimFlightsKey, '1900-01-01 00:00:00' AS create_date, '1900-01-01 00:00:00' AS update_date FROM workspace.silver.silver_flights").display()

# COMMAND ----------

df_trg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOIN CONDITION

# COMMAND ----------

# DBTITLE 1,Build Join Condition
"""
BUILD JOIN CONDITION DYNAMICALLY

Constructs the join condition based on the business key columns.
Example: If key_cols_list = ['flight_id'], this creates: "src.flight_id = trg.flight_id"
Example: If key_cols_list = ['flight_id', 'airline_code'], this creates: "src.flight_id = trg.flight_id AND src.airline_code = trg.airline_code"
"""

join_condition = ' AND '.join([f"src.{i} = trg.{i}" for i in key_cols_list])

# COMMAND ----------

# DBTITLE 1,LEFT JOIN: Match Source vs Target
"""
LEFT JOIN: MATCH SOURCE VS TARGET TO IDENTIFY OLD AND NEW RECORDS

This is the core logic that determines which records are updates vs inserts:

- LEFT JOIN ensures ALL source records are returned
- Records that MATCH on business keys get populated target columns (surrogate_key, create_date, update_date)
  → These are OLD records (existing dimension records being updated)
- Records that DON'T MATCH get NULL for target columns
  → These are NEW records (not yet in the dimension, will get new surrogate keys)

Result DataFrame (df_join) contains:
- All source columns (passenger_id, name, gender, nationality, modifiedDate)
- Target surrogate key (DimPassengersKey) - populated for OLD, NULL for NEW
- Target timestamps (create_date, update_date) - populated for OLD, NULL for NEW
"""

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
            SELECT src.*, 
                   trg.{surrogate_key},
                   trg.create_date,
                   trg.update_date
            FROM src
            LEFT JOIN trg
            ON {join_condition}
            """)

# COMMAND ----------

df_join.display()

# COMMAND ----------

# DBTITLE 1,Split OLD and NEW Records
"""
SPLIT OLD AND NEW RECORDS BASED ON SURROGATE KEY

After the LEFT JOIN, we can identify record types by checking if the surrogate key is NULL:

- OLD RECORDS: Surrogate key IS NOT NULL
  → These matched existing dimension records on business keys
  → Will be UPDATED in the target dimension table
  → Keep their existing surrogate key and create_date, update update_date only

- NEW RECORDS: Surrogate key IS NULL
  → These didn't match any existing dimension records
  → Will be INSERTED into the target dimension table
  → Need to generate new surrogate keys and set both create_date and update_date
"""

# OLD RECORDS - Existing dimension records being updated
df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())

# NEW RECORDS - New dimension records being inserted
df_new = df_join.filter(col(f'{surrogate_key}').isNull())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ENRICHING DFS

# COMMAND ----------

# MAGIC %md
# MAGIC Preparing DF_OLD

# COMMAND ----------

# DBTITLE 1,Enrich OLD Records
"""
ENRICH OLD RECORDS (Existing Dimension Records)

For existing records being updated:
- Keep the existing surrogate key (already populated from the JOIN)
- Keep the existing create_date (when the record was first inserted)
- Update the update_date to current timestamp (mark when this update occurred)
"""

df_old_enr = df_old.withColumn('update_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC Preparing DF_NEW

# COMMAND ----------

df_new.display()

# COMMAND ----------

# DBTITLE 1,Enrich NEW Records
"""
ENRICH NEW RECORDS (Insert New Dimension Records)

For new records being inserted:
- Generate new surrogate keys starting from the current max + 1
- Use monotonically_increasing_id() to ensure unique keys across partitions
- Set both create_date and update_date to current timestamp (first time seeing this record)

Two scenarios:
1. Table exists: Get max surrogate key from existing table, start new keys from max+1
2. Table doesn't exist (initial load): Start surrogate keys from 1

Formula: new_surrogate_key = max_existing_key + 1 + monotonically_increasing_id()
Example: If max key is 225, new keys will be 226, 227, 228, ...
"""

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"): 
    # Get the highest surrogate key from existing dimension table
    max_surrogate_key = spark.sql(f"""
                            SELECT max({surrogate_key}) FROM {catalog}.{target_schema}.{target_object}
                        """).collect()[0][0]
    # Generate new surrogate keys starting from max + 1
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                    .withColumn('create_date', current_timestamp())\
                    .withColumn('update_date', current_timestamp())    

else:
    # Initial load - start surrogate keys from 1
    max_surrogate_key = 0
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                    .withColumn('create_date', current_timestamp())\
                    .withColumn('update_date', current_timestamp())


# COMMAND ----------

df_old_enr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unioning OLD AND NEW RECORDS
# MAGIC

# COMMAND ----------

# DBTITLE 1,Union OLD and NEW Records
"""
UNION OLD AND NEW RECORDS

Combine the enriched OLD and NEW DataFrames into a single dataset.
This unified dataset contains:
- OLD records: With existing surrogate keys and updated update_date
- NEW records: With newly generated surrogate keys and fresh timestamps

Both DataFrames have the same schema after enrichment, so we can safely union them.
"""

df_union = df_old_enr.unionByName(df_new_enr)

# COMMAND ----------

df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC UPSERT

# COMMAND ----------

from delta.tables import DeltaTable 

# COMMAND ----------

# DBTITLE 1,UPSERT to Target Dimension Table
"""
UPSERT (UPDATE + INSERT) TO TARGET DIMENSION TABLE

This is the final step that writes changes to the gold dimension table.

Two scenarios:

1. TABLE EXISTS - Incremental MERGE (UPSERT):
   - Match on surrogate key between source (df_union) and target (dimension table)
   - WHEN MATCHED: Update the dimension record IF source modifiedDate >= target modifiedDate
     → This prevents out-of-order updates from overwriting newer data
   - WHEN NOT MATCHED: Insert new dimension records
   
2. TABLE DOESN'T EXIST - Initial Load:
   - Simply append all records to create the dimension table for the first time
   - All records are NEW, so no merge logic needed

Delta Lake MERGE ensures transactional consistency - the operation is all-or-nothing.
"""

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    # MERGE operation for incremental updates
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
                        .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
                        .whenNotMatchedInsertAll()\
                        .execute()

else: 

    # Initial load - create the table and append all records
    df_union.write.format("delta")\
            .mode("append")\
            .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dimpassengers where passenger_id = 'P0049'

# COMMAND ----------


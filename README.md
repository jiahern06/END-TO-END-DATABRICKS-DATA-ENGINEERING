# ✈️ End-to-End Flight Booking Data Engineering Pipeline

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)

## 📋 Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies](#technologies)
- [Project Structure](#project-structure)
- [Data Pipeline Flow](#data-pipeline-flow)
- [Layer Implementation](#layer-implementation)
- [Data Model](#data-model)
- [Key Features](#key-features)
- [Setup & Deployment](#setup--deployment)
- [Use Cases](#use-cases)

---

## 🎯 Overview

This project implements a **production-ready, scalable data engineering pipeline** for processing flight booking data on Databricks. It follows the **Medallion Architecture** (Bronze → Silver → Gold) and demonstrates best practices in:

- ✅ **Incremental data ingestion** using Auto Loader
- ✅ **Streaming ETL** with Delta Live Tables (DLT)
- ✅ **Change Data Capture (CDC)** for real-time updates
- ✅ **Data quality enforcement** with automated expectations
- ✅ **Dimensional modeling** for analytics (Star Schema)
- ✅ **SCD Type 1** handling for slowly changing dimensions

### Business Problem

Airlines and travel platforms need to process millions of booking transactions, flight schedules, passenger records, and airport data in real-time. This pipeline enables:
- Real-time booking analytics
- Customer behavior insights
- Flight performance monitoring
- Revenue tracking and forecasting

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         MEDALLION ARCHITECTURE                          │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────┐       ┌──────────────┐       ┌──────────────┐       ┌──────────────┐
│              │       │              │       │              │       │              │
│  RAW LAYER   │──────▶│ BRONZE LAYER │──────▶│ SILVER LAYER │──────▶│  GOLD LAYER  │
│              │       │              │       │              │       │              │
│  CSV Files   │ Auto  │ Raw Delta    │  DLT  │  Cleansed    │ Star  │ Fact & Dim   │
│  in Volumes  │Loader │   Tables     │ +CDC  │   Tables     │Schema │   Tables     │
│              │       │              │       │              │       │              │
└──────────────┘       └──────────────┘       └──────────────┘       └──────────────┘
      │                      │                       │                      │
      │                      │                       │                      │
      ▼                      ▼                       ▼                      ▼
   Source                Schema              Data Quality           Analytics Ready
    Data               Evolution            Enforcement             Dimensional Model
                      (Rescue)              (Expectations)
```

### Layer Breakdown

| Layer | Purpose | Technology | Key Features |
|-------|---------|------------|-------------|
| **Raw** | Landing zone for source data | Unity Catalog Volumes | CSV files from upstream systems |
| **Bronze** | Raw ingestion with schema inference | Auto Loader + Structured Streaming | Incremental load, schema evolution, checkpointing |
| **Silver** | Cleansed and validated data | Delta Live Tables (DLT) | Data quality rules, CDC, deduplication |
| **Gold** | Business-ready dimensional model | Star Schema | Fact tables, dimension tables, surrogate keys |

---

## 🛠️ Technologies

- **Platform**: Databricks on AWS
- **Compute**: Serverless Clusters (Photon-enabled)
- **Storage**: Unity Catalog Volumes + Delta Lake
- **Processing**: 
  - Apache Spark (PySpark)
  - Delta Live Tables (DLT)
  - Structured Streaming
- **Ingestion**: Auto Loader (cloudFiles)
- **Data Quality**: DLT Expectations (`@dlt.expect_all_or_drop`)
- **CDC**: `dlt.apply_changes()` with SCD Type 1

---

## 📁 Project Structure

```
DataBricks End To End Flight Project/
│
├── BronzeLayer.ipynb                    # Auto Loader ingestion notebook
├── Setup.ipynb                          # Initial catalog/volume setup
├── SrcParameters.ipynb                  # Configuration parameters
│
├── DLT/
│   └── DLT_Silver_Layer/
│       └── transformations/
│           └── silver_transformations.py # DLT pipeline definitions
│
├── GOLD_DIMS.ipynb                      # Dimension table ETL (SCD Type 1)
├── GOLD_FACT.ipynb                      # Fact table ETL (incremental)
└── SilverNotebook.ipynb                 # Ad-hoc silver layer queries


📊 Unity Catalog Structure:

workspace (catalog)
├── raw (schema)
│   └── rawvolume (volume)              # Landing zone for CSV files
│       ├── bookings/
│       ├── flights/
│       ├── customers/
│       └── airports/
│
├── bronze (schema)
│   └── bronzevolume (volume)           # Raw Delta tables + checkpoints
│       ├── bookings/
│       ├── flights/
│       ├── customers/
│       └── airports/
│
├── silver (schema)                     # DLT managed tables
│   ├── stage_bookings
│   ├── silver_bookings
│   ├── silver_flights
│   ├── silver_passengers
│   ├── silver_airports
│   └── silver_business (joined view)
│
└── gold (schema)                       # Star schema
    ├── FactBookings
    ├── DimFlights
    ├── DimPassengers
    └── DimAirports
```

---

## 🔄 Data Pipeline Flow

### 1️⃣ **Raw to Bronze Layer** (Auto Loader)

**Notebook**: `BronzeLayer.ipynb`

```python
# Incremental ingestion with schema evolution
df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("cloudFiles.schemaLocation", f"/Volumes/.../checkpoint")\
    .option("cloudFiles.schemaEvolutionMode", "rescue")\
    .load(f"/Volumes/.../rawdata/{src_value}/")

df.writeStream.format("delta")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("checkpointLocation", f"/Volumes/.../checkpoint")\
    .start()
```

**Key Features**:
- ✅ Incremental file processing (only new files)
- ✅ Schema inference and evolution
- ✅ Rescued data column for malformed records
- ✅ Exactly-once processing with checkpoints

---

### 2️⃣ **Bronze to Silver Layer** (Delta Live Tables)

**File**: `silver_transformations.py`

#### Bookings Pipeline (Streaming ETL)

```python
@dlt.table(name="stage_bookings")
def stage_bookings():
    return spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")

@dlt.temporary_view(name="trans_bookings")
def trans_bookings():
    df = spark.readStream.table("stage_bookings")
    return df.withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("modifiedDate", current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")

rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}

@dlt.table(name="silver_bookings")
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    return spark.readStream.table("trans_bookings")
```

#### CDC Pipeline (Flights, Passengers, Airports)

```python
@dlt.view(name="trans_flights")
def trans_flights():
    return spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")\
        .drop("_rescued_data")\
        .withColumn("modifiedDate", current_timestamp())

dlt.create_streaming_table("silver_flights")

dlt.apply_changes(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1  # Keep latest version only
)
```

**Key Features**:
- ✅ Streaming transformations with DLT
- ✅ Data type casting and standardization
- ✅ Automated data quality checks (drop invalid records)
- ✅ CDC handling for dimension tables
- ✅ Deduplication based on business keys

---

### 3️⃣ **Silver to Gold Layer** (Dimensional Modeling)

**Notebooks**: `GOLD_DIMS.ipynb`, `GOLD_FACT.ipynb`

#### Dimension Tables (SCD Type 1)

**Notebook**: `GOLD_DIMS.ipynb`

```python
# Configuration
catalog = "workspace"
key_cols_list = ['flight_id']  # Business key
cdc_col = "modifiedDate"
source_object = "silver_flights"
target_object = "DimFlights"
surrogate_key = "DimFlightsKey"

# Incremental load: Only changed records
last_load = spark.sql(f"SELECT max({cdc_col}) FROM {target_object}").collect()[0][0]
df_src = spark.sql(f"SELECT * FROM {source_schema}.{source_object} WHERE {cdc_col} > '{last_load}'")

# Generate surrogate keys
max_key = spark.sql(f"SELECT COALESCE(MAX({surrogate_key}), 0) FROM {target_object}").collect()[0][0]
df_new = df_new.withColumn(surrogate_key, row_number().over(...) + max_key)

# Upsert into dimension table
from delta.tables import DeltaTable

DeltaTable.forPath(spark, target_path).alias("trg").merge(
    df_final.alias("src"),
    merge_condition
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

**Features**:
- ✅ Incremental dimension loading (CDC-based)
- ✅ Auto-generated surrogate keys
- ✅ SCD Type 1 updates (overwrite on match)
- ✅ Schema evolution handling

#### Fact Table (Incremental Loading)

**Notebook**: `GOLD_FACT.ipynb`

```python
# Dynamic fact query builder
def generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_column, last_load):
    # Join silver bookings with dimension tables to get surrogate keys
    query = f"""
    SELECT 
        f.amount, f.booking_date, f.modifiedDate,
        DimPassengers.DimPassengersKey,
        DimFlights.DimFlightsKey,
        DimAirports.DimAirportsKey
    FROM {fact_table} f
    LEFT JOIN DimPassengers ON f.passenger_id = DimPassengers.passenger_id
    LEFT JOIN DimFlights ON f.flight_id = DimFlights.flight_id
    LEFT JOIN DimAirports ON f.airport_id = DimAirports.airport_id
    WHERE f.{cdc_column} > '{last_load}'
    """
    return query

# Upsert into fact table (composite key)
fact_key_cols = ["DimPassengersKey", "DimFlightsKey", "DimAirportsKey", "booking_date"]
merge_condition = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])

DeltaTable.forPath(spark, fact_path).alias("trg").merge(
    df_fact.alias("src"),
    merge_condition
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

**Features**:
- ✅ Dynamic query generation
- ✅ Incremental fact loading (only new bookings)
- ✅ Foreign key lookups (surrogate keys from dimensions)
- ✅ Composite key upserts

---

## 📊 Data Model

### Star Schema Design

```
                    ┌─────────────────────┐
                    │   FactBookings      │
                    ├─────────────────────┤
                    │ DimPassengersKey FK │
                    │ DimFlightsKey    FK │
                    │ DimAirportsKey   FK │
                    │ booking_date        │
                    │ amount              │
                    │ modifiedDate        │
                    └─────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ DimPassengers   │  │   DimFlights    │  │  DimAirports    │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│DimPassengersKey │  │ DimFlightsKey   │  │ DimAirportsKey  │
│ passenger_id    │  │ flight_id       │  │ airport_id      │
│ passenger_name  │  │ departure_city  │  │ airport_name    │
│ gender          │  │ arrival_city    │  │ city            │
│ nationality     │  │ departure_time  │  │ country         │
│ create_date     │  │ arrival_time    │  │ create_date     │
│ update_date     │  │ create_date     │  │ update_date     │
└─────────────────┘  │ update_date     │  └─────────────────┘
                     └─────────────────┘
```

### Table Schemas

#### FactBookings
- **Grain**: One row per booking
- **Foreign Keys**: DimPassengersKey, DimFlightsKey, DimAirportsKey
- **Measures**: amount (revenue)
- **Dimensions**: booking_date

#### DimPassengers (SCD Type 1)
- **Business Key**: passenger_id
- **Attributes**: passenger_name, gender, nationality
- **Audit**: create_date, update_date

#### DimFlights (SCD Type 1)
- **Business Key**: flight_id
- **Attributes**: departure_city, arrival_city, departure_time, arrival_time
- **Audit**: create_date, update_date

#### DimAirports (SCD Type 1)
- **Business Key**: airport_id
- **Attributes**: airport_name, city, country
- **Audit**: create_date, update_date

---

## ⚡ Key Features

### 1. **Incremental Data Processing**
- Only new/changed files are processed (Auto Loader)
- CDC-based incremental loads for dimensions and facts
- Checkpoint management for exactly-once semantics

### 2. **Data Quality Enforcement**
```python
rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}
@dlt.expect_all_or_drop(rules)
```
- Automated validation with DLT expectations
- Invalid records are dropped and logged

### 3. **Schema Evolution**
```python
.option("cloudFiles.schemaEvolutionMode", "rescue")
```
- Automatic schema inference
- New columns captured in `_rescued_data`

### 4. **Change Data Capture**
```python
dlt.apply_changes(
    target="silver_flights",
    source="trans_flights",
    keys=["flight_id"],
    sequence_by=col("modifiedDate"),
    stored_as_scd_type=1
)
```
- Real-time upserts based on modification timestamp
- Automatic deduplication

### 5. **Dynamic ETL Patterns**
- Parameterized dimension loading
- Reusable query generation functions
- Configuration-driven pipelines

### 6. **Performance Optimizations**
- Photon-enabled Serverless compute
- Delta Lake optimizations (Z-ordering, file compaction)
- Partitioning strategies

---

## 🚀 Setup & Deployment

### Prerequisites
- Databricks workspace (AWS, Azure, or GCP)
- Unity Catalog enabled
- Serverless compute or cluster with DBR 17.3+

### Step 1: Initialize Catalog Structure

```sql
-- Run in Setup.ipynb
CREATE CATALOG IF NOT EXISTS workspace;

CREATE SCHEMA IF NOT EXISTS workspace.raw;
CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;

CREATE VOLUME IF NOT EXISTS workspace.raw.rawvolume;
CREATE VOLUME IF NOT EXISTS workspace.bronze.bronzevolume;
```

### Step 2: Upload Source Data

Upload CSV files to Unity Catalog volumes:
```
/Volumes/workspace/raw/rawvolume/rawdata/
  ├── bookings/
  ├── flights/
  ├── customers/
  └── airports/
```

### Step 3: Run Bronze Layer Ingestion

```python
# BronzeLayer.ipynb
dbutils.widgets.text("src", "")
src_value = dbutils.widgets.get("src")  # Pass "bookings", "flights", etc.

# Run notebook for each source
```

### Step 4: Deploy DLT Pipeline

1. Create a new DLT pipeline in Databricks UI
2. Add `silver_transformations.py` as the notebook path
3. Configure:
   - **Target Schema**: `workspace.silver`
   - **Storage Location**: `/Volumes/workspace/silver/`
   - **Pipeline Mode**: Triggered or Continuous
4. Start the pipeline

### Step 5: Run Gold Layer ETL

```python
# Run GOLD_DIMS.ipynb for each dimension:
# - DimPassengers
# - DimFlights
# - DimAirports

# Then run GOLD_FACT.ipynb
```

---

## 💼 Use Cases

This pipeline enables various analytics and operational use cases:

### 1. **Revenue Analytics**
```sql
SELECT 
    p.nationality,
    f.departure_city,
    SUM(fb.amount) as total_revenue,
    COUNT(*) as booking_count
FROM workspace.gold.FactBookings fb
JOIN workspace.gold.DimPassengers p ON fb.DimPassengersKey = p.DimPassengersKey
JOIN workspace.gold.DimFlights f ON fb.DimFlightsKey = f.DimFlightsKey
GROUP BY p.nationality, f.departure_city
ORDER BY total_revenue DESC;
```

### 2. **Flight Performance Monitoring**
```sql
SELECT 
    f.flight_id,
    f.departure_city,
    f.arrival_city,
    COUNT(fb.DimFlightsKey) as bookings,
    SUM(fb.amount) as revenue
FROM workspace.gold.DimFlights f
LEFT JOIN workspace.gold.FactBookings fb ON f.DimFlightsKey = fb.DimFlightsKey
GROUP BY f.flight_id, f.departure_city, f.arrival_city
ORDER BY bookings DESC;
```

### 3. **Customer Segmentation**
```sql
SELECT 
    p.nationality,
    p.gender,
    COUNT(DISTINCT fb.DimPassengersKey) as passenger_count,
    AVG(fb.amount) as avg_booking_value
FROM workspace.gold.DimPassengers p
JOIN workspace.gold.FactBookings fb ON p.DimPassengersKey = fb.DimPassengersKey
GROUP BY p.nationality, p.gender;
```

### 4. **Airport Traffic Analysis**
```sql
SELECT 
    a.airport_name,
    a.city,
    a.country,
    COUNT(fb.DimAirportsKey) as total_bookings
FROM workspace.gold.DimAirports a
JOIN workspace.gold.FactBookings fb ON a.DimAirportsKey = fb.DimAirportsKey
GROUP BY a.airport_name, a.city, a.country
ORDER BY total_bookings DESC;
```
---

## 📧 Contact

**Author**: Alvin Wong  
**Email**: alvinwongjh2006@gmail.com  
**LinkedIn**: [Connect with me](https://www.linkedin.com/in/alvin-wong)  
**GitHub**: [View my projects](https://github.com/alvinwongjh2006)

---

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

⭐ **If you found this project helpful, please give it a star!**

---

*Built with ❤️ on Databricks*

# End-to-End Databricks Flight Data Engineering Project

An end-to-end medallion architecture project built in Databricks for ingesting, cleaning, enriching, and modeling flight booking data. The pipeline starts with raw CSV files, lands them in a bronze Delta layer, transforms them into curated silver tables with Delta Live Tables, and publishes analytics-ready gold dimension and fact tables for reporting.

## Project highlights

- **Medallion architecture**: raw → bronze → silver → gold
- **Incremental processing** with CDC-style logic and `modifiedDate`
- **Auto Loader ingestion** for scalable file-based loading
- **Delta Live Tables (DLT)** for streaming transformations and data quality rules
- **Surrogate-key dimensions** and a **star-schema fact table**
- **Upsert logic** for both dimensions and fact tables to keep data current

## How the project works

### 1) Raw layer
Raw source files are organized by dataset, such as `bookings`, `flights`, `customers`, and `airports`.

`Setup.py` creates the raw volume used by the pipeline:

- `workspace.raw.rawvolume`

`SrcParameters.py` defines the list of source datasets used by the job orchestration layer.

### 2) Bronze layer
`SilverNotebook.py` contains the bronze ingestion logic, while `BronzeLayer.py` sets up the raw volume and provides a simple Delta read for inspection.

Main behavior:

- Uses `cloudFiles` for incremental file ingestion
- Reads from `/Volumes/workspace/raw/rawvolume/rawdata/<source>/`
- Writes bronze Delta output to `/Volumes/workspace/bronze/bronzevolume/<source>/`
- Uses checkpointing so each source can be processed independently

This layer preserves the raw structure while making the data queryable in Delta format.

Suggested execution order:

1. Run `Setup.py` to create the raw volume.
2. Run the bronze ingestion notebook logic to land CSV files into Delta bronze tables.
3. Run the DLT silver pipeline to standardize and validate the data.
4. Run `GOLD_DIMS.py` and `GOLD_FACT.py` to publish the gold model.

### 3) Silver layer
`DLT/DLT_Silver_Layer/DLT_Pipeline-2026-08-11 08_57_39.821.py` and `SilverNotebook.py` define the silver transformations.

The silver pipeline:

- Creates a staging table for bookings
- Cleans and casts fields like `amount`
- Converts dates to proper date types
- Adds `modifiedDate` for incremental processing
- Applies data quality rules to drop invalid bookings
- Builds streaming silver tables for:
  - `silver_bookings`
  - `silver_flights`
  - `silver_passengers`
  - `silver_airports`
- Produces a joined business view, `silver_business`, that combines bookings with flights, passengers, and airports

### 4) Gold layer
The gold layer contains the final dimensional model:

- `GOLD_DIMS.py` builds and incrementally updates:
  - `DimFlights`
  - `DimAirports`
  - `DimPassengers`
- `GOLD_FACT.py` builds:
  - `FactBookings`

Gold processing includes:

- incremental filtering by the CDC column
- dynamic joins to dimension tables
- surrogate key handling
- deduplication before merge
- Delta merge-based upserts

## Key design patterns

### Incremental ingestion
The project avoids full reloads by filtering on the most recent `modifiedDate` or a user-supplied backfill date.

### Data quality
DLT expectations ensure records missing critical business keys are dropped before they reach downstream layers.

### Dimensional modeling
Gold dimensions use surrogate keys and audit timestamps so the project can support analytics workloads cleanly.

### Dynamic fact enrichment
The fact notebook dynamically builds joins to the dimension tables, which makes the pipeline easier to extend as the model evolves.

## Notebook / file guide

| File | Purpose |
|---|---|
| `Setup.py` | Creates the raw volume used by the project |
| `SrcParameters.py` | Defines the source dataset list for orchestration |
| `BronzeLayer.py` | Bronze ingestion entry point |
| `SilverNotebook.py` | Silver transformation experiments and DLT examples |
| `DLT/DLT_Silver_Layer/DLT_Pipeline-2026-08-11 08_57_39.821.py` | Main DLT silver pipeline |
| `GOLD_DIMS.py` | Incremental dimension loads |
| `GOLD_FACT.py` | Incremental fact load |

## Data flow summary

`Raw CSV files` → `Bronze Delta tables` → `Silver cleaned tables` → `Gold dimensions and fact tables`

## What makes this portfolio-worthy

This project demonstrates practical production-style Databricks engineering:

- layered lakehouse design
- streaming ingestion
- schema handling
- reusable parameterized notebooks
- DLT-based data quality controls
- incremental CDC-style loads
- dimensional modeling for downstream analytics

## Notes

- Paths are written for Databricks volumes and the `workspace` catalog.
- The project is designed to run inside a Databricks environment with Delta Lake and DLT support.

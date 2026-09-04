# Big Data Engineering Capstone — Samsung Innovation Campus

*(Group Project — Group 17)*

An end-to-end big data pipeline built for the Samsung Innovation Campus Big Data Engineering track. Raw UK retail sales and NOAA weather data are ingested through Apache NiFi, staged in HDFS, transformed and modeled with Spark on YARN, and loaded into Snowflake as a Kimball-style star schema — the whole flow orchestrated by Apache Airflow and containerized with Docker.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Pipeline Stages](#pipeline-stages)
  - [1. Ingestion (NiFi → HDFS)](#1-ingestion-nifi--hdfs)
  - [2. Transformations (Silver layer)](#2-transformations-silver-layer)
  - [3. Modeling (Gold layer → Snowflake)](#3-modeling-gold-layer--snowflake)
- [Orchestration (Airflow DAG)](#orchestration-airflow-dag)
- [Star Schema](#star-schema)
- [Tech Stack](#tech-stack)
- [Running the Project](#running-the-project)
- [Repository Contents](#repository-contents)
- [Data Quality Notes](#data-quality-notes)
- [Possible Extensions](#possible-extensions)

## Overview

This project simulates a production-style big data platform: a scheduled, fault-tolerant pipeline that ingests two very different data sources — retail transactions and weather observations — reconciles them geographically and temporally, and lands the result in a cloud data warehouse ready for BI and analytics. The goal was to practice the full stack covered in the SIC Big Data Engineering track: distributed ingestion, distributed storage, distributed compute, workflow orchestration, and cloud warehousing, rather than any single tool in isolation.

## Architecture

```
                 ┌─────────────┐
Raw files  ───▶  │  Apache     │  ───▶  HDFS staging_zone
(CSV/Parquet/    │  NiFi       │        (/staging_zone/nifi/weather_sales_data/)
 fixed-width)    └─────────────┘
                                              │
                                              ▼
                              ┌───────────────────────────┐
                              │  Spark on YARN             │
                              │  sic_gp_transformations.py │  ── Silver layer (HDFS Parquet)
                              └───────────────────────────┘
                                              │
                                              ▼
                              ┌───────────────────────────┐
                              │  Spark on YARN             │
                              │  sic_gp_apply_modeling.py  │  ── Gold layer (star schema)
                              └───────────────────────────┘
                                              │
                                              ▼
                                        ❄ Snowflake
                                (sales_weather_db.gold)

All stages orchestrated by Apache Airflow (final_project_workflow_fn DAG)
Cluster (HDFS / YARN / Hive metastore / Spark / NiFi / Airflow) run via Docker Compose
```

## Data Sources

| Source | File(s) | Format | What it provides |
|---|---|---|---|
| UK retail sales | `uk_retail_with_city.parquet` | Parquet | Transaction-level sales: product, category, supermarket, price, unit, capture date, city |
| GB city coordinates | `gb.csv` | CSV | UK city names with latitude/longitude |
| NOAA GHCND daily weather | `2024.csv` | CSV (no header) | Daily station observations: element (TMAX/TMIN/PRCP/TAVG), value, flags, date |
| NOAA GHCND station metadata | `ghcnd-stations.txt` | Fixed-width text | Station ID, lat/lon, elevation, state, name |

All four files are staged into HDFS under `/staging_zone/nifi/weather_sales_data/` before the Spark stages begin.

## Pipeline Stages

### 1. Ingestion (NiFi → HDFS)

Raw files are copied into a shared directory that NiFi watches and ingests into HDFS. The Airflow DAG polls HDFS (`wait_for_all_files_in_hdfs`) — up to 20 attempts, 30 seconds apart — until all four expected files are present before letting the pipeline proceed, so a slow or partial NiFi run never lets downstream Spark jobs read incomplete data.

### 2. Transformations (Silver layer)

Implemented in `sic_gp_transformations.py`:

- **Load.** Reads all four sources into Spark DataFrames — the weather station file is fixed-width text, parsed with `substring()` offsets for station ID, latitude, longitude, elevation, state, and name.
- **Data quality pass.** A reusable `analyze_quality()` helper reports null counts/percentages per column and total duplicate rows for each source before any cleaning happens, so cleaning decisions are based on measured data quality rather than assumption.
- **Weather cleaning.** Drops columns with excessive nulls (`M_FLAG`, `Q_FLAG`, `OBS_TIME`), parses the date column, then pivots the long `Element`/`Value` format into wide columns (`TMAX`, `TMIN`, `PRCP`, `TAVG`), converting NOAA's tenths-of-a-unit encoding to real values and backfilling missing `TAVG` from the max/min average when absent.
- **Retail cleaning.** Drops duplicate and null rows (affecting <0.02% of records), derives `quantity` from price ÷ unit price, and adds day-of-week / weekend flags from the capture date.
- **Station filtering.** Restricts weather stations to UK latitude/longitude bounds (49.8–61.1°N, -8.7–2.0°E) and further filters to station IDs prefixed `UK`, discarding the small number of out-of-region records.
- **Geospatial matching.** Every station is paired with every candidate city via a cross join (feasible because the city list is small), and great-circle distance is computed with the **Haversine formula**:

  ```
  a = sin²(Δlat/2) + cos(lat1)·cos(lat2)·sin²(Δlon/2)
  c = 2·asin(√a)
  distance_km = R·c        (R = 6371 km, Earth's mean radius)
  ```

  A window function (`partitionBy("station_id")`, ordered by distance) keeps only the nearest city per station; a second window (`partitionBy("city_clean")`) then picks each city's single best station for the weather join.
- **Join.** Weather (by best station per city) is joined to retail transactions on normalized city name and date, producing one enriched, analysis-ready row per transaction.
- **Output.** Writes two Parquet datasets to HDFS's Silver zone: `df_retail_weather_station` (the joined fact-level data) and `df_city_best_station` (the city→station lookup), both under `hdfs://namenode:9000/silver/`.

### 3. Modeling (Gold layer → Snowflake)

Implemented in `sic_gp_apply_modeling.py`:

- Reads both Silver-layer Parquet datasets back from HDFS.
- Builds five Kimball-style warehouse tables (see [Star Schema](#star-schema) below), generating surrogate keys with `row_number()` over deterministic window orderings.
- Writes all five tables to **Snowflake** (`sales_weather_db.gold` schema) via the Spark-Snowflake connector (`net.snowflake:spark-snowflake_2.12`), one `overwrite`-mode write per table.

## Orchestration (Airflow DAG)

`final_project_dag.py` defines `final_project_workflow_fn`, a daily-scheduled DAG (`owner: group_17`, 2 retries, 1-minute retry delay) with four tasks run in sequence:

```
send_data_to_nifi >> wait_for_nifi_ingestion >> run_spark_transformations >> run_spark_modeling
```

- `send_data_to_nifi` (BashOperator) — copies the four raw files into NiFi's watched input directory.
- `wait_for_nifi_ingestion` (PythonOperator) — polls HDFS for all four expected files, failing the run if they don't appear within the timeout.
- `run_spark_transformations` (BashOperator) — `spark-submit` on YARN (client mode, Hive metastore configured) running the transformations script.
- `run_spark_modeling` (BashOperator) — `spark-submit` on YARN with the Snowflake connector packages, running the modeling/load script.

Each Spark submit is tuned with modest resource requests (1 driver core/1 GB, 2 executors × 2 cores/2 GB) appropriate for a training-cluster environment.

## Star Schema

| Table | Grain | Key Columns |
|---|---|---|
| `DIM_DATE` | One row per calendar date | `date_id`, year, month, day, day_name, is_weekend |
| `DIM_PRODUCT` | One row per product/category/unit combination | `product_id`, product_name, category_name, unit, price_unit_gbp |
| `DIM_SUPERMARKET` | One row per supermarket | `supermarket_id`, supermarket_name |
| `DIM_STATION` | One row per city, with its nearest weather station | `station_id`, nearest_city, latitude, longitude |
| `FACT_SALES` | One row per transaction | `transc_id`, date_id, product_id, supermarket_id, station_id, quantity, price_gbp, price_unit_gbp, avg/max/min_temp, precipitation, temp_category, rain_category |

`FACT_SALES` joins all four dimension tables, so every transaction carries both its commercial details and the weather conditions in its city on that day — enabling questions like "does rain reduce footfall for category X?" or "how does average temperature correlate with sales of Y?" directly in the warehouse.

## Tech Stack

Apache NiFi · HDFS (Hadoop) · Apache Spark (PySpark, YARN, Hive metastore) · Apache Airflow · Snowflake (Spark-Snowflake connector) · Docker / Docker Compose

## Running the Project

> Cluster services (HDFS, YARN, Hive metastore, NiFi, Airflow, Spark) are defined in `final-project-cluster-docker-compose.yaml`.

1. **Start the cluster:**
   ```bash
   docker compose -f final-project-cluster-docker-compose.yaml up -d
   ```
2. **Configure NiFi** to watch the shared input directory and route ingested files into `/staging_zone/nifi/weather_sales_data/` in HDFS.
3. **Place raw source files** (`2024.csv`, `uk_retail_with_city.parquet`, `ghcnd-stations.txt`, `gb.csv`) under `/data/` so the DAG's first task can stage them for NiFi.
4. **Set Snowflake credentials** used by `sic_gp_apply_modeling.py` (`sfURL`, `sfAccount`, `sfUser`, `sfPassword`, `sfDatabase`, `sfSchema`, `sfWarehouse`) — move these to environment variables / Airflow connections rather than hardcoding before any real deployment.
5. **Trigger the DAG** (`final_project_workflow_fn`) from the Airflow UI, or let the daily schedule run it automatically.
6. **Verify output** by querying the `gold` schema in Snowflake once the DAG completes.

## Repository Contents

- `final_project_dag.py` — Airflow DAG orchestrating the full pipeline
- `sic_gp_transformations.py` — PySpark ingestion, cleaning, geospatial matching, and Silver-layer transformations
- `sic_gp_apply_modeling.py` — PySpark star-schema modeling and Snowflake load (Gold layer)
- `final-project-cluster-docker-compose.yaml` — Docker Compose setup for the big data cluster (HDFS, Spark, Hive, Airflow, NiFi)
- `Final project document V2.docx` — written project report
- `SIC_GP_Presentation.pptx` — capstone presentation slides

## Data Quality Notes

- The retail source file is named with a `.parquet` extension but the underlying content structure was verified rather than assumed — a reminder that file extensions in ingested data should never be trusted blindly.
- Retail nulls/duplicates affected under 0.02% of rows, so they were simply dropped rather than imputed.
- Weather metadata columns (`M_FLAG`, `Q_FLAG`, `OBS_TIME`) were dropped for exceeding a 90% null threshold.
- Station coordinates were bounded to UK latitude/longitude ranges *and* filtered to `UK`-prefixed station IDs as a second, independent check — catching the small number of stations that passed the coordinate filter but weren't actually UK stations.

## Possible Extensions

- Parameterize Snowflake credentials via Airflow Connections/Variables instead of inline dictionaries.
- Add data-quality assertions (e.g. Great Expectations) as explicit Airflow tasks between Silver and Gold stages.
- Incorporate additional weather elements (wind, humidity) if available in the NOAA source.
- Add a BI layer (dashboard) on top of the Snowflake gold schema to visualize weather-sales correlations.

# E-Commerce Medallion Lakehouse & Spark Declarative Pipeline (SDP)

End-to-End E-Commerce Data Engineering project built on **Databricks, PySpark, Spark SQL, and Delta Lake** using **Unity Catalog** for data governance and PII protection.

## High-Level Architecture

[./High-Level%20Diagram%20Architecture.jfif]

## Project Overview

This pipeline processes relational e-commerce datasets across 4 tables (`person`, `product`, `order`, `orderline`) through a Multi-Hop Medallion Architecture:

### 1. Ingest (Bronze Layer)
- Built both manual prototypes and a dynamic configuration-driven framework (`@dataclass BronzeLayer`) reading from `pipeline_config_table`.
- Ingested raw CSV files from Unity Catalog Volumes using **Spark Structured Streaming (`readStream` + `foreachBatch` with `availableNow=True`)**.
- Added metadata control columns (`_load_dt`, `_load_dttm`, `_file_name`, `_file_path`, `_file_size`, `_mod_time`) for data lineage.
- Protected sensitive PII columns (`email`, `phone`) in the `person` table using **Databricks Secret Scope**, **AES-256 GCM encryption (`aes_encrypt`)**, and **`xxhash64` hashing**.
- Enabled **Change Data Feed (CDF)** and **Liquid Clustering (`CLUSTER BY`)** on Delta tables.

### 2. Process & Data Quality (Silver Layer)
- Extracted incremental changes from Bronze using Change Data Feed (`get_change_from_cdf`).
- Implemented a 3-stage Data Quality validation pipeline (`@dataclass SilverLayer`):
  1. **Key Null Check:** Validated primary business keys.
  2. **Invalid Format Check:** Verified numeric ranges, dates, and email patterns using Regex.
  3. **Duplicate Check:** Handled exact row duplicates (`row_dup`) prior to conflicting key duplicates (`key_dup`).
- Used `.melt()` wide-to-long transformation to collect error reasons and isolated malformed records into quarantine tables (`*_bad_rec`).
- Filtered clean records via `left_anti` join, standardized text formatting and phone numbers, and cast data types into `*_silver` tables.

### 3. Store, Model & Analyze (Gold Layer - SDP)
- Orchestrated the Gold layer using **Apache Spark Declarative Pipelines (SDP / Lakeflow)**:
  - **Fact Streaming Tables:** Loaded `fact_orders` and `fact_orderlines` via `@dp.append_flow` with data quality expectations.
  - **Auto CDC SCD Type 2 Dimensions:** Tracked historical changes on `dim_person` and `dim_product` using `dp.create_auto_cdc_flow(stored_as_scd_type=2)` with automated `__START_AT` and `__END_AT` columns.
  - **Materialized Views:** Joined active records (`__END_AT IS NULL`) into `all_sales` and aggregated business KPIs into `total_sales_per_city` and `total_sales_per_category` for BI reporting.

### 4. Data Governance (Unity Catalog)
- Created a SQL UDF (`mask_cii`) and applied **Dynamic Column Masking (`SET MASK`)** on `person_silver` (`email`, `phone`) so non-admin users see `'*******'` while authorized admins can access the actual values.

## Repository Structure

- `Ecommerce-medallion-lakehouse/Bronze/` — `register.py`, `bronze_Manual.py`, `bronze_Dynamic.py`
- `Ecommerce-medallion-lakehouse/Silver/` — `Silver_Manual.py`, `Silver_Dynamic.py`
- `Ecommerce-medallion-lakehouse/Gold_Layer/` — `Gold.py` (Spark Declarative Pipeline code & queries)
- `Ecommerce-medallion-lakehouse/source/` — Raw e-commerce CSV files (`person.csv`, `product.csv`, `order.csv`, `orderline.csv`)

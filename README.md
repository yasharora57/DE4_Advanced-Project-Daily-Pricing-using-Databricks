# Project-Daily-Pricing-Analytics

## Daily Pricing Data Engineering 🛒📈☁️

### 📑 Contents:
- **Code**
- **Project Resources:** Data Model, Preview Data Files
- **Project Architecture**

The **Daily Pricing Data Engineering Project** is designed to ingest, transform, and store daily pricing information of market products for the year 2023 using the **Lakehouse Medallion Architecture** in **Databricks**, integrated with **Azure Data Lake Gen2 (ADLS)** and **Unity Catalog**. The project supports use cases for both **Business Intelligence (BI)** and **Machine Learning (ML)** by sourcing data from **REST APIs** and **SQL Server**.

---

## ✅ Highlights

- ✅ **End-to-End Lakehouse Project** for both **BI** and **ML**
- ✅ **Medallion Architecture**: Bronze → Silver → Gold
- ✅ **Incremental Loads** with log-based filtering
- ✅ **SCD Type-2 Implementation** using staging + merge
- ✅ **Star Schema Modeling** for analytics
- ✅ **Enriched ML Table** combining pricing, weather, geo data

---

### 🚀 Key Features:

- **Tech Stack:** Databricks, ADLS Gen-2, Unity Catalog, PySpark, SQL, JDBC, Delta Lake, REST APIs

- **Data Storage:**
  - **Bronze Layer:** Raw data ingestion (REST APIs, SQL Server)
  - **Silver Layer:** Cleaned and typed tables with added metadata (CDC ready)
  - **Gold Layer:** Star schema (Fact & Dim tables), enriched with SCD Type-2 and ML features

> **Note:** 
> - **Incremental Ingestion:** Implemented for daily pricing data using a log table
> - **SCD Type-2:** Applied for all dimension tables using staging logic
> - **Dual Usage:** Supports both analytical dashboards and predictive models

---

### 🔧 Architecture Setup & Configuration for the Unity Catalog

#### 📌 Unity Catalog & External Storage Configuration:

1. **Attach ADLS to Databricks via Unity Catalog Access Connector**
   - Create an *Azure Databricks Access Connector*
   - Assign role: `Storage Blob Data Contributor` to the Access Connector
   - Register Access Connector in Databricks > External Data > Credentials

2. **Create External Locations in Unity Catalog**
   - One external location for each **Bronze**, **Silver**, **Gold** container
   - Another external location for **Managed Tables** under a container named `pricing-analytics`

3. **Set up Unity Metastore**
   - Optional: Assign a default managed location
   - Assign catalog (`pricing_analytics`) to external managed container path

---

### 📋 Process Logging

Centralized Log Table: `processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS`

- Tracks process status per table
- Records latest processed date per run
- Used for filtering new data in all layers:
  ```sql
  CREATE TABLE IF NOT EXISTS processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS(
  PROCESS_NAME STRING,
  PROCESSED_FILE_TABLE_DATE DATE,
  PROCESS_STATUS STRING,
  PROCESSED_TABLE_DATETIME TIMESTAMP
  )
  ```

---

### 🔄 Data Ingestion Workflow (HTTPS Source - Denormalized Fact Table)

1. **Ingestion Metadata Management**
   - Created a **log table** to track latest ingested date
   - Source/Sink paths parameterized for flexibility

2. **Ingestion Logic**
   - Read JSON data using `pandas`, convert to Spark DataFrame
   - Append ingested records to **bronze** container in ADLS Gen-2
   - Create an external table in the bronze schema `bronze.daily_pricing`

3. **Data Characteristics**
   - One JSON file per day ingested from the web API
   - Year-long (2023) data retrieved incrementally using the log

---

### 🗃️ Reference Data (SQL Server via JDBC - Lookup/Reference Tables)

- 5 lookup/reference tables ingested:
  - One-time full load stored under `bronze/reference-data` in ADLS Gen-2
  - Connected using **JDBC Connector**
  - Triggered manually only once via **parameterized workflow job**
  - Designed for yearly refresh

---

### 🌐 Geo Location API (External REST API Integration - For ML implementation)

- API Source: [`https://geocoding-api.open-meteo.com`]([https://geocoding-api.open-meteo.com](https://geocoding-api.open-meteo.com/v1/search?name=kovilpatti&count=10&language=en&format=json))
- Purpose: To enrich **market** data with **geographical attributes**
- Key Fields Extracted:
  - `latitude`, `longitude`, `population`
- Data is stored in: `geo-location` folder under bronze container
- Processing:
  - Flattened nested arrays and normalized schema using PySpark
  - Stored as structured Delta tables in **Silver Layer** for analytical use -`silver.geo_location_silver`
  - Joined to **market-level** pricing data in **Gold Layer** using state/market name

---

### 🌦️ Weather Data API (External REST API Integration - For ML implementation)

- API Source: [`https://archive-api.open-meteo.com`]([https://archive-api.open-meteo.com](https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2023-01-01&end_date=2024-01-01&daily=temperature_2m_max,temperature_2m_min,rain_sum))
- Purpose: To augment pricing data with **daily weather metrics**
- Key Fields Extracted:
  - `temperature_2m_max`, `temperature_2m_min`, `rain_sum`
- Data is stored in: `weather-data` folder under Bronze layer
- Processing:
  - Transformed date-indexed array structure to tabular format saved in the silver schema - `silver.weather_data_silver`
  - Enriched pricing fact table in **Gold Layer** with:
    - Temperature Unit, Min/Max Temperature, Rainfall Unit, Rainfall Amount
- Final Enriched Table: `PRICE_PREDICTION_GOLD` (used for ML modeling)

---


### 🧾 Fact & Dim Table Structures (Gold Layer)

#### 📌 Dimensions:

- `REPORTING_DIM_DATE_GOLD`
- `REPORTING_DIM_STATE_GOLD`
- `REPORTING_DIM_MARKET_GOLD`
- `REPORTING_DIM_PRODUCT_GOLD_SCDTYPE2`
- `REPORTING_DIM_VARIETY_GOLD`

Each includes:
- Business attributes
- Surrogate Keys (e.g. `STATE_ID`, `PRODUCT_ID`)
- Metadata (`inserted_date`, `updated_date`)

#### 📌 Fact Table:

`REPORTING_FACT_DAILY_PRICING_GOLD`

Includes:
- `DATE_ID`, `STATE_ID`, `MARKET_ID`, `PRODUCT_ID`, `VARIETY_ID`
- `ARRIVAL_IN_TONNES`, `MINIMUM_PRICE`, `MAXIMUM_PRICE`, `MODAL_PRICE`

---

### ⚙️ Data Modelling & SCD Type-2 Implementation for Dimension Tables

- Implemented using **intermediate staging tables**:
  - `reporting_dim_product_stage_1`, `_stage_2`, `_stage_3`
- Surrogate keys generated via `ROW_NUMBER()` + max key logic
- **Merge Operation** used to update `end_date` on change
- Inserts new records or changed rows with updated start dates

---

### 🧱 Medallion Architecture

#### **Bronze Layer** (Raw Zone):
- Stores all API and JDBC source data as-is
- Tables: `daily_pricing_raw`, `geo_location_raw`, `weather_data_raw`, `reference_data_json`

#### **Silver Layer** (Cleaned Zone):
- Clean schema with proper data types
- Adds metadata: `lakehouse_inserted_date`, `lakehouse_updated_date`
- Implements CDC logic with log-based filters
- Stores `daily_pricing_silver`, `geo_location_silver`, `weather_data_silver`, and lookup tables

#### **Gold Layer** (Business Zone):
- Dimension and Fact tables created using **Star Schema**
- Implements **SCD Type-2** for all dimensions using staging logic
- Fact Table: `reporting_fact_daily_pricing_gold`
- Enriched Table: `price_prediction_gold` (for ML)

---

## 🔧 How to Use

1. **Create Azure Resources**
   - ADLS Gen2 with containers: `bronze`, `silver`, `gold`, `pricing-analytics`
   - Azure Databricks workspace
   - Unity Catalog + Metastore + Access Connector

2. **Configure Unity Catalog**
   - Assign external locations
   - Register credentials and sources

3. **Run Ingestion Logic**
   - Read HTTPS APIs via notebooks
   - Store in Bronze layer
   - Track dates via log table

4. **Run Transformation Pipelines**
   - Clean & cast data → Silver layer
   - Build star schema → Gold layer
   - Execute SCD logic on dim tables

5. **(Optional)** ML Pipeline
   - Extract Geo & Weather data
   - Create `price_prediction_gold` for downstream ML tasks

---

This project demonstrates a **modern lakehouse architecture** using **Databricks + ADLS + Unity Catalog**, built for scalable **incremental ingestion**, **star schema modeling**, **SCD-2 implementation**, and **ML feature engineering**. 🎯

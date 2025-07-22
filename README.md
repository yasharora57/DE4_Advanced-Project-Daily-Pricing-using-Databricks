# Project-Daily-Pricing-Analytics

## Daily Pricing Data Engineering 🛒📈☁️

### 📑 Contents:
- **Code**
- **Project Resources:** Data Model, Preview Data Files
- **Project Architecture**

The **Daily Pricing Data Engineering Project** is designed to ingest, transform, and store daily pricing information of market products for the year 2023 using the **Lakehouse Medallion Architecture** in **Databricks**, integrated with **Azure Data Lake Gen2 (ADLS)** and **Unity Catalog**. The project supports use cases for both **Business Intelligence (BI)** and **Machine Learning (ML)** by sourcing data from **REST APIs** and **SQL Server**.

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

### 🔧 Architecture Setup & Configuration

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

### 🔄 Data Ingestion Workflow (HTTPS Source)

1. **Ingestion Metadata Management**
   - Created a **log table** to track latest ingested date
   - Source/Sink paths parameterized for flexibility

2. **Ingestion Logic**
   - Read JSON data using `pandas`, convert to Spark DataFrame
   - Append ingested records to **Bronze Layer**
   - Update log table with latest processed date

3. **Data Characteristics**
   - One JSON file per day ingested from the web API
   - Year-long (2023) data retrieved incrementally using the log

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

### 🗃️ Reference Data (SQL Server via JDBC)

- Five lookup/reference tables ingested:
  - One-time full load stored under `bronze/reference-data`
  - Connected using **JDBC Connector**
  - Loaded manually or triggered via **parameterized notebooks**
  - Designed for yearly refresh

---

### ⚙️ SCD Type-2 Implementation for Dimension Tables

- Implemented using **intermediate staging tables**:
  - `reporting_dim_product_stage_1`, `_stage_2`, `_stage_3`
- Surrogate keys generated via `ROW_NUMBER()` + max key logic
- **Merge Operation** used to update `end_date` on change
- Inserts new records or changed rows with updated start dates

Example (Product Dimension):

```sql
MERGE INTO gold.reporting_dim_product_gold_SCDTYPE2 goldDim
USING silver.reporting_dim_product_stage_3 silverDim
ON goldDim.PRODUCT_ID = silverDim.GOLD_PRODUCT_ID
WHEN MATCHED THEN 
  UPDATE SET goldDim.end_date = current_timestamp(),
             goldDim.lakehouse_updated_date = current_timestamp()
WHEN NOT MATCHED THEN 
  INSERT (PRODUCTGROUP_NAME, PRODUCT_NAME, PRODUCT_ID, start_date, end_date, lakehouse_inserted_date, lakehouse_updated_date)
  VALUES (PRODUCTGROUP_NAME, PRODUCT_NAME, PRODUCT_ID, current_timestamp(), NULL, current_timestamp(), current_timestamp())
```

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

### 📦 ML Feature Engineering

1. **Geo Location Enrichment**
   - API: `https://geocoding-api.open-meteo.com`
   - Extracts: `latitude`, `longitude`, `population`

2. **Weather Data Integration**
   - API: `https://archive-api.open-meteo.com`
   - Extracts: `temperature_2m_max/min`, `rain_sum`

3. **Final ML Table:** `PRICE_PREDICTION_GOLD`

Combines:
- Pricing Data
- Weather Info
- Geo Location Info

```sql
CREATE TABLE gold.PRICE_PREDICTION_GOLD AS
SELECT ...
FROM silver.daily_pricing_silver
JOIN silver.geo_location_silver
JOIN silver.weather_data_silver
...
```

---

### 📋 Process Logging

Centralized Log Table: `processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS`

- Tracks process status per table
- Records latest processed date per run
- Used for filtering new data in all layers:
  ```sql
  WHERE lakehouse_updated_date > (
    SELECT NVL(MAX(PROCESSED_TABLE_DATETIME), '1900-01-01') 
    FROM processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
    WHERE process_name = '...' AND process_status = 'Completed'
  )
  ```

---

## ✅ Highlights

- ✅ **End-to-End Lakehouse Project** for both **BI** and **ML**
- ✅ **Medallion Architecture**: Bronze → Silver → Gold
- ✅ **Incremental Loads** with log-based filtering
- ✅ **SCD Type-2 Implementation** using staging + merge
- ✅ **Star Schema Modeling** for analytics
- ✅ **Enriched ML Table** combining pricing, weather, geo data

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

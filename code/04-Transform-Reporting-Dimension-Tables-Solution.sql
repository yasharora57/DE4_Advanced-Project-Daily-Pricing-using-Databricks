-- Databricks notebook source
USE CATALOG pricing_analytics;

CREATE OR REPLACE TABLE silver.reporting_dim_state_stage_1 AS
SELECT 
 DISTINCT STATE_NAME
FROM silver.daily_pricing_silver
WHERE lakehouse_updated_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'reportingDimensionTablesLoad' AND process_status = 'Completed' )


-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_state_stage_2 AS 
SELECT 
  silverDim.STATE_NAME
 ,ROW_NUMBER() OVER (  ORDER BY silverDim.STATE_NAME)  as STATE_ID
 ,current_timestamp() as lakehouse_inserted_date
 ,current_timestamp() as lakehouse_updated_date
FROM silver.reporting_dim_state_stage_1 silverDim
LEFT OUTER JOIN gold.reporting_dim_state_gold goldDim
ON silverDim.STATE_NAME = goldDim.STATE_NAME
WHERE goldDim.STATE_NAME IS NULL

-- COMMAND ----------


CREATE OR REPLACE TABLE silver.reporting_dim_state_stage_3 AS
SELECT
silverDim.STATE_NAME 
,silverDim.STATE_ID + PREV_MAX_SK_ID as STATE_ID
,current_timestamp() as lakehouse_inserted_date
,current_timestamp() as lakehouse_updated_date
FROM 
silver.reporting_dim_state_stage_2 silverDim
CROSS JOIN (SELECT NVL(MAX(STATE_ID),0) as PREV_MAX_SK_ID FROM gold.reporting_dim_state_gold ) goldDim

-- COMMAND ----------

INSERT INTO gold.reporting_dim_state_gold
SELECT
STATE_NAME
,STATE_ID
,current_timestamp()
,current_timestamp() 
From silver.reporting_dim_state_stage_3

-- COMMAND ----------

USE CATALOG pricing_analytics;
CREATE OR REPLACE TABLE silver.reporting_dim_market_stage_1 AS
SELECT 
 DISTINCT MARKET_NAME
FROM silver.daily_pricing_silver
WHERE lakehouse_updated_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'reportingDimensionTablesLoad' AND process_status = 'Completed' )

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_market_stage_2 AS 
SELECT 
  silverDim.MARKET_NAME
 ,ROW_NUMBER() OVER (  ORDER BY silverDim.MARKET_NAME)  as MARKET_ID
 ,current_timestamp() as lakehouse_inserted_date
 ,current_timestamp() as lakehouse_updated_date
FROM silver.reporting_dim_market_stage_1 silverDim
LEFT OUTER JOIN gold.reporting_dim_market_gold goldDim
ON silverDim.MARKET_NAME = goldDim.MARKET_NAME
WHERE goldDim.MARKET_NAME IS NULL;


-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_market_stage_3 AS 
SELECT
silverDim.MARKET_NAME 
,silverDim.MARKET_ID + PREV_MAX_SK_ID as MARKET_ID
,current_timestamp() as lakehouse_inserted_date
,current_timestamp() as lakehouse_updated_date
FROM 
silver.reporting_dim_market_stage_2 silverDim
CROSS JOIN (SELECT NVL(MAX(MARKET_ID),0) as PREV_MAX_SK_ID FROM gold.reporting_dim_market_gold ) goldDim;

-- COMMAND ----------

INSERT INTO gold.reporting_dim_market_gold
SELECT
MARKET_NAME
,MARKET_ID
,current_timestamp() 
,current_timestamp() 
FROM silver.reporting_dim_market_stage_3;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_variety_stage_1 AS
SELECT 
 DISTINCT VARIETY
FROM silver.daily_pricing_silver
WHERE lakehouse_updated_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'reportingDimensionTablesLoad' AND process_status = 'Completed' );


-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_variety_stage_2 AS 
SELECT 
  silverDim.VARIETY
 ,ROW_NUMBER() OVER (  ORDER BY silverDim.VARIETY)  as VARIETY_ID
 ,current_timestamp() as lakehouse_inserted_date
 ,current_timestamp() as lakehouse_updated_date
FROM silver.reporting_dim_variety_stage_1 silverDim
LEFT OUTER JOIN gold.reporting_dim_variety_gold goldDim
ON silverDim.VARIETY= goldDim.VARIETY
WHERE goldDim.VARIETY IS NULL;

-- COMMAND ----------

CREATE OR REPLACE TABLE silver.reporting_dim_variety_stage_3 AS 
SELECT
silverDim.VARIETY 
,silverDim.VARIETY_ID + PREV_MAX_SK_ID as VARIETY_ID
,PREV_MAX_SK_ID
,current_timestamp() as lakehouse_inserted_date
,current_timestamp() as lakehouse_updated_date
FROM 
silver.reporting_dim_variety_stage_2 silverDim
CROSS JOIN (SELECT nvl(MAX(VARIETY_ID),0) as PREV_MAX_SK_ID FROM gold.reporting_dim_variety_gold ) goldDim;



-- COMMAND ----------

INSERT INTO gold.reporting_dim_variety_gold
SELECT
VARIETY
,VARIETY_ID
,current_timestamp() 
,current_timestamp() 
FROM silver.reporting_dim_variety_stage_3

-- COMMAND ----------

INSERT INTO  processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS(PROCESS_NAME,PROCESSED_TABLE_DATETIME,PROCESS_STATUS)
SELECT 'reportingDimensionTablesLoad' , max(lakehouse_updated_date) ,'Completed' FROM silver.daily_pricing_silver
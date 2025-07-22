-- Databricks notebook source
USE CATALOG pricing_analytics;
INSERT INTO  silver.daily_pricing_silver
SELECT
to_date(DATE_OF_PRICING,'dd/MM/yyyy'),
cast(ROW_ID as bigint) ,
STATE_NAME,
MARKET_NAME,
PRODUCTGROUP_NAME,
PRODUCT_NAME,
VARIETY,
ORIGIN,
cast(ARRIVAL_IN_TONNES as decimal(18,2)),
cast(MINIMUM_PRICE as decimal(36,2)),
cast(MAXIMUM_PRICE as decimal(36,2)),
cast(MODAL_PRICE as decimal(36,2)),
source_file_load_date ,
current_timestamp(),
current_timestamp()
FROM pricing_analytics.bronze.daily_pricing
WHERE source_file_load_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'daily_pricing_silver' AND process_status = 'Completed' )


-- COMMAND ----------

INSERT INTO  pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS(PROCESS_NAME,PROCESSED_TABLE_DATETIME,PROCESS_STATUS)
SELECT 'daily_pricing_silver' , max(source_file_load_date) ,'Completed' FROM pricing_analytics.silver.daily_pricing_silver;
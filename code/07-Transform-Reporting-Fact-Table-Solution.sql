-- Databricks notebook source
USE CATALOG pricing_analytics;
INSERT INTO gold.reporting_fact_daily_pricing_gold
SELECT
dateDim.DATE_ID
,stateDIM.STATE_ID
,marketDim.MARKET_ID
,productDim.PRODUCT_ID
,varieytyDim.VARIETY_ID
,silverFact.ROW_ID
,silverFact.ARRIVAL_IN_TONNES
,silverFact.MAXIMUM_PRICE
,silverFact.MINIMUM_PRICE
,silverFact.MODAL_PRICE
,current_timestamp()
,current_timestamp()
FROM silver.daily_pricing_silver silverFact
LEFT OUTER JOIN gold.reporting_dim_date_gold dateDim
on silverFact.DATE_OF_PRICING = dateDim.CALENDAR_DATE
LEFT OUTER JOIN gold.reporting_dim_state_gold stateDim
on silverFact.STATE_NAME = stateDim.STATE_NAME
LEFT OUTER JOIN gold.reporting_dim_market_gold marketDim
on silverFact.MARKET_NAME = marketDim.MARKET_NAME
LEFT OUTER JOIN gold.reporting_dim_product_gold productDim
on silverFact.PRODUCT_NAME = productDim.PRODUCT_NAME
AND silverFact.PRODUCTGROUP_NAME = productDim.PRODUCTGROUP_NAME
AND productDim.end_date = NULL
LEFT OUTER JOIN gold.reporting_dim_variety_gold varieytyDim
on silverFact.VARIETY = varieytyDim.VARIETY
WHERE silverFact.lakehouse_updated_date > (SELECT nvl(max(PROCESSED_TABLE_DATETIME),'1900-01-01') FROM processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE process_name = 'reportingFactTableLoad' AND process_status = 'Completed' )

-- COMMAND ----------

INSERT INTO  processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS(PROCESS_NAME,PROCESSED_TABLE_DATETIME,PROCESS_STATUS)
SELECT 'reportingFactTableLoad' , max(lakehouse_updated_date) ,'Completed' FROM silver.daily_pricing_silver
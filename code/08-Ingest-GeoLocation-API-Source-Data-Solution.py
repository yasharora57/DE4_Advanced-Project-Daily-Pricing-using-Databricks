# Databricks notebook source
#geoLocationSourceAPIURL = "https://geocoding-api.open-meteo.com/v1/search?name=kovilpatti&count=10&language=en&format=json"

geoLocationSourceAPIBaseURL = "https://geocoding-api.open-meteo.com/v1/search?name="
geoLocationSourceAPIURLOptions = "&count=10&language=en&format=json"

geoLocationSinkLayerName = 'bronze'
geoLocationSinkStorageAccountName = 'adlsudadatalakehousedev'
geoLocationSinkFolderName = 'geo-location'

geoLocationSinkFolderPath = f"abfss://{geoLocationSinkLayerName}@{geoLocationSinkStorageAccountName}.dfs.core.windows.net/{geoLocationSinkFolderName}"

# COMMAND ----------

import requests
import json
import pandas as pds

# COMMAND ----------

geoLocationAPIResponse = requests.get(geoLocationSourceAPIURL).json()
geoLocationPandasDF = pds.DataFrame(geoLocationAPIResponse)
geoLocationSparkDF = spark.createDataFrame(geoLocationPandasDF)

# COMMAND ----------

dailyPricingMarketNamesDF = spark.sql("SELECT MARKET_NAME from pricing_analytics.gold.reporting_dim_market_gold ")

# COMMAND ----------

from pyspark.sql.types import *
marketNames = [dailyPricingMarketNames["MARKET_NAME"] for dailyPricingMarketNames in dailyPricingMarketNamesDF.collect()]


geoLocationAPIResponseList = []
for marketName in marketNames:
  
  geoLocationSourceAPIURL = f"{geoLocationSourceAPIBaseURL}{marketName}{geoLocationSourceAPIURLOptions}"
  geoLocationAPIResponse = requests.get(geoLocationSourceAPIURL).json()
  
  
  if isinstance(geoLocationAPIResponse, dict):
      geoLocationAPIResponseList.append(geoLocationAPIResponse)    


geoLocationSparkRDD =sc.parallelize(geoLocationAPIResponseList)

geoLocationSparkDF = spark.read.json(geoLocationSparkRDD)

(geoLocationSparkDF
.filter("results.admin1 IS NOT NULL")
.write
.mode("overwrite")
.json(geoLocationSinkFolderPath))
      


# COMMAND ----------

from pyspark.sql.functions import col, array_contains
geoLocationBronzeDF = (spark
                       .read
                                             .json(geoLocationSinkFolderPath))
                       

display(geoLocationBronzeDF)
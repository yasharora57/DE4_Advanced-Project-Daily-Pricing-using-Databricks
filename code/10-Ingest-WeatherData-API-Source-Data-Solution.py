# Databricks notebook source
#weatherDataSourceAPIURL = "https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2023-01-01&end_date=2024-01-01&daily=temperature_2m_max,temperature_2m_min,rain_sum"

weatherDataSourceAPIBaseURL = "https://archive-api.open-meteo.com/v1/archive?latitude="
weatherDataSourceAPIURLOptions = "&daily=temperature_2m_max,temperature_2m_min,rain_sum"

weatherDataSinkLayerName = 'bronze'
weatherDataSinkStorageAccountName = 'adlsudadatalakehousedev'
weatherDataSinkFolderName = 'weather-data'

weatherDataSinkFolderPath = f"abfss://{weatherDataSinkLayerName}@{weatherDataSinkStorageAccountName}.dfs.core.windows.net/{weatherDataSinkFolderName}"

# COMMAND ----------

import requests
import json
import pandas as pds

# COMMAND ----------

geoLocationsDF = spark.sql("SELECT latitude,longitude,marketName from pricing_analytics.silver.geo_location_silver LIMIT 100 ")
display(geoLocationsDF.count())

# COMMAND ----------


weatherDataAPIResponseList = []
for geoLocations in geoLocationsDF.collect():
  #print(geoLocations["marketName"],geoLocations["latitude"],geoLocations["longitude"])
  weatherDataSourceAPIURL = f"{weatherDataSourceAPIBaseURL}{geoLocations['latitude']}&longitude={geoLocations['longitude']}&start_date=2023-01-01&end_date=2023-12-31{weatherDataSourceAPIURLOptions}"
  weatherDataAPIResponse = requests.get(weatherDataSourceAPIURL).json()
  weatherDataAPIResponse["marketName"] = geoLocations["marketName"]
  weatherDataAPIResponseJson = json.dumps(weatherDataAPIResponse)
  #print(weatherDataAPIResponseJson)
  if isinstance(weatherDataAPIResponse, dict):
   weatherDataAPIResponseList.append(weatherDataAPIResponseJson)

weatherDataRDD = sc.parallelize(weatherDataAPIResponseList)

weatherDataSparkDF = spark.read.json(weatherDataRDD)

(weatherDataSparkDF
.write
.mode("overwrite")
.json(weatherDataSinkFolderPath))

  
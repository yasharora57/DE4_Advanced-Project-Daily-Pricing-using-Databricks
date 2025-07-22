# Databricks notebook source
weatherDataSourceLayerName = 'bronze'
weatherDataSourceStorageAccountName = '<datalakestorageaccountname>'
weatherDataSourceFolderName = 'weather-data'

weatherDataSourceFolderPath = f"abfss://{weatherDataSourceLayerName}@{weatherDataSourceStorageAccountName}.dfs.core.windows.net/{weatherDataSourceFolderName}"

# COMMAND ----------

weatherDataBronzeDF = (spark
                       .read
                       .json(weatherDataSourceFolderPath))

display(weatherDataBronzeDF)

# COMMAND ----------

from pyspark.sql.functions import *
weatherDataDailyDateTransDF = (weatherDataBronzeDF
                          .select(
                          explode("daily.time").alias("weatherDate")
                          ,col("marketName")
                          ,col("latitude").alias("latitude")
                          ,col("longitude").alias("longitude")
                          ,monotonically_increasing_id().alias('sequenceId')
                          ))

display(weatherDataDailyDateTransDF)

# COMMAND ----------

weatherDataMaxTemparatureTransDF = (weatherDataBronzeDF
                          .select(
                          explode("daily.temperature_2m_max").alias("maximumTemparature")
                          ,col("marketName")
                          ,col("latitude").alias("latitude")
                          ,col("longitude").alias("longitude")
                          ,monotonically_increasing_id().alias('sequenceId')
                          ,col("daily_units.temperature_2m_max").alias("unitOfTemparature")

                          ))

display(weatherDataMaxTemparatureTransDF)

# COMMAND ----------

weatherDataMinTemparatureTransDF = (weatherDataBronzeDF
                          .select(
                          explode("daily.temperature_2m_min").alias("minimumTemparature")
                          ,col("marketName")
                          ,col("latitude").alias("latitude")
                          ,col("longitude").alias("longitude")                          
                          ,monotonically_increasing_id().alias('sequenceId')

                          ))

display(weatherDataMinTemparatureTransDF)

# COMMAND ----------

weatherDataRainFallTransDF = (weatherDataBronzeDF
                          .select(
                          explode("daily.rain_sum").alias("rainFall")
                          ,col("marketName")
                          ,col("latitude").alias("latitude")
                          ,col("longitude").alias("longitude")                          
                          ,monotonically_increasing_id().alias('sequenceId')
                          ,col("daily_units.rain_sum").alias("unitOfRainFall")

                          ))

display(weatherDataRainFallTransDF)

# COMMAND ----------

weatherDataTransDF = (weatherDataDailyDateTransDF
                      .join(weatherDataMaxTemparatureTransDF, ['marketName','latitude','longitude','sequenceId'])
                      .join(weatherDataMinTemparatureTransDF, ['marketName','latitude','longitude','sequenceId'])
                      .join(weatherDataRainFallTransDF, ['marketName','latitude','longitude','sequenceId'])
                      .select(col("marketName")
                              ,col("weatherDate")
                              ,col("unitOfTemparature")
                              ,col("maximumTemparature")
                              ,col("minimumTemparature")
                              ,col("unitOfRainFall")
                              ,col("rainFall")
                              ,col("latitude")
                              ,col("longitude"))
                     
)

# COMMAND ----------

(weatherDataTransDF 
.write
.mode('overwrite')
.saveAsTable("pricing_analytics.silver.weather_data_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  pricing_analytics.silver.weather_data_silver
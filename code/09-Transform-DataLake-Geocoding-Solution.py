# Databricks notebook source
geoLocationSourceLayerName = 'bronze'
geoLocationSourceStorageAccountName = 'adlsudadatalakehousedev'
geoLocationSourceFolderName = 'geo-location'

geoLocationSourceFolderPath = f"abfss://{geoLocationSourceLayerName}@{geoLocationSourceStorageAccountName}.dfs.core.windows.net/{geoLocationSourceFolderName}"

# COMMAND ----------

geoLocationBronzeDF = (spark
                       .read
                       .json(geoLocationSourceFolderPath))

# COMMAND ----------

display(geoLocationBronzeDF)

# COMMAND ----------

from pyspark.sql.functions import *

geoLocationSilverDF = ( geoLocationBronzeDF
                       .select(col('results.admin1').alias('stateName')
                              ,col('results.admin2').alias('districtName')
                               , col('results.country').alias('countryName')
                               , col('results.latitude').alias('latitude')
                               , col('results.longitude').alias('longitude')
                               , col('results.name').alias('marketName')
                               , col('results.population').alias('population')
)
)

display(geoLocationSilverDF)

# COMMAND ----------

geoLocationStateTransDF = ( geoLocationSilverDF
 .select(explode("stateName")
 ,monotonically_increasing_id().alias('stateSequenceId')))

# COMMAND ----------


geoLocationStateTransDF = ( geoLocationSilverDF
 .select(explode("stateName").alias('stateName')
 ,monotonically_increasing_id().alias('stateSequenceId')))

geoLocationDistrictTransDF = (geoLocationSilverDF
 .select(explode("districtName").alias('districtName')
         ,monotonically_increasing_id().alias('districtSequenceId')
))
geoLocationCountryTransDF = (geoLocationSilverDF
 .select(explode("countryName").alias('countryName')
 ,monotonically_increasing_id().alias('countryNameSequenceId')))

geoLocationLatitudeTransDF = (geoLocationSilverDF
 .select(explode("latitude").alias('latitude')
 ,monotonically_increasing_id().alias('latitudeSequenceId')))

geoLocationLongitudeTransDF = (geoLocationSilverDF
 .select(explode("longitude").alias('longitude')
 ,monotonically_increasing_id().alias('longitudeSequenceId')))

geoLocationMarkeTransDF = (geoLocationSilverDF
 .select(explode("marketName").alias('marketName')
 ,monotonically_increasing_id().alias('marketSequenceId')))

geoLocationPopulationTransDF = (geoLocationSilverDF
 .select(explode("population").alias('population')
 ,monotonically_increasing_id().alias('populationSequenceId')))

# COMMAND ----------

geoLocationSilverTransDF = ( geoLocationStateTransDF
                            .join(geoLocationDistrictTransDF, col("stateSequenceId") == col("districtSequenceId"))
                             .join(geoLocationCountryTransDF, col("stateSequenceId") == col("countryNameSequenceId")) 
                            .join(geoLocationLatitudeTransDF, col("stateSequenceId") == col("latitudeSequenceId")) 
                            .join(geoLocationLongitudeTransDF, col("stateSequenceId") == col("longitudeSequenceId")) 
                            .join(geoLocationMarkeTransDF, col("stateSequenceId") == col("marketSequenceId")) 
                            .join(geoLocationPopulationTransDF, col("stateSequenceId") == col("populationSequenceId"))  
                                 .select(col("stateName")
                                    ,col("districtName")
                                  ,col("countryName")
                                    ,col("latitude")
                                    ,col("longitude")
                                    ,col("marketName")
                                    ,col("population")

                             ) )
display(geoLocationSilverTransDF)                             

# COMMAND ----------

(geoLocationSilverTransDF 
.write
.mode('overwrite')
.saveAsTable("pricing_analytics.silver.geo_location_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT geolocation.*
# MAGIC  FROM pricing_analytics.silver.geo_location_silver geolocation
# MAGIC inner join pricing_analytics.silver.daily_pricing_silver dailyPricing
# MAGIC on geolocation.stateName = dailyPricing.STATE_NAME
# MAGIC and geolocation.marketName = dailyPricing.MARKET_NAME
# MAGIC where geolocation.countryName = 'India'
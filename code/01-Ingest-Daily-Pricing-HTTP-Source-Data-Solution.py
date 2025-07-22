# Databricks notebook source
processName = dbutils.widgets.get('prm_processName')

nextSourceFileDateSql = f"""SELECT NVL(MAX(PROCESSED_FILE_TABLE_DATE)+1,'2023-01-01')  as NEXT_SOURCE_FILE_DATE FROM pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS 
WHERE PROCESS_NAME = '{processName}' and PROCESS_STATUS='Completed'"""


nextSourceFileDateDF = spark.sql(nextSourceFileDateSql)
display(nextSourceFileDateDF)



# COMMAND ----------

from datetime import datetime


# COMMAND ----------


dailyPricingSourceBaseURL = 'https://retailpricing.blob.core.windows.net/'
dailyPricingSourceFolder = 'daily-pricing/'
dailyPricingSourceFileDate = datetime.strptime(str(nextSourceFileDateDF.select('NEXT_SOURCE_FILE_DATE').collect()[0]['NEXT_SOURCE_FILE_DATE']),'%Y-%m-%d').strftime('%m%d%Y')
dailyPricingSourceFileName = f"PW_MW_DR_{dailyPricingSourceFileDate}.csv"


dailyPricingSinkLayerName = 'bronze'
dailyPricingSinkStorageAccountName = 'adlsudadatalakehousedev'
dailyPricingSinkFolderName =  'daily-pricing'



# COMMAND ----------

import pandas as pds

# COMMAND ----------

dailyPricingSourceURL = dailyPricingSourceBaseURL + dailyPricingSourceFolder + dailyPricingSourceFileName
print(dailyPricingSourceURL)

dailyPricingPandasDF = pds.read_csv(dailyPricingSourceURL)
print(dailyPricingPandasDF)


# COMMAND ----------

dailyPricingSparkDF =  spark.createDataFrame(dailyPricingPandasDF)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
dailyPricingSinkFolderPath = f"abfss://{dailyPricingSinkLayerName}@{dailyPricingSinkStorageAccountName}.dfs.core.windows.net/{dailyPricingSinkFolderName}"


(
    dailyPricingSparkDF
    .withColumn("source_file_load_date",current_timestamp())
    .write
    .mode("append")
    .option("header","true")
    .csv(dailyPricingSinkFolderPath)

)


# COMMAND ----------


processFileDate = nextSourceFileDateDF.select('NEXT_SOURCE_FILE_DATE').collect()[0]['NEXT_SOURCE_FILE_DATE']
processStatus ='Completed'

processInsertSql = f""" INSERT INTO pricing_analytics.processrunlogs.DELTALAKEHOUSE_PROCESS_RUNS(PROCESS_NAME,PROCESSED_FILE_TABLE_DATE,PROCESS_STATUS) VALUES('{processName}','{processFileDate}','{processStatus}')"""

spark.sql(processInsertSql)



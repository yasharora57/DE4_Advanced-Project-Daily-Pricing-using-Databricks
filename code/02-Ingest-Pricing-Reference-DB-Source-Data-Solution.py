# Databricks notebook source
pricingReferenceSourceTableName = dbutils.widgets.get("prm_pricingReferenceSourceTableName")

pricingReferenceSinkLayerName = 'bronze'
pricingReferenceSinkStorageAccountName = 'adlsudadatalakehousedev'
pricingReferenceSinkFolderName = 'reference-data'

# COMMAND ----------


JDBCconnectionUrl = "jdbc:sqlserver://asqludacoursesserver.database.windows.net;encrypt=true;databaseName=asqludacourses;user=sourcereader;password=DBReader@2024";

print(JDBCconnectionUrl)



pricingReferenceSourceTableDF =  (spark
                                 .read
                                 .format('jdbc')
                                 .option("url",JDBCconnectionUrl)
                                 .option("dbtable",pricingReferenceSourceTableName)
                                 .load()
)



# COMMAND ----------

pricingReferenceSinkTableFolder = pricingReferenceSourceTableName.replace('.','/')

# COMMAND ----------

pricingReferenceSinkFolderPath = f"abfss://{pricingReferenceSinkLayerName}@{pricingReferenceSinkStorageAccountName}.dfs.core.windows.net/{pricingReferenceSinkFolderName}/{pricingReferenceSinkTableFolder}"

(
    pricingReferenceSourceTableDF
    .write
    .mode("overwrite")
    .json(pricingReferenceSinkFolderPath)
)


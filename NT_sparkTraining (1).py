# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://sptrainingcontainer@spaccountst.blob.core.windows.net",
  mount_point = "/mnt/spark_training",
  extra_configs = {"fs.azure.account.key.spaccountst.blob.core.windows.net":dbutils.secrets.get(scope = "key-vault-secret-sptraining", key = "spaccountst")})

# COMMAND ----------

csvFile= "/mnt/spark_training/GreatHouse Holdings District.csv"

df_GH_district = (spark.read           # The DataFrameReader
.option("delimiter", ";")       # Use tab delimiter (default is comma-separator)
 .option("header", "true")   # Use first line of all files as header
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)
df_GH_district.display()

# COMMAND ----------

csvFile= "/mnt/spark_training/Stock GreatHouse Holdings.csv"

df_GH_stock = (spark.read           # The DataFrameReader
.option("delimiter", ";")       # Use tab delimiter (default is comma-separator)
 .option("header", "true")   # Use first line of all files as header
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)
df_GH_stock.display()

# COMMAND ----------

csvFile= "/mnt/spark_training/Roger&Brothers District.csv"

df_RB_district = (spark.read           # The DataFrameReader
.option("delimiter", ";")       # Use tab delimiter (default is comma-separator)
 .option("header", "true")   # Use first line of all files as header
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)
df_RB_district.display()

# COMMAND ----------

csvFile= "/mnt/spark_training/Stock Roger&Brothers.csv"

df_RB_stock = (spark.read           # The DataFrameReader
.option("delimiter", ";")       # Use tab delimiter (default is comma-separator)
 .option("header", "true")   # Use first line of all files as header
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)
df_RB_stock.display()

# COMMAND ----------

from pyspark.sql.functions import *
df_merged_district = df_RB_district.unionByName(df_GH_district)

df_merged_district.display()

# COMMAND ----------


df_merged_stock = df_RB_stock.unionByName(df_GH_stock)
df_merged_stock.display()

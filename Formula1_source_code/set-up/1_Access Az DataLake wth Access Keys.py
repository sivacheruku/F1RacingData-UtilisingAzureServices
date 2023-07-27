# Databricks notebook source
# MAGIC %md
# MAGIC ##Accessing data from Azure Data Lake storage (gen2) with ACCESS KEY
# MAGIC 1. Set spark configuration to access the Access Key of the Azure account
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get('formula1scope','formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    'fs.azure.account.key.formula1deltalke.dfs.core.windows.net', formula1dl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@formula1deltalke.dfs.core.windows.net/'))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1deltalke.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------



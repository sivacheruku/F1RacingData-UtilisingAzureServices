# Databricks notebook source
# MAGIC %md
# MAGIC ##Accessing data from Azure Data Lake storage (gen2) with SAS Token
# MAGIC 1. Set spark configuration for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv

# COMMAND ----------

dbutils.secrets.list('formula1scope')

# COMMAND ----------

formula1_sas_token = dbutils.secrets.get('formula1scope', 'formula1dl-sastoken')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1deltalke.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1deltalke.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1deltalke.dfs.core.windows.net", formula1_sas_token )

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@formula1deltalke.dfs.core.windows.net/'))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1deltalke.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


